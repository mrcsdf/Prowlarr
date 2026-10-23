using System;
using System.Buffers.Binary;
using System.Data.SQLite;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using NzbDrone.Core.Datastore;

namespace NzbDrone.Core.IndexerSearch
{
    public static class TerminalEvidenceWorker
    {
        public const string EntryPointArgument = "--d8-terminal-evidence-worker-v1";
        public const string JournalEntryPointArgument = "--d8-search-journal-worker-v1";
        public const int MaximumRequestFrameBytes = 16 * 1024;
        public const int MaximumJournalRequestFrameBytes = 96 * 1024;
        public const int MaximumResponseFrameBytes = 128 * 1024;
        private const int PrSetPdeathsig = 1;
        private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
        private static readonly JsonSerializerOptions JournalJsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
        };

        [DllImport("libc", SetLastError = true)]
        private static extern int prctl(int option, ulong arg2, ulong arg3, ulong arg4, ulong arg5);

        [DllImport("libc")]
        private static extern int getppid();

        public static async Task<int> RunAsync(Stream input, Stream output)
        {
            if (!OperatingSystem.IsLinux() ||
                !int.TryParse(Environment.GetEnvironmentVariable("PROWLARR_D8_WORKER_PARENT_PID"), out var expectedParent) ||
                prctl(PrSetPdeathsig, 9, 0, 0, 0) != 0 || getppid() != expectedParent)
            {
                return 30;
            }

            try
            {
                var requestBytes = await ReadFrameAsync(input, MaximumRequestFrameBytes).ConfigureAwait(false);
                var request = JsonSerializer.Deserialize<TerminalEvidenceWorkerRequest>(requestBytes, JsonOptions);
                if (request == null || request.Version != 1 || request.Operation != "read-and-redeem" ||
                    !Guid.TryParseExact(request.OperationId, "D", out _) ||
                    !Guid.TryParseExact(request.TokenId, "D", out var tokenId) || tokenId.ToString("D") != request.TokenId ||
                    !TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(request.CorrelationId) ||
                    request.ExpiresAtUnixMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                {
                    return 31;
                }

                var connectionString = Environment.GetEnvironmentVariable("PROWLARR_D8_WORKER_SQLITE_CONNECTION");
                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    return 32;
                }

                var builder = new SQLiteConnectionStringBuilder(connectionString);
                if (builder.Version != 3 || !Path.IsPathFullyQualified(builder.DataSource) || !File.Exists(builder.DataSource))
                {
                    return 33;
                }

                builder.Pooling = false;
                builder.BusyTimeout = 1000;
                builder.DefaultTimeout = 1;
                var database = new Database("D8 terminal evidence", () =>
                {
                    var connection = new SQLiteConnection(builder.ConnectionString);
                    connection.Open();
                    return connection;
                });
                var mainDatabase = new MainDatabase(database);
                var replayStore = new TerminalEvidenceCapabilityReplayStore(mainDatabase, null);
                var recordRepository = new IndexerSearchTerminalRecordRepository(mainDatabase, null);

                var redeemed = await replayStore.TryRedeemAsync(request.TokenId, request.ExpiresAtUnixMs, CancellationToken.None).ConfigureAwait(false);
                if (!redeemed)
                {
                    await WriteFrameAsync(output, new TerminalEvidenceWorkerResponse(1, false, false, null)).ConfigureAwait(false);
                    return 0;
                }

                var record = await recordRepository.GetAsync(request.OperationId, CancellationToken.None).ConfigureAwait(false);
                var recordJson = record == null ? null : Newtonsoft.Json.JsonConvert.SerializeObject(record);
                if (recordJson != null && System.Text.Encoding.UTF8.GetByteCount(recordJson) > IndexerSearchTerminalContract.MaximumRecordBytes)
                {
                    return 34;
                }

                await WriteFrameAsync(output, new TerminalEvidenceWorkerResponse(1, true, record != null, recordJson)).ConfigureAwait(false);
                return 0;
            }
            catch (Exception)
            {
                // The parent treats a nonzero exit or missing response as an ambiguous failure.
                // Never include database paths, SQL text, tokens, or exception messages on IPC/stderr.
                return 40;
            }
        }

        public static async Task<int> RunJournalAsync(Stream input, Stream output)
        {
            if (!TryEnableParentDeathContainment())
            {
                return 30;
            }

            try
            {
                var bytes = await ReadFrameAsync(input, MaximumJournalRequestFrameBytes).ConfigureAwait(false);
                var request = JsonSerializer.Deserialize<D8SearchJournalWorkerRequest>(bytes, JournalJsonOptions);
                if (!IsValidJournalRequest(request))
                {
                    return 31;
                }

                var connectionString = Environment.GetEnvironmentVariable("PROWLARR_D8_WORKER_SQLITE_CONNECTION");
                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    return 32;
                }

                var builder = new SQLiteConnectionStringBuilder(connectionString);
                if (builder.Version != 3 || !Path.IsPathFullyQualified(builder.DataSource) || !File.Exists(builder.DataSource))
                {
                    return 33;
                }

                builder.Pooling = false;
                builder.BusyTimeout = 1000;
                builder.DefaultTimeout = 1;
                var database = new Database("D8 search journal", () =>
                {
                    var connection = new SQLiteConnection(builder.ConnectionString);
                    connection.Open();
                    return connection;
                });
                var main = new MainDatabase(database);
                var terminalRecords = new IndexerSearchTerminalRecordRepository(main, null);
                var journal = new D8SearchOperationJournalRepository(main, terminalRecords);
                D8SearchJournalWorkerResponse response;
                try
                {
                    response = ExecuteJournalCommand(journal, request);
                }
                catch (SQLiteException exception)
                {
                    response = D8SearchJournalWorkerResponse.Failure(D8SearchJournalFailure.FromSqliteError(exception.ResultCode));
                }
                catch (InvalidOperationException)
                {
                    response = D8SearchJournalWorkerResponse.Failure(D8SearchJournalFailure.Rejected);
                }
                catch (Exception)
                {
                    response = D8SearchJournalWorkerResponse.Failure(D8SearchJournalFailure.Internal);
                }

                await WriteJournalFrameAsync(output, response).ConfigureAwait(false);
                return 0;
            }
            catch (Exception)
            {
                return 40;
            }
        }

        private static bool TryEnableParentDeathContainment()
        {
            return OperatingSystem.IsLinux() &&
                int.TryParse(Environment.GetEnvironmentVariable("PROWLARR_D8_WORKER_PARENT_PID"), out var expectedParent) &&
                prctl(PrSetPdeathsig, 9, 0, 0, 0) == 0 && getppid() == expectedParent;
        }

        private static bool IsValidJournalRequest(D8SearchJournalWorkerRequest request)
        {
            if (request == null || request.Version != 1 || request.Operation is not ("start" or "attempt-intent" or "pre-dispatch-boundary" or "attempt-terminal" or "finalize" or "fail" or "recover"))
            {
                return false;
            }

            if (request.Operation == "start")
            {
                return request.IndexerId > 0 && !string.IsNullOrEmpty(request.CallerIdentity) && request.CallerIdentity.Length <= 256 &&
                    !string.IsNullOrEmpty(request.IdempotencyKey) && request.IdempotencyKey.Length <= 128 &&
                    Guid.TryParseExact(request.OperationId, "D", out var operationId) && operationId.ToString("D") == request.OperationId &&
                    Guid.TryParseExact(request.ProcessInstanceId, "D", out var processInstanceId) && processInstanceId.ToString("D") == request.ProcessInstanceId &&
                    TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(request.CorrelationId) && request.StartedAtUnixMs > 0 &&
                    request.EndedAtUnixMs == 0 && request.FailureReason == null && request.AttemptId == null && request.Attempt == null && request.TerminalRecord == null;
            }

            if (request.Operation is "attempt-intent" or "attempt-terminal")
            {
                return Guid.TryParseExact(request.OperationId, "D", out var operationId) && operationId.ToString("D") == request.OperationId &&
                    request.Attempt != null && JsonSerializer.SerializeToUtf8Bytes(request.Attempt).Length <= 4096 &&
                    request.CallerIdentity == null && request.IdempotencyKey == null && request.IndexerId == 0 &&
                    request.ProcessInstanceId == null && request.CorrelationId == null && request.StartedAtUnixMs == 0 &&
                    request.EndedAtUnixMs == 0 && request.FailureReason == null && request.AttemptId == null && request.TerminalRecord == null;
            }

            if (request.Operation == "pre-dispatch-boundary")
            {
                return Guid.TryParseExact(request.OperationId, "D", out var operationId) && operationId.ToString("D") == request.OperationId &&
                    Guid.TryParseExact(request.AttemptId, "D", out var attemptId) && attemptId.ToString("D") == request.AttemptId &&
                    request.CallerIdentity == null && request.IdempotencyKey == null && request.IndexerId == 0 &&
                    request.ProcessInstanceId == null && request.CorrelationId == null && request.StartedAtUnixMs == 0 &&
                    request.EndedAtUnixMs == 0 && request.FailureReason == null && request.Attempt == null && request.TerminalRecord == null;
            }

            if (request.Operation == "finalize")
            {
                return Guid.TryParseExact(request.OperationId, "D", out var operationId) && operationId.ToString("D") == request.OperationId &&
                    request.TerminalRecord != null &&
                    System.Text.Encoding.UTF8.GetByteCount(Newtonsoft.Json.JsonConvert.SerializeObject(request.TerminalRecord)) <= IndexerSearchTerminalContract.MaximumRecordBytes &&
                    request.AttemptId == null && request.Attempt == null && request.CallerIdentity == null &&
                    request.IdempotencyKey == null && request.IndexerId == 0 && request.ProcessInstanceId == null &&
                    request.CorrelationId == null && request.StartedAtUnixMs == 0 && request.EndedAtUnixMs == 0 && request.FailureReason == null;
            }

            if (request.Operation == "fail")
            {
                return Guid.TryParseExact(request.OperationId, "D", out var operationId) && operationId.ToString("D") == request.OperationId &&
                    !string.IsNullOrWhiteSpace(request.CallerIdentity) && request.CallerIdentity.Length <= 256 &&
                    TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(request.CorrelationId) && request.EndedAtUnixMs > 0 &&
                    !string.IsNullOrWhiteSpace(request.FailureReason) && request.FailureReason.Length <= 128 &&
                    request.IdempotencyKey == null && request.IndexerId == 0 && request.ProcessInstanceId == null &&
                    request.StartedAtUnixMs == 0 && request.AttemptId == null && request.Attempt == null && request.TerminalRecord == null;
            }

            return request.Operation == "recover" && request.CallerIdentity == null && request.IdempotencyKey == null &&
                request.IndexerId == 0 && request.OperationId == null && request.ProcessInstanceId == null && request.CorrelationId == null &&
                request.StartedAtUnixMs == 0 && request.EndedAtUnixMs == 0 && request.FailureReason == null && request.AttemptId == null && request.Attempt == null && request.TerminalRecord == null;
        }

        private static D8SearchJournalWorkerResponse ExecuteJournalCommand(D8SearchOperationJournalRepository journal, D8SearchJournalWorkerRequest request)
        {
            switch (request.Operation)
            {
                case "start":
                    return D8SearchJournalWorkerResponse.Success(JsonSerializer.Serialize(journal.Start(
                        request.CallerIdentity,
                        request.IdempotencyKey,
                        request.IndexerId,
                        request.OperationId,
                        request.ProcessInstanceId,
                        request.CorrelationId,
                        DateTimeOffset.FromUnixTimeMilliseconds(request.StartedAtUnixMs))));
                case "attempt-intent":
                    journal.RecordAttemptIntent(request.OperationId, request.Attempt);
                    return D8SearchJournalWorkerResponse.Success(null);
                case "pre-dispatch-boundary":
                    journal.RecordPreDispatchBoundary(request.OperationId, Guid.ParseExact(request.AttemptId, "D"));
                    return D8SearchJournalWorkerResponse.Success(null);
                case "attempt-terminal":
                    journal.TerminalizeAttempt(request.OperationId, request.Attempt);
                    return D8SearchJournalWorkerResponse.Success(null);
                case "finalize":
                    return D8SearchJournalWorkerResponse.Success(Newtonsoft.Json.JsonConvert.SerializeObject(
                        journal.CompleteFromWorker(request.OperationId, request.TerminalRecord)));
                case "fail":
                    return D8SearchJournalWorkerResponse.Success(Newtonsoft.Json.JsonConvert.SerializeObject(journal.Fail(
                        request.OperationId,
                        request.CallerIdentity,
                        request.CorrelationId,
                        DateTimeOffset.FromUnixTimeMilliseconds(request.EndedAtUnixMs),
                        request.FailureReason)));
                case "recover":
                    journal.RecoverInterrupted();
                    return D8SearchJournalWorkerResponse.Success(null);
                default:
                    return D8SearchJournalWorkerResponse.Failure(D8SearchJournalFailure.Rejected);
            }
        }

        private static async Task<byte[]> ReadFrameAsync(Stream stream, int maximumBytes)
        {
            var prefix = new byte[4];
            await ReadExactlyAsync(stream, prefix).ConfigureAwait(false);
            var length = BinaryPrimitives.ReadInt32BigEndian(prefix);
            if (length < 2 || length > maximumBytes)
            {
                throw new InvalidDataException("Invalid worker frame length");
            }

            var body = new byte[length];
            await ReadExactlyAsync(stream, body).ConfigureAwait(false);
            return body;
        }

        private static async Task ReadExactlyAsync(Stream stream, byte[] buffer)
        {
            var offset = 0;
            while (offset < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(offset)).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                offset += read;
            }
        }

        private static async Task WriteFrameAsync(Stream stream, TerminalEvidenceWorkerResponse response)
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(response);
            if (body.Length > MaximumResponseFrameBytes)
            {
                throw new InvalidDataException("Worker response exceeds limit");
            }

            var prefix = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(prefix, body.Length);
            await stream.WriteAsync(prefix).ConfigureAwait(false);
            await stream.WriteAsync(body).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }

        private static async Task WriteJournalFrameAsync(Stream stream, D8SearchJournalWorkerResponse response)
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(response);
            if (body.Length > MaximumResponseFrameBytes)
            {
                throw new InvalidDataException("Journal worker response exceeds the frame bound");
            }

            var prefix = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(prefix, body.Length);
            await stream.WriteAsync(prefix).ConfigureAwait(false);
            await stream.WriteAsync(body).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }
    }

    public sealed class TerminalEvidenceWorkerRequest
    {
        public int Version { get; set; }
        public string Operation { get; set; }
        public string OperationId { get; set; }
        public string CorrelationId { get; set; }
        public string TokenId { get; set; }
        public long ExpiresAtUnixMs { get; set; }
    }

    public sealed class TerminalEvidenceWorkerResponse
    {
        public TerminalEvidenceWorkerResponse(int version, bool redeemed, bool found, string recordJson)
        {
            Version = version;
            Redeemed = redeemed;
            Found = found;
            RecordJson = recordJson;
        }

        public int Version { get; }
        public bool Redeemed { get; }
        public bool Found { get; }
        public string RecordJson { get; }
    }

    public sealed class D8SearchJournalWorkerRequest
    {
        public int Version { get; set; }
        public string Operation { get; set; }
        public string CallerIdentity { get; set; }
        public string IdempotencyKey { get; set; }
        public int IndexerId { get; set; }
        public string OperationId { get; set; }
        public string ProcessInstanceId { get; set; }
        public string CorrelationId { get; set; }
        public long StartedAtUnixMs { get; set; }
        public long EndedAtUnixMs { get; set; }
        public string FailureReason { get; set; }
        public string AttemptId { get; set; }
        public ObservedAttempt Attempt { get; set; }
        public IndexerSearchTerminalRecord TerminalRecord { get; set; }
    }

    public sealed class D8SearchJournalWorkerResponse
    {
        public int Version { get; set; } = 1;
        public string ResultCode { get; set; }
        public string PayloadJson { get; set; }

        public static D8SearchJournalWorkerResponse Success(string payload) => new() { ResultCode = "ok", PayloadJson = payload };
        public static D8SearchJournalWorkerResponse Failure(string code) => new() { ResultCode = code };
    }
}
