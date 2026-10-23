using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.IndexerSearch;

[assembly: InternalsVisibleTo("Prowlarr.Integration.Test")]

namespace NzbDrone.Host
{
    /// <summary>
    /// Creates a worker process start configuration.
    /// </summary>
    public interface ITerminalEvidenceWorkerProcessLauncher
    {
        ProcessStartInfo CreateStartInfo(string entryPointArgument = null);
    }

    public sealed class TerminalEvidenceWorkerProcessLauncher : ITerminalEvidenceWorkerProcessLauncher
    {
        public ProcessStartInfo CreateStartInfo(string entryPointArgument = null)
        {
            var executable = Environment.ProcessPath;
            if (string.IsNullOrWhiteSpace(executable) || !File.Exists(executable))
            {
                throw new InvalidOperationException("Prowlarr worker executable path is unavailable");
            }

            var startInfo = new ProcessStartInfo(executable)
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
            startInfo.ArgumentList.Add(entryPointArgument ?? TerminalEvidenceWorker.EntryPointArgument);
            return startInfo;
        }
    }

    /// <summary>
    /// Owns termination and reaping calls so shutdown failures can be exercised with real
    /// child processes in focused tests.
    /// </summary>
    internal interface ITerminalEvidenceWorkerProcessControl
    {
        void Kill(Process worker);
        Task WaitForExitAsync(Process worker, CancellationToken cancellationToken);
    }

    internal sealed class TerminalEvidenceWorkerProcessControl : ITerminalEvidenceWorkerProcessControl
    {
        public void Kill(Process worker)
        {
            worker.Kill(entireProcessTree: true);
        }

        public Task WaitForExitAsync(Process worker, CancellationToken cancellationToken)
        {
            return worker.WaitForExitAsync(cancellationToken);
        }
    }

    public sealed class TerminalEvidenceWorkerClient
    {
        private const string ConnectionStringVariable = "PROWLARR_D8_WORKER_SQLITE_CONNECTION";
        private const string ParentPidVariable = "PROWLARR_D8_WORKER_PARENT_PID";
        private static readonly string[] InheritedWorkerEnvironment =
        {
            "PATH",
            "DOTNET_ROOT",
            "DOTNET_ROOT_X64",
            "DOTNET_ROOT_ARM64",
            "TMPDIR",
            "TMP",
            "TEMP"
        };
        private static readonly TimeSpan RequestDeadline = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan CleanupReserve = TimeSpan.FromMilliseconds(500);
        private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

        private readonly IConnectionStringFactory _connectionStringFactory;
        private readonly ITerminalEvidenceWorkerProcessLauncher _launcher;
        private readonly ITerminalEvidenceWorkerProcessControl _processControl;

        // One active child per traffic class (journal writes and evidence reads), with a
        // bounded FIFO waiter queue. The total child-process ceiling is therefore two.
        private readonly FairAsyncAdmission _journalAdmission = new(8);
        private readonly FairAsyncAdmission _retrievalAdmission = new(8);
        private readonly CancellationTokenSource _shutdown = new();
        private readonly ConcurrentDictionary<int, Process> _pending = new();
        private readonly ConcurrentDictionary<int, Process> _activeWorkers = new();
        private string _sqliteConnectionString;
        private volatile bool _unhealthy;

        internal int PendingWorkerCount => _pending.Count;
        internal bool IsUnhealthy => _unhealthy;
        internal Type ProcessControlType => _processControl.GetType();

        public TerminalEvidenceWorkerClient(
            IConnectionStringFactory connectionStringFactory,
            ITerminalEvidenceWorkerProcessLauncher launcher)
            : this(connectionStringFactory, launcher, new TerminalEvidenceWorkerProcessControl())
        {
        }

        internal TerminalEvidenceWorkerClient(
            IConnectionStringFactory connectionStringFactory,
            ITerminalEvidenceWorkerProcessLauncher launcher,
            ITerminalEvidenceWorkerProcessControl processControl)
        {
            _connectionStringFactory = connectionStringFactory;
            _launcher = launcher;
            _processControl = processControl;
        }

        public void ValidateConfiguration()
        {
            _ = GetSqliteConnectionString();
        }

        private string GetSqliteConnectionString()
        {
            if (_sqliteConnectionString != null)
            {
                return _sqliteConnectionString;
            }

            var connection = _connectionStringFactory.MainDbConnection;
            if (connection.DatabaseType != DatabaseType.SQLite)
            {
                throw new InvalidOperationException("D8 process-isolated evidence currently requires SQLite");
            }

            var builder = new SQLiteConnectionStringBuilder(connection.ConnectionString);
            if (builder.Version != 3 || !Path.IsPathFullyQualified(builder.DataSource) || !File.Exists(builder.DataSource))
            {
                throw new InvalidOperationException("D8 worker requires an existing absolute SQLite database path");
            }

            builder.Pooling = false;
            _sqliteConnectionString = builder.ConnectionString;
            return _sqliteConnectionString;
        }

        public async Task<TerminalEvidenceWorkerResult> ReadAndRedeemAsync(
            string operationId,
            string correlationId,
            string tokenId,
            long expiresAtUnixMs,
            CancellationToken cancellationToken)
        {
            var started = Stopwatch.GetTimestamp();
            if (_unhealthy || _shutdown.IsCancellationRequested)
            {
                throw new TerminalEvidenceWorkerUnavailableException();
            }

            IDisposable admissionLease = null;
            Process worker = null;
            var workerId = 0;
            var reaped = false;
            CancellationTokenSource deadline = null;
            var stderrDrain = Task.CompletedTask;
            try
            {
                using var admissionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
                admissionLease = await _retrievalAdmission.AcquireAsync(
                    Remaining(started, RequestDeadline - CleanupReserve), admissionCancellation.Token).ConfigureAwait(false);
                if (_unhealthy)
                {
                    throw new TerminalEvidenceWorkerUnavailableException();
                }

                deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
                deadline.Token.ThrowIfCancellationRequested();
                if (!Guid.TryParseExact(operationId, "D", out _) ||
                    !Guid.TryParseExact(tokenId, "D", out var parsedToken) || parsedToken.ToString("D") != tokenId ||
                    !TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(correlationId) ||
                    expiresAtUnixMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
                {
                    throw new InvalidDataException("D8 worker request is invalid");
                }

                var remainingWork = Remaining(started, RequestDeadline - CleanupReserve);
                if (remainingWork <= TimeSpan.Zero)
                {
                    throw new TimeoutException("D8 worker request deadline expired before launch");
                }

                deadline.CancelAfter(remainingWork);
                var startInfo = _launcher.CreateStartInfo();
                RestrictWorkerEnvironment(startInfo);
                startInfo.Environment[ConnectionStringVariable] = GetSqliteConnectionString();
                startInfo.Environment[ParentPidVariable] = Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                worker = Process.Start(startInfo) ?? throw new InvalidOperationException("D8 worker process failed to start");
                workerId = worker.Id;
                _activeWorkers[workerId] = worker;
                stderrDrain = DrainStandardErrorAsync(worker.StandardError.BaseStream);

                var request = new TerminalEvidenceWorkerRequest
                {
                    Version = 1,
                    Operation = "read-and-redeem",
                    OperationId = operationId,
                    CorrelationId = correlationId,
                    TokenId = tokenId,
                    ExpiresAtUnixMs = expiresAtUnixMs
                };
                var requestBytes = JsonSerializer.SerializeToUtf8Bytes(request);
                if (requestBytes.Length > TerminalEvidenceWorker.MaximumRequestFrameBytes)
                {
                    throw new InvalidDataException("D8 worker request exceeds the protocol bound");
                }

                var prefix = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(prefix, requestBytes.Length);
                await worker.StandardInput.BaseStream.WriteAsync(prefix, deadline.Token).ConfigureAwait(false);
                await worker.StandardInput.BaseStream.WriteAsync(requestBytes, deadline.Token).ConfigureAwait(false);
                await worker.StandardInput.BaseStream.FlushAsync(deadline.Token).ConfigureAwait(false);
                worker.StandardInput.Close();

                var responseBytes = await ReadFrameBeforeDeadlineAsync(worker.StandardOutput.BaseStream, Remaining(started, RequestDeadline - CleanupReserve), deadline.Token).ConfigureAwait(false);
                var response = JsonSerializer.Deserialize<TerminalEvidenceWorkerResponse>(responseBytes, JsonOptions);
                if (response == null || response.Version != 1 ||
                    (response.RecordJson != null && Encoding.UTF8.GetByteCount(response.RecordJson) > IndexerSearchTerminalContract.MaximumRecordBytes))
                {
                    throw new InvalidDataException("D8 worker response is invalid");
                }

                var remainingExit = Remaining(started, RequestDeadline - CleanupReserve);
                if (remainingExit <= TimeSpan.Zero)
                {
                    throw new TimeoutException("D8 worker response arrived after the work deadline");
                }

                await _processControl.WaitForExitAsync(worker, deadline.Token).WaitAsync(remainingExit, CancellationToken.None).ConfigureAwait(false);
                reaped = worker.HasExited;
                await stderrDrain.WaitAsync(Remaining(started, RequestDeadline), CancellationToken.None).ConfigureAwait(false);
                if (!reaped || worker.ExitCode != 0)
                {
                    throw new InvalidOperationException("D8 worker exited unsuccessfully");
                }

                if (!response.Redeemed)
                {
                    if (response.Found || response.RecordJson != null)
                    {
                        throw new InvalidDataException("D8 worker returned a record without redeeming its capability");
                    }

                    return new TerminalEvidenceWorkerResult(false, null);
                }

                if (!response.Found || string.IsNullOrWhiteSpace(response.RecordJson))
                {
                    return new TerminalEvidenceWorkerResult(true, null);
                }

                var record = Newtonsoft.Json.JsonConvert.DeserializeObject<IndexerSearchTerminalRecord>(response.RecordJson);
                if (record == null || record.OperationId != operationId || record.RequestCorrelationId != correlationId)
                {
                    throw new InvalidDataException("D8 worker record binding is invalid");
                }

                record.CommitAcknowledged = true;
                return new TerminalEvidenceWorkerResult(true, record);
            }
            catch (Exception exception)
            {
                if (worker != null && worker.HasExited)
                {
                    reaped = true;
                }
                else if (worker != null)
                {
                    reaped = await TerminateAndReapAsync(worker, started).ConfigureAwait(false);
                }

                if (reaped)
                {
                    try
                    {
                        var remaining = Remaining(started, RequestDeadline);
                        if (remaining > TimeSpan.Zero)
                        {
                            await stderrDrain.WaitAsync(remaining, CancellationToken.None).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        // Child diagnostics are discarded; observe completion/failure so a
                        // failed request cannot leave an unobserved drain task behind.
                    }
                }

                throw new TerminalEvidenceWorkerException("D8 evidence worker failed closed", exception, !_unhealthy && reaped);
            }
            finally
            {
                try
                {
                    if (worker != null)
                    {
                        if (worker.HasExited || reaped)
                        {
                            _activeWorkers.TryRemove(workerId, out _);
                            _pending.TryRemove(workerId, out _);
                            worker.Dispose();
                        }
                        else
                        {
                            TrackPendingWorker(workerId, worker);
                        }
                    }
                }
                finally
                {
                    try
                    {
                        deadline?.Dispose();
                    }
                    finally
                    {
                        admissionLease?.Dispose();
                    }
                }
            }
        }

        public async Task<D8SearchJournalWorkerResponse> RunJournalCommandAsync(
            D8SearchJournalWorkerRequest request,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            var started = Stopwatch.GetTimestamp();
            if (remainingDeadline <= CleanupReserve || _unhealthy || _shutdown.IsCancellationRequested)
            {
                throw new TerminalEvidenceWorkerUnavailableException();
            }

            IDisposable admissionLease = null;
            Process worker = null;
            var workerId = 0;
            var reaped = false;
            CancellationTokenSource deadline = null;
            var stderrDrain = Task.CompletedTask;
            var workBudget = TimeSpan.Zero;
            try
            {
                using var admissionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
                admissionLease = await _journalAdmission.AcquireAsync(
                    Remaining(started, remainingDeadline - CleanupReserve), admissionCancellation.Token).ConfigureAwait(false);
                if (_unhealthy)
                {
                    throw new TerminalEvidenceWorkerUnavailableException();
                }

                // Recompute after FIFO admission. The queue wait is part of the caller's
                // original monotonic deadline; no stale pre-wait timeout is reused.
                workBudget = Remaining(started, remainingDeadline - CleanupReserve);
                if (workBudget <= TimeSpan.Zero)
                {
                    throw new TimeoutException("D8 journal admission exceeded its work deadline");
                }

                if (request == null || request.Version != 1)
                {
                    throw new InvalidDataException("D8 journal worker request is invalid");
                }

                deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
                deadline.Token.ThrowIfCancellationRequested();
                deadline.CancelAfter(workBudget);
                var startInfo = _launcher.CreateStartInfo(TerminalEvidenceWorker.JournalEntryPointArgument);
                RestrictWorkerEnvironment(startInfo);
                startInfo.Environment[ConnectionStringVariable] = GetSqliteConnectionString();
                startInfo.Environment[ParentPidVariable] = Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                worker = Process.Start(startInfo) ?? throw new InvalidOperationException("D8 journal worker failed to start");
                workerId = worker.Id;
                _activeWorkers[workerId] = worker;
                stderrDrain = DrainStandardErrorAsync(worker.StandardError.BaseStream);

                var bytes = JsonSerializer.SerializeToUtf8Bytes(request);
                if (bytes.Length is < 2 or > TerminalEvidenceWorker.MaximumJournalRequestFrameBytes)
                {
                    throw new InvalidDataException("D8 journal worker request exceeds its frame bound");
                }

                var prefix = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(prefix, bytes.Length);
                await worker.StandardInput.BaseStream.WriteAsync(prefix, deadline.Token).ConfigureAwait(false);
                await worker.StandardInput.BaseStream.WriteAsync(bytes, deadline.Token).ConfigureAwait(false);
                await worker.StandardInput.BaseStream.FlushAsync(deadline.Token).ConfigureAwait(false);
                worker.StandardInput.Close();

                var responseBytes = await ReadFrameBeforeDeadlineAsync(
                    worker.StandardOutput.BaseStream,
                    Remaining(started, workBudget),
                    deadline.Token).ConfigureAwait(false);
                var response = JsonSerializer.Deserialize<D8SearchJournalWorkerResponse>(responseBytes, JsonOptions);
                if (response == null || response.Version != 1 ||
                    (response.ResultCode != "ok" && !D8SearchJournalFailure.IsKnown(response.ResultCode)))
                {
                    throw new InvalidDataException("D8 journal worker response is malformed");
                }

                var remainingExit = Remaining(started, workBudget);
                if (remainingExit <= TimeSpan.Zero)
                {
                    throw new TimeoutException("D8 journal worker exceeded its work deadline");
                }

                await _processControl.WaitForExitAsync(worker, deadline.Token).WaitAsync(remainingExit, CancellationToken.None).ConfigureAwait(false);
                reaped = worker.HasExited;
                await stderrDrain.WaitAsync(Remaining(started, remainingDeadline), CancellationToken.None).ConfigureAwait(false);
                if (!reaped || worker.ExitCode != 0)
                {
                    throw new InvalidOperationException("D8 journal worker exited unsuccessfully");
                }

                return response;
            }
            catch
            {
                if (worker != null && worker.HasExited)
                {
                    reaped = true;
                }
                else if (worker != null)
                {
                    try
                    {
                        if (!worker.HasExited)
                        {
                            _processControl.Kill(worker);
                        }

                        var cleanup = Remaining(started, remainingDeadline);
                        if (cleanup > TimeSpan.Zero)
                        {
                            await _processControl.WaitForExitAsync(worker, CancellationToken.None)
                                .WaitAsync(cleanup, CancellationToken.None).ConfigureAwait(false);
                            reaped = worker.HasExited;
                        }
                    }
                    catch
                    {
                        _unhealthy = true;
                    }
                }

                throw;
            }
            finally
            {
                if (worker != null)
                {
                    if (worker.HasExited || reaped)
                    {
                        _activeWorkers.TryRemove(workerId, out _);
                        _pending.TryRemove(workerId, out _);
                        worker.Dispose();
                    }
                    else
                    {
                        TrackPendingWorker(workerId, worker);
                    }
                }

                try
                {
                    deadline?.Dispose();
                }
                finally
                {
                    admissionLease?.Dispose();
                }
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                _shutdown.Cancel();
            }
            catch
            {
                _unhealthy = true;
                PreservePendingWorkers();
                throw;
            }

            foreach (var active in _activeWorkers.Values.ToArray())
            {
                try
                {
                    if (!active.HasExited)
                    {
                        _processControl.Kill(active);
                    }
                }
                catch (InvalidOperationException)
                {
                    // The operation may have reaped and disposed the process concurrently.
                }
                catch
                {
                    _unhealthy = true;
                    PreservePendingWorkers();
                    throw;
                }
            }

            try
            {
                await Task.WhenAll(
                    _journalAdmission.WaitForIdleAsync(TimeSpan.FromSeconds(2)),
                    _retrievalAdmission.WaitForIdleAsync(TimeSpan.FromSeconds(2)))
                    .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _unhealthy = true;
                PreservePendingWorkers();
                throw;
            }
            catch (TimeoutException)
            {
                _unhealthy = true;
                PreservePendingWorkers();
                throw new TimeoutException("D8 workers did not stop within the shutdown bound");
            }
            catch
            {
                _unhealthy = true;
                PreservePendingWorkers();
                throw;
            }

            foreach (var item in _pending.ToArray())
            {
                var worker = item.Value;
                try
                {
                    if (!worker.HasExited)
                    {
                        _processControl.Kill(worker);
                    }

                    await _processControl.WaitForExitAsync(worker, cancellationToken)
                        .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
                    _pending.TryRemove(item.Key, out _);
                    worker.Dispose();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _unhealthy = true;
                    throw;
                }
                catch (TimeoutException)
                {
                    _unhealthy = true;
                    throw new TimeoutException("D8 workers did not stop within the shutdown bound");
                }
                catch
                {
                    _unhealthy = true;
                    throw;
                }
            }
        }

        private void PreservePendingWorkers()
        {
            // Latch first so no new work can be admitted while pending state is published.
            _unhealthy = true;
            foreach (var active in _activeWorkers)
            {
                _pending.TryAdd(active.Key, active.Value);
            }
        }

        private void TrackPendingWorker(int workerId, Process worker)
        {
            // Make the fail-closed state visible before the child becomes pending, and
            // publish it before removing the active entry so process tracking has no gap.
            _unhealthy = true;
            _pending[workerId] = worker;
            _activeWorkers.TryRemove(workerId, out _);
        }

        private async Task<bool> TerminateAndReapAsync(Process worker, long started)
        {
            try
            {
                if (!worker.HasExited)
                {
                    _processControl.Kill(worker);
                }

                var remaining = Remaining(started, RequestDeadline);
                if (remaining <= TimeSpan.Zero)
                {
                    _unhealthy = true;
                    return worker.HasExited;
                }

                await _processControl.WaitForExitAsync(worker, CancellationToken.None).WaitAsync(remaining).ConfigureAwait(false);
                return worker.HasExited;
            }
            catch
            {
                _unhealthy = true;
                return worker.HasExited;
            }
        }

        private static async Task<byte[]> ReadFrameBeforeDeadlineAsync(Stream stream, TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (timeout <= TimeSpan.Zero)
            {
                throw new TimeoutException("D8 worker response deadline expired");
            }

            using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutSource.CancelAfter(timeout);
            var prefix = new byte[4];
            await ReadExactlyAsync(stream, prefix, timeoutSource.Token).ConfigureAwait(false);
            var length = BinaryPrimitives.ReadInt32BigEndian(prefix);
            if (length is < 2 or > TerminalEvidenceWorker.MaximumResponseFrameBytes)
            {
                throw new InvalidDataException("D8 worker response frame length is invalid");
            }

            var body = new byte[length];
            await ReadExactlyAsync(stream, body, timeoutSource.Token).ConfigureAwait(false);
            return body;
        }

        private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
        {
            var offset = 0;
            while (offset < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(offset), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException("D8 worker closed its response pipe");
                }

                offset += read;
            }
        }

        private static async Task DrainStandardErrorAsync(Stream stream)
        {
            var buffer = new byte[1024];
            while (await stream.ReadAsync(buffer).ConfigureAwait(false) != 0)
            {
                // Drain but never relay child diagnostics, which could expose local paths or data.
            }
        }

        private static void RestrictWorkerEnvironment(ProcessStartInfo startInfo)
        {
            var inherited = InheritedWorkerEnvironment
                .Select(name => (Name: name, Value: Environment.GetEnvironmentVariable(name)))
                .Where(pair => pair.Value != null)
                .ToArray();
            startInfo.Environment.Clear();
            foreach (var (name, value) in inherited)
            {
                startInfo.Environment[name] = value;
            }
        }

        private sealed class FairAsyncAdmission
        {
            private readonly object _gate = new();
            private readonly int _maximumOutstanding;
            private Task _tail = Task.CompletedTask;
            private int _outstanding;

            public FairAsyncAdmission(int maximumQueued)
            {
                _maximumOutstanding = maximumQueued + 1;
            }

            public async Task<IDisposable> AcquireAsync(TimeSpan timeout, CancellationToken cancellationToken)
            {
                if (timeout <= TimeSpan.Zero)
                {
                    throw new TimeoutException("D8 worker admission deadline expired");
                }

                var turn = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                Task predecessor;
                lock (_gate)
                {
                    if (_outstanding >= _maximumOutstanding)
                    {
                        throw new TerminalEvidenceWorkerUnavailableException();
                    }

                    _outstanding++;
                    predecessor = _tail;
                    _tail = turn.Task;
                }

                try
                {
                    await predecessor.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
                    return new AdmissionLease(this, turn);
                }
                catch
                {
                    lock (_gate)
                    {
                        _outstanding--;
                    }

                    // A cancelled/timed-out ticket remains in the FIFO chain only until its
                    // predecessor releases. It then drains without holding capacity or blocking
                    // later tickets. Every wait has the caller's absolute remaining budget.
                    if (predecessor.IsCompleted)
                    {
                        turn.TrySetResult(true);
                    }
                    else
                    {
                        _ = predecessor.ContinueWith(
                            static (_, state) => ((TaskCompletionSource<bool>)state).TrySetResult(true),
                            turn,
                            CancellationToken.None,
                            TaskContinuationOptions.ExecuteSynchronously,
                            TaskScheduler.Default);
                    }

                    throw;
                }
            }

            public Task WaitForIdleAsync(TimeSpan timeout)
            {
                Task tail;
                lock (_gate)
                {
                    tail = _tail;
                }

                return tail.WaitAsync(timeout);
            }

            private void Release(TaskCompletionSource<bool> turn)
            {
                lock (_gate)
                {
                    _outstanding--;
                    turn.TrySetResult(true);
                }
            }

            private sealed class AdmissionLease : IDisposable
            {
                private readonly TaskCompletionSource<bool> _turn;
                private FairAsyncAdmission _owner;

                public AdmissionLease(FairAsyncAdmission owner, TaskCompletionSource<bool> turn)
                {
                    _owner = owner;
                    _turn = turn;
                }

                public void Dispose()
                {
                    Interlocked.Exchange(ref _owner, null)?.Release(_turn);
                }
            }
        }

        private static TimeSpan Remaining(long started, TimeSpan budget) => budget - Stopwatch.GetElapsedTime(started);
    }

    public sealed class TerminalEvidenceWorkerResult
    {
        public TerminalEvidenceWorkerResult(bool redeemed, IndexerSearchTerminalRecord record)
        {
            Redeemed = redeemed;
            Record = record;
        }

        public bool Redeemed { get; }
        public IndexerSearchTerminalRecord Record { get; }
    }

    public sealed class TerminalEvidenceWorkerException : Exception
    {
        public TerminalEvidenceWorkerException(string message, Exception innerException, bool workerReaped)
            : base(message, innerException)
        {
            WorkerReaped = workerReaped;
        }

        public bool WorkerReaped { get; }
    }

    public sealed class TerminalEvidenceWorkerUnavailableException : Exception
    {
        public TerminalEvidenceWorkerUnavailableException()
            : base("D8 evidence worker is busy or unhealthy")
        {
        }
    }
}
