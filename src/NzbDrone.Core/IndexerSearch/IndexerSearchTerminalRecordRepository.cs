using System;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Newtonsoft.Json;
using NzbDrone.Core.Datastore;

namespace NzbDrone.Core.IndexerSearch
{
    public sealed class IndexerSearchTerminalRecordRepository : IIndexerSearchTerminalRecordRepository
    {
        private static readonly Regex SafeVersionPattern = new("^[A-Za-z0-9.+-]{1,64}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex CorrelationTokenPattern = new("^d8b3-(?:[0-9a-f]{32}|[A-Za-z0-9_-]{43})$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex FailureCodePattern = new("^[A-Za-z0-9_.,+-]{1,128}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private readonly IMainDatabase _database;
        private readonly ITerminalEvidenceSqliteConnectionFactory _sqliteConnections;

        public IndexerSearchTerminalRecordRepository(
            IMainDatabase database,
            ITerminalEvidenceSqliteConnectionFactory sqliteConnections)
        {
            _database = database;
            _sqliteConnections = sqliteConnections;
        }

        public IndexerSearchTerminalRecord Save(IndexerSearchTerminalRecord record)
        {
            record.StartedAt = Normalize(record.StartedAt);
            record.EndedAt = Normalize(record.EndedAt);
            record.Attempts = record.Attempts?.Select(attempt => attempt with
            {
                StartedAt = Normalize(attempt.StartedAt),
                CompletedAt = attempt.CompletedAt.HasValue ? Normalize(attempt.CompletedAt.Value) : null
            }).ToArray();
            Validate(record);
            var attemptsJson = JsonConvert.SerializeObject(record.Attempts);
            var serialized = JsonConvert.SerializeObject(record);
            if (Encoding.UTF8.GetByteCount(serialized) > IndexerSearchTerminalContract.MaximumRecordBytes ||
                Encoding.UTF8.GetByteCount(attemptsJson) > IndexerSearchTerminalContract.MaximumRecordBytes)
            {
                throw new InvalidOperationException("Terminal evidence exceeds the bounded record size");
            }

            if (_database.DatabaseType is not (DatabaseType.SQLite or DatabaseType.PostgreSQL))
            {
                throw new NotSupportedException("Terminal evidence supports only SQLite and PostgreSQL");
            }

            using var connection = _database.OpenConnection();
            RequireEligibleStorage(connection);
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

            // Retain at most the configured age. Deletion and insertion share the transaction.
            connection.Execute("DELETE FROM \"IndexerSearchTerminalRecords\" WHERE \"EndedAtUnixMs\" < @Cutoff",
                new { Cutoff = DateTimeOffset.UtcNow.AddDays(-IndexerSearchTerminalContract.RetentionDays).ToUnixTimeMilliseconds() },
                transaction);
            connection.Execute("""
                INSERT INTO "IndexerSearchTerminalRecords"
                ("OperationId", "SchemaVersion", "ApplicationVersion", "ProcessInstanceId", "RequestCorrelationId", "IndexerId", "OperationType", "StartedAtUnixMs", "EndedAtUnixMs", "Outcome", "FailureReason", "ResultCount", "AttemptsJson")
                VALUES
                (@OperationId, @SchemaVersion, @ApplicationVersion, @ProcessInstanceId, @RequestCorrelationId, @IndexerId, @OperationType, @StartedAtUnixMs, @EndedAtUnixMs, @Outcome, @FailureReason, @ResultCount, @AttemptsJson)
                """,
                new
                {
                    record.OperationId,
                    record.SchemaVersion,
                    record.ApplicationVersion,
                    record.ProcessInstanceId,
                    record.RequestCorrelationId,
                    record.IndexerId,
                    record.OperationType,
                    StartedAtUnixMs = record.StartedAt.ToUnixTimeMilliseconds(),
                    EndedAtUnixMs = record.EndedAt.ToUnixTimeMilliseconds(),
                    record.Outcome,
                    record.FailureReason,
                    record.ResultCount,
                    AttemptsJson = attemptsJson
                },
                transaction);
            transaction.Commit();
            record.CommitAcknowledged = true;
            return record;
        }

        public IndexerSearchTerminalRecord Get(string operationId)
        {
            return Get(operationId, CancellationToken.None);
        }

        public IndexerSearchTerminalRecord Get(string operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Guid.TryParseExact(operationId, "D", out _))
            {
                return null;
            }

            IDbConnection openedConnection;
            try
            {
                openedConnection = OpenConnection(cancellationToken);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();
            using var connection = openedConnection;
            using var interruption = connection is SQLiteConnection sqliteConnection
                ? cancellationToken.Register(() => sqliteConnection.Cancel())
                : default;
            try
            {
                RequireEligibleStorage(connection);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (NotSupportedException)
            {
                return null;
            }

            cancellationToken.ThrowIfCancellationRequested();
            TerminalRow row;
            try
            {
                row = connection.QuerySingleOrDefault<TerminalRow>(new CommandDefinition("""
                SELECT "OperationId", "SchemaVersion", "ApplicationVersion", "ProcessInstanceId", "RequestCorrelationId", "IndexerId", "OperationType", "StartedAtUnixMs", "EndedAtUnixMs", "Outcome", "FailureReason", "ResultCount", "AttemptsJson"
                FROM "IndexerSearchTerminalRecords"
                WHERE "OperationId" = @OperationId
                    AND "EndedAtUnixMs" >= @Cutoff
                    AND length(CAST("SchemaVersion" AS BLOB))
                      + length(CAST("ApplicationVersion" AS BLOB))
                      + length(CAST("ProcessInstanceId" AS BLOB))
                      + length(CAST("RequestCorrelationId" AS BLOB))
                      + length(CAST("OperationType" AS BLOB))
                      + length(CAST("Outcome" AS BLOB))
                      + length(CAST(COALESCE("FailureReason", '') AS BLOB))
                      + length(CAST("AttemptsJson" AS BLOB)) <= @MaximumRecordBytes
                """,
                new
                {
                    OperationId = operationId,
                    Cutoff = DateTimeOffset.UtcNow.AddDays(-IndexerSearchTerminalContract.RetentionDays).ToUnixTimeMilliseconds(),
                    MaximumRecordBytes = IndexerSearchTerminalContract.MaximumRecordBytes
                },
                commandTimeout: 2,
                cancellationToken: cancellationToken));
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (row == null)
            {
                return null;
            }

            return ToRecord(row);
        }

        public async Task<IndexerSearchTerminalRecord> GetAsync(string operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Guid.TryParseExact(operationId, "D", out _) || _database.DatabaseType != DatabaseType.SQLite)
            {
                return null;
            }

            IDbConnection openedConnection;
            try
            {
                openedConnection = OpenConnection(cancellationToken);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();
            using var connection = openedConnection;
            using var interruption = connection is SQLiteConnection sqliteConnection
                ? cancellationToken.Register(() => sqliteConnection.Cancel())
                : default;

            try
            {
                if (!string.Equals(
                    await connection.ExecuteScalarAsync<string>(
                        new CommandDefinition("PRAGMA journal_mode", cancellationToken: cancellationToken)).ConfigureAwait(false),
                    "wal",
                    StringComparison.OrdinalIgnoreCase) ||
                    await connection.ExecuteScalarAsync<int>(
                        new CommandDefinition("PRAGMA synchronous", cancellationToken: cancellationToken)).ConfigureAwait(false) != 2)
                {
                    return null;
                }

                var row = await connection.QuerySingleOrDefaultAsync<TerminalRow>(new CommandDefinition("""
                    SELECT "OperationId", "SchemaVersion", "ApplicationVersion", "ProcessInstanceId", "RequestCorrelationId", "IndexerId", "OperationType", "StartedAtUnixMs", "EndedAtUnixMs", "Outcome", "FailureReason", "ResultCount", "AttemptsJson"
                    FROM "IndexerSearchTerminalRecords"
                    WHERE "OperationId" = @OperationId
                        AND "EndedAtUnixMs" >= @Cutoff
                        AND length(CAST("SchemaVersion" AS BLOB))
                          + length(CAST("ApplicationVersion" AS BLOB))
                          + length(CAST("ProcessInstanceId" AS BLOB))
                          + length(CAST("RequestCorrelationId" AS BLOB))
                          + length(CAST("OperationType" AS BLOB))
                          + length(CAST("Outcome" AS BLOB))
                          + length(CAST(COALESCE("FailureReason", '') AS BLOB))
                          + length(CAST("AttemptsJson" AS BLOB)) <= @MaximumRecordBytes
                    """,
                    new
                    {
                        OperationId = operationId,
                        Cutoff = DateTimeOffset.UtcNow.AddDays(-IndexerSearchTerminalContract.RetentionDays).ToUnixTimeMilliseconds(),
                        MaximumRecordBytes = IndexerSearchTerminalContract.MaximumRecordBytes
                    },
                    commandTimeout: 2,
                    cancellationToken: cancellationToken)).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                return row == null ? null : ToRecord(row);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
        }

        private static IndexerSearchTerminalRecord ToRecord(TerminalRow row)
        {
            if (Encoding.UTF8.GetByteCount(row.AttemptsJson ?? string.Empty) > IndexerSearchTerminalContract.MaximumRecordBytes)
            {
                return null;
            }

            ObservedAttempt[] attempts;
            try
            {
                attempts = JsonConvert.DeserializeObject<ObservedAttempt[]>(row.AttemptsJson);
            }
            catch (JsonException)
            {
                return null;
            }

            if (attempts == null || attempts.Length > IndexerSearchTerminalContract.MaximumAttempts)
            {
                return null;
            }

            try
            {
                var record = new IndexerSearchTerminalRecord
                {
                    OperationId = row.OperationId,
                    SchemaVersion = row.SchemaVersion,
                    ApplicationVersion = row.ApplicationVersion,
                    ProcessInstanceId = row.ProcessInstanceId,
                    RequestCorrelationId = row.RequestCorrelationId,
                    IndexerId = row.IndexerId,
                    OperationType = row.OperationType,
                    StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(row.StartedAtUnixMs),
                    EndedAt = DateTimeOffset.FromUnixTimeMilliseconds(row.EndedAtUnixMs),
                    Outcome = row.Outcome,
                    FailureReason = row.FailureReason,
                    ResultCount = row.ResultCount,
                    Attempts = attempts,
                    CommitAcknowledged = true
                };
                Validate(record);
                return record;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }

        private static void Validate(IndexerSearchTerminalRecord record)
        {
            if (record == null || !Guid.TryParseExact(record.OperationId, "D", out _) ||
                record.SchemaVersion != IndexerSearchTerminalContract.SchemaVersion ||
                !SafeVersionPattern.IsMatch(record.ApplicationVersion ?? string.Empty) ||
                !Guid.TryParseExact(record.ProcessInstanceId, "D", out _) ||
                !CorrelationTokenPattern.IsMatch(record.RequestCorrelationId ?? string.Empty) ||
                record.IndexerId <= 0 || record.OperationType != IndexerSearchTerminalContract.OperationType ||
                record.StartedAt.Offset != TimeSpan.Zero || record.EndedAt.Offset != TimeSpan.Zero || record.EndedAt < record.StartedAt ||
                record.Outcome is not ("SUCCEEDED" or "FAILED" or "NO_SEARCH" or "TIMED_OUT" or "CANCELLED" or "INDETERMINATE") || record.ResultCount < 0 ||
                (record.Outcome == "SUCCEEDED" && !string.IsNullOrEmpty(record.FailureReason)) ||
                (record.Outcome != "SUCCEEDED" && !FailureCodePattern.IsMatch(record.FailureReason ?? string.Empty)) ||
                record.Attempts == null || record.Attempts.Count > IndexerSearchTerminalContract.MaximumAttempts ||
                (record.Outcome == "SUCCEEDED" && record.Attempts.Count == 0) ||
                record.Attempts.Any(a => a == null || a.Id == Guid.Empty || a.StartedAt.Offset != TimeSpan.Zero ||
                    !a.CompletedAt.HasValue || a.CompletedAt.Value.Offset != TimeSpan.Zero || a.CompletedAt.Value < a.StartedAt ||
                    a.StartedAt < record.StartedAt || a.CompletedAt.Value > record.EndedAt ||
                    a.Method is not ("GET" or "POST") || string.IsNullOrWhiteSpace(a.Host) || a.Host.Length > 253 ||
                    Uri.CheckHostName(a.Host) == UriHostNameType.Unknown ||
                    a.ExceptionType?.Length > 128 || a.StatusCode is < 100 or > 599))
            {
                throw new InvalidOperationException("Terminal evidence does not satisfy the versioned bounded contract");
            }

            if (record.Attempts.Select(a => a.Id).Distinct().Count() != record.Attempts.Count ||
                (record.Outcome == "SUCCEEDED" && record.Attempts.Any(a => a.StatusCode is not (>= 200 and <= 299) || a.ExceptionType != null)))
            {
                throw new InvalidOperationException("Terminal evidence contains duplicate or unsuccessful attempts");
            }
        }

        private static DateTimeOffset Normalize(DateTimeOffset value) => DateTimeOffset.FromUnixTimeMilliseconds(value.ToUnixTimeMilliseconds());

        private IDbConnection OpenConnection(CancellationToken cancellationToken)
        {
            if (_sqliteConnections != null && _database.DatabaseType == DatabaseType.SQLite)
            {
                return _sqliteConnections.OpenConnection(cancellationToken);
            }

            return _database.OpenConnection();
        }

        private void RequireEligibleStorage(IDbConnection connection)
        {
            if (_database.DatabaseType != DatabaseType.SQLite)
            {
                throw new NotSupportedException("Terminal evidence is eligible only on verified SQLite WAL/FULL storage; PostgreSQL is not enabled for this contract");
            }

            var journalMode = connection.ExecuteScalar<string>("PRAGMA journal_mode");
            var synchronous = connection.ExecuteScalar<int>("PRAGMA synchronous");
            if (!string.Equals(journalMode, "wal", StringComparison.OrdinalIgnoreCase) || synchronous != 2)
            {
                throw new NotSupportedException("Terminal evidence requires the active SQLite connection to use journal_mode=WAL and synchronous=FULL");
            }
        }

        private sealed class TerminalRow
        {
            public string OperationId { get; set; }
            public string SchemaVersion { get; set; }
            public string ApplicationVersion { get; set; }
            public string ProcessInstanceId { get; set; }
            public string RequestCorrelationId { get; set; }
            public int IndexerId { get; set; }
            public string OperationType { get; set; }
            public long StartedAtUnixMs { get; set; }
            public long EndedAtUnixMs { get; set; }
            public string Outcome { get; set; }
            public string FailureReason { get; set; }
            public int ResultCount { get; set; }
            public string AttemptsJson { get; set; }
        }
    }
}
