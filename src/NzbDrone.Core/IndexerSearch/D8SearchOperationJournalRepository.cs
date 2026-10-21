using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Dapper;
using Newtonsoft.Json;
using NzbDrone.Common.EnvironmentInfo;
using NzbDrone.Core.Datastore;

namespace NzbDrone.Core.IndexerSearch
{
    public sealed class D8SearchOperationJournalRepository
    {
        private static readonly Regex IdempotencyPattern = new("^[A-Za-z0-9._~-]{1,128}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex CorrelationPattern = new("^d8b3-[A-Za-z0-9_-]{43}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private readonly IMainDatabase _database;
        private readonly IIndexerSearchTerminalRecordRepository _terminalRecords;

        public D8SearchOperationJournalRepository(IMainDatabase database, IIndexerSearchTerminalRecordRepository terminalRecords)
        {
            _database = database;
            _terminalRecords = terminalRecords;
        }

        public D8SearchOperationStartResult Start(string callerIdentity, string idempotencyKey, int indexerId, string operationId, string processInstanceId, string correlationId, DateTimeOffset startedAt)
        {
            if (_database.DatabaseType != DatabaseType.SQLite || string.IsNullOrWhiteSpace(callerIdentity) || callerIdentity.Length > 256 ||
                string.IsNullOrEmpty(idempotencyKey) || !IdempotencyPattern.IsMatch(idempotencyKey) || indexerId <= 0 ||
                !Guid.TryParseExact(operationId, "D", out _) || !Guid.TryParseExact(processInstanceId, "D", out _) ||
                !CorrelationPattern.IsMatch(correlationId ?? string.Empty) || startedAt.Offset != TimeSpan.Zero)
            {
                throw new InvalidOperationException("D8 search initiation is outside the bounded SQLite operation contract");
            }

            using var connection = _database.OpenConnection();
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var existing = connection.QuerySingleOrDefault<OperationRow>(
                """
                SELECT * FROM "D8SearchOperations" WHERE "CallerIdentity" = @CallerIdentity AND "IdempotencyKey" = @IdempotencyKey
                """,
                new { CallerIdentity = callerIdentity, IdempotencyKey = idempotencyKey },
                transaction);
            if (existing != null)
            {
                if (existing.IndexerId != indexerId)
                {
                    throw new InvalidOperationException("D8 idempotency key is already bound to a different indexer");
                }

                transaction.Commit();
                return new D8SearchOperationStartResult(ToEntry(existing), false);
            }

            var entry = new D8SearchOperationJournalEntry
            {
                OperationId = operationId,
                CallerIdentity = callerIdentity,
                IdempotencyKey = idempotencyKey,
                CorrelationId = correlationId,
                IndexerId = indexerId,
                ProcessInstanceId = processInstanceId,
                StartedAt = Normalize(startedAt),
                State = "RUNNING",
                Attempts = Array.Empty<ObservedAttempt>()
            };

            try
            {
                Insert(connection, transaction, entry);
                transaction.Commit();
                return new D8SearchOperationStartResult(entry, true);
            }
            catch (Exception)
            {
                transaction.Rollback();
                var duplicate = GetByIdempotency(callerIdentity, idempotencyKey);
                if (duplicate == null || duplicate.IndexerId != indexerId)
                {
                    throw;
                }

                return new D8SearchOperationStartResult(duplicate, false);
            }
        }

        public void AttemptStarted(string operationId, ObservedAttempt attempt) => MutateAttempt(operationId, attempt, true);

        public void RecordAttemptIntent(string operationId, ObservedAttempt attempt)
        {
            if (attempt == null || attempt.Id == Guid.Empty || attempt.CompletedAt.HasValue || attempt.StartedAt.Offset != TimeSpan.Zero ||
                attempt.Method is not ("GET" or "POST") || string.IsNullOrWhiteSpace(attempt.Host) || attempt.Host.Length > 253 ||
                attempt.ExceptionType != null || attempt.StatusCode.HasValue)
            {
                throw new InvalidOperationException("D8 attempt intent is invalid");
            }

            using var connection = _database.OpenConnection();
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var row = connection.QuerySingleOrDefault<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId", new { OperationId = operationId }, transaction)
                ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            if (row.State != "RUNNING")
            {
                throw new InvalidOperationException("D8 search operation is no longer accepting attempts");
            }

            var attempts = JsonConvert.DeserializeObject<ObservedAttempt[]>(row.AttemptsJson) ?? Array.Empty<ObservedAttempt>();
            if (attempts.Length >= IndexerSearchTerminalContract.MaximumAttempts || attempts.Any(item => item.Id == attempt.Id))
            {
                throw new InvalidOperationException("D8 attempt intent is duplicate or exceeds the bound");
            }

            var normalized = attempt with { StartedAt = Normalize(attempt.StartedAt) };
            connection.Execute(
                "INSERT INTO \"D8SearchAttemptProgress\" (\"OperationId\", \"AttemptId\", \"Stage\", \"AttemptJson\") VALUES (@OperationId, @AttemptId, 'INTENT', @AttemptJson)",
                new { OperationId = operationId, AttemptId = attempt.Id.ToString("D"), AttemptJson = JsonConvert.SerializeObject(normalized) },
                transaction);
            connection.Execute(
                "UPDATE \"D8SearchOperations\" SET \"AttemptsJson\" = @AttemptsJson WHERE \"OperationId\" = @OperationId",
                new { OperationId = operationId, AttemptsJson = JsonConvert.SerializeObject(attempts.Append(normalized)) },
                transaction);
            transaction.Commit();
        }

        public void RecordPreDispatchBoundary(string operationId, Guid attemptId)
        {
            using var connection = _database.OpenConnection();
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var progress = connection.QuerySingleOrDefault<AttemptProgressRow>(
                "SELECT * FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId",
                new { OperationId = operationId, AttemptId = attemptId.ToString("D") },
                transaction);
            if (progress == null || progress.Stage != "INTENT")
            {
                throw new InvalidOperationException("D8 pre-dispatch boundary requires one durable attempt intent");
            }

            connection.Execute(
                "UPDATE \"D8SearchAttemptProgress\" SET \"Stage\" = 'BOUNDARY_RECORDED' WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId AND \"Stage\" = 'INTENT'",
                new { OperationId = operationId, AttemptId = attemptId.ToString("D") },
                transaction);
            transaction.Commit();
        }

        public void TerminalizeAttempt(string operationId, ObservedAttempt attempt)
        {
            if (attempt == null || attempt.Id == Guid.Empty || !attempt.CompletedAt.HasValue ||
                attempt.StartedAt.Offset != TimeSpan.Zero || attempt.CompletedAt.Value.Offset != TimeSpan.Zero ||
                attempt.CompletedAt.Value < attempt.StartedAt || attempt.Method is not ("GET" or "POST") ||
                string.IsNullOrWhiteSpace(attempt.Host) || attempt.Host.Length > 253 || attempt.ExceptionType?.Length > 128)
            {
                throw new InvalidOperationException("D8 attempt terminal record is invalid");
            }

            using var connection = _database.OpenConnection();
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var row = connection.QuerySingleOrDefault<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId", new { OperationId = operationId }, transaction)
                ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            var progress = connection.QuerySingleOrDefault<AttemptProgressRow>(
                "SELECT * FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId",
                new { OperationId = operationId, AttemptId = attempt.Id.ToString("D") },
                transaction);
            if (row.State != "RUNNING" || progress == null || progress.Stage != "BOUNDARY_RECORDED" || !attempt.CompletedAt.HasValue)
            {
                throw new InvalidOperationException("D8 attempt terminalization requires a recorded pre-dispatch boundary");
            }

            var attempts = JsonConvert.DeserializeObject<ObservedAttempt[]>(row.AttemptsJson) ?? Array.Empty<ObservedAttempt>();
            var index = Array.FindIndex(attempts, item => item.Id == attempt.Id);
            if (index < 0 || attempts[index].CompletedAt.HasValue)
            {
                throw new InvalidOperationException("D8 attempt terminalization has no unique durable intent");
            }

            if (attempts[index].StartedAt != Normalize(attempt.StartedAt) || attempts[index].Method != attempt.Method ||
                attempts[index].Host != attempt.Host)
            {
                throw new InvalidOperationException("D8 attempt terminal record does not match its durable intent");
            }

            attempts[index] = attempt with
            {
                StartedAt = Normalize(attempt.StartedAt),
                CompletedAt = Normalize(attempt.CompletedAt.Value)
            };
            connection.Execute(
                "UPDATE \"D8SearchOperations\" SET \"AttemptsJson\" = @Json WHERE \"OperationId\" = @OperationId",
                new { OperationId = operationId, Json = JsonConvert.SerializeObject(attempts) },
                transaction);
            connection.Execute(
                "UPDATE \"D8SearchAttemptProgress\" SET \"Stage\" = 'TERMINAL', \"AttemptJson\" = @AttemptJson WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId",
                new { OperationId = operationId, AttemptId = attempt.Id.ToString("D"), AttemptJson = JsonConvert.SerializeObject(attempts[index]) },
                transaction);
            transaction.Commit();
        }

        public void AttemptFinished(string operationId, ObservedAttempt attempt) => MutateAttempt(operationId, attempt, false);

        public IndexerSearchTerminalRecord Complete(string operationId, IndexerSearchTerminalRecord terminalRecord)
        {
            var entry = Get(operationId) ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            if (entry.State != "RUNNING" || terminalRecord.OperationId != operationId ||
                terminalRecord.RequestCorrelationId != entry.CorrelationId || terminalRecord.ProcessInstanceId != entry.ProcessInstanceId ||
                terminalRecord.IndexerId != entry.IndexerId || terminalRecord.StartedAt != entry.StartedAt)
            {
                throw new InvalidOperationException("D8 search terminal record does not match its durable operation");
            }

            terminalRecord.Attempts = entry.Attempts;
            if (entry.Attempts.Count == 0)
            {
                terminalRecord.Outcome = "NO_SEARCH";
                terminalRecord.FailureReason ??= "no_attempt_dispatched";
            }
            else if (terminalRecord.Outcome == "NO_SEARCH")
            {
                terminalRecord.Outcome = "INDETERMINATE";
                terminalRecord.FailureReason = "attempt_journal_mismatch";
            }

            SetState(operationId, "FINALIZING", terminalRecord.EndedAt, terminalRecord.FailureReason, terminalRecord.ResultCount, entry.Attempts);
            _terminalRecords.Save(terminalRecord);
            SetState(operationId, terminalRecord.Outcome, terminalRecord.EndedAt, terminalRecord.FailureReason, terminalRecord.ResultCount, entry.Attempts);
            return terminalRecord;
        }

        public IndexerSearchTerminalRecord CompleteFromWorker(string operationId, IndexerSearchTerminalRecord terminalRecord)
        {
            var entry = Get(operationId) ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            using var connection = _database.OpenConnection();
            var progress = connection.Query<AttemptProgressRow>(
                "SELECT \"AttemptId\", \"Stage\", \"AttemptJson\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId",
                new { OperationId = operationId }).ToArray();
            if (progress.Length != entry.Attempts.Count || progress.Any(item => item.Stage != "TERMINAL") ||
                entry.Attempts.Any(attempt => !progress.Any(item => item.AttemptId == attempt.Id.ToString("D") &&
                    JsonConvert.DeserializeObject<ObservedAttempt>(item.AttemptJson) == attempt)))
            {
                throw new InvalidOperationException("D8 worker finalization has unresolved or unaccounted attempt progress");
            }

            return Complete(operationId, terminalRecord);
        }

        public IndexerSearchTerminalRecord Fail(string operationId, string callerIdentity, string correlationId, DateTimeOffset endedAt, string failureReason)
        {
            var entry = Get(operationId) ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            if (entry.CallerIdentity != callerIdentity || entry.CorrelationId != correlationId)
            {
                throw new InvalidOperationException("D8 search failure scope does not match its durable operation");
            }

            if (entry.State == "FINALIZING")
            {
                var existing = _terminalRecords.Get(operationId);
                if (existing == null)
                {
                    throw new InvalidOperationException("D8 finalization has no durable terminal record");
                }

                SetState(operationId, existing.Outcome, existing.EndedAt, existing.FailureReason, existing.ResultCount, existing.Attempts);
                return existing;
            }

            if (entry.State != "RUNNING")
            {
                var terminal = _terminalRecords.Get(operationId);
                if (terminal != null)
                {
                    return terminal;
                }

                throw new InvalidOperationException("D8 search operation is not eligible for failure terminalization");
            }

            var attempts = entry.Attempts.Select(attempt => attempt.CompletedAt.HasValue ? attempt : attempt with
            {
                ExceptionType = "OperationInterrupted",
                CompletedAt = endedAt
            }).ToArray();
            foreach (var attempt in attempts.Where(attempt => !entry.Attempts.Any(original => original.Id == attempt.Id && original.CompletedAt.HasValue)))
            {
                MutateAttempt(operationId, attempt, false);
            }

            var reason = string.IsNullOrWhiteSpace(failureReason) ? "operation_failed" : failureReason;
            if (reason.Length > 128)
            {
                reason = reason.Substring(0, 128);
            }

            return Complete(operationId, new IndexerSearchTerminalRecord
            {
                OperationId = entry.OperationId,
                SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                ApplicationVersion = BuildInfo.Release,
                ProcessInstanceId = entry.ProcessInstanceId,
                RequestCorrelationId = entry.CorrelationId,
                IndexerId = entry.IndexerId,
                OperationType = IndexerSearchTerminalContract.OperationType,
                StartedAt = entry.StartedAt,
                EndedAt = endedAt,
                Outcome = attempts.Length == 0 ? "NO_SEARCH" : "INDETERMINATE",
                FailureReason = attempts.Length == 0 ? reason : "operation_interrupted",
                ResultCount = 0,
                Attempts = attempts
            });
        }

        public void RecoverInterrupted()
        {
            using var connection = _database.OpenConnection();
            var unfinished = connection.Query<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"State\" IN ('RUNNING', 'FINALIZING')").Select(ToEntry).ToArray();
            foreach (var entry in unfinished)
            {
                var existing = _terminalRecords.Get(entry.OperationId);
                if (existing != null)
                {
                    SetState(entry.OperationId, existing.Outcome, existing.EndedAt, existing.FailureReason, existing.ResultCount, existing.Attempts);
                    continue;
                }

                var endedAt = DateTimeOffset.UtcNow;
                var attempts = entry.Attempts.Select(attempt => attempt.CompletedAt.HasValue ? attempt : attempt with
                {
                    ExceptionType = "ProcessInterrupted",
                    CompletedAt = endedAt
                }).ToArray();
                var noSearch = attempts.Length == 0;
                var terminal = new IndexerSearchTerminalRecord
                {
                    OperationId = entry.OperationId,
                    SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                    ApplicationVersion = BuildInfo.Release,
                    ProcessInstanceId = entry.ProcessInstanceId,
                    RequestCorrelationId = entry.CorrelationId,
                    IndexerId = entry.IndexerId,
                    OperationType = IndexerSearchTerminalContract.OperationType,
                    StartedAt = entry.StartedAt,
                    EndedAt = endedAt,
                    Outcome = noSearch ? "NO_SEARCH" : "INDETERMINATE",
                    FailureReason = noSearch ? "no_attempt_dispatched" : "process_interrupted",
                    ResultCount = entry.ResultCount,
                    Attempts = attempts
                };
                _terminalRecords.Save(terminal);
                SetState(entry.OperationId, terminal.Outcome, endedAt, terminal.FailureReason, terminal.ResultCount, attempts);
            }
        }

        private void MutateAttempt(string operationId, ObservedAttempt attempt, bool start)
        {
            attempt = attempt with
            {
                StartedAt = Normalize(attempt.StartedAt),
                CompletedAt = attempt.CompletedAt.HasValue ? Normalize(attempt.CompletedAt.Value) : null
            };

            using var connection = _database.OpenConnection();
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var row = connection.QuerySingleOrDefault<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId", new { OperationId = operationId }, transaction)
                ?? throw new InvalidOperationException("D8 search operation journal row is missing");
            if (row.State != "RUNNING")
            {
                throw new InvalidOperationException("D8 search operation is no longer accepting attempts");
            }

            var attempts = JsonConvert.DeserializeObject<ObservedAttempt[]>(row.AttemptsJson) ?? Array.Empty<ObservedAttempt>();
            if (start)
            {
                if (attempts.Length >= IndexerSearchTerminalContract.MaximumAttempts || attempts.Any(item => item.Id == attempt.Id) ||
                    attempt.CompletedAt.HasValue || attempt.StartedAt.Offset != TimeSpan.Zero)
                {
                    throw new InvalidOperationException("D8 search attempt start is invalid or exceeds the bound");
                }

                attempts = attempts.Append(attempt).ToArray();
            }
            else
            {
                var index = Array.FindIndex(attempts, item => item.Id == attempt.Id);
                if (index < 0 || attempts[index].CompletedAt.HasValue || !attempt.CompletedAt.HasValue)
                {
                    throw new InvalidOperationException("D8 search attempt completion has no unique durable start");
                }

                attempts[index] = attempt;
            }

            var json = JsonConvert.SerializeObject(attempts);
            if (Encoding.UTF8.GetByteCount(json) > IndexerSearchTerminalContract.MaximumRecordBytes)
            {
                throw new InvalidOperationException("D8 search attempt journal exceeds its storage bound");
            }

            connection.Execute(
                "UPDATE \"D8SearchOperations\" SET \"AttemptsJson\" = @AttemptsJson WHERE \"OperationId\" = @OperationId",
                new { AttemptsJson = json, OperationId = operationId },
                transaction);
            transaction.Commit();
        }

        private D8SearchOperationJournalEntry Get(string operationId)
        {
            using var connection = _database.OpenConnection();
            var row = connection.QuerySingleOrDefault<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId", new { OperationId = operationId });
            return row == null ? null : ToEntry(row);
        }

        private D8SearchOperationJournalEntry GetByIdempotency(string callerIdentity, string idempotencyKey)
        {
            using var connection = _database.OpenConnection();
            var row = connection.QuerySingleOrDefault<OperationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"CallerIdentity\" = @CallerIdentity AND \"IdempotencyKey\" = @IdempotencyKey",
                new { CallerIdentity = callerIdentity, IdempotencyKey = idempotencyKey });
            return row == null ? null : ToEntry(row);
        }

        private void SetState(string operationId, string state, DateTimeOffset endedAt, string failureReason, int resultCount, System.Collections.Generic.IReadOnlyList<ObservedAttempt> attempts)
        {
            using var connection = _database.OpenConnection();
            connection.Execute("UPDATE \"D8SearchOperations\" SET \"State\" = @State, \"EndedAtUnixMs\" = @EndedAtUnixMs, \"FailureReason\" = @FailureReason, \"ResultCount\" = @ResultCount, \"AttemptsJson\" = @AttemptsJson WHERE \"OperationId\" = @OperationId",
                new
                {
                    OperationId = operationId,
                    State = state,
                    EndedAtUnixMs = endedAt.ToUnixTimeMilliseconds(),
                    FailureReason = failureReason,
                    ResultCount = resultCount,
                    AttemptsJson = JsonConvert.SerializeObject(attempts)
                });
        }

        private static void Insert(IDbConnection connection, IDbTransaction transaction, D8SearchOperationJournalEntry entry)
        {
            connection.Execute("""
                INSERT INTO "D8SearchOperations" ("OperationId", "CallerIdentity", "IdempotencyKey", "CorrelationId", "IndexerId", "ProcessInstanceId", "StartedAtUnixMs", "EndedAtUnixMs", "State", "FailureReason", "ResultCount", "AttemptsJson")
                VALUES (@OperationId, @CallerIdentity, @IdempotencyKey, @CorrelationId, @IndexerId, @ProcessInstanceId, @StartedAtUnixMs, NULL, @State, NULL, 0, @AttemptsJson)
                """,
                new
                {
                    entry.OperationId,
                    entry.CallerIdentity,
                    entry.IdempotencyKey,
                    entry.CorrelationId,
                    entry.IndexerId,
                    entry.ProcessInstanceId,
                    StartedAtUnixMs = entry.StartedAt.ToUnixTimeMilliseconds(),
                    entry.State,
                    AttemptsJson = JsonConvert.SerializeObject(entry.Attempts)
                },
                transaction);
        }

        private static DateTimeOffset Normalize(DateTimeOffset value) => DateTimeOffset.FromUnixTimeMilliseconds(value.ToUnixTimeMilliseconds());

        private static D8SearchOperationJournalEntry ToEntry(OperationRow row) => new()
        {
            OperationId = row.OperationId,
            CallerIdentity = row.CallerIdentity,
            IdempotencyKey = row.IdempotencyKey,
            CorrelationId = row.CorrelationId,
            IndexerId = row.IndexerId,
            ProcessInstanceId = row.ProcessInstanceId,
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(row.StartedAtUnixMs),
            EndedAt = row.EndedAtUnixMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(row.EndedAtUnixMs.Value) : null,
            State = row.State,
            FailureReason = row.FailureReason,
            ResultCount = row.ResultCount,
            Attempts = JsonConvert.DeserializeObject<ObservedAttempt[]>(row.AttemptsJson) ?? Array.Empty<ObservedAttempt>()
        };

        private sealed class OperationRow
        {
            public string OperationId { get; set; }
            public string CallerIdentity { get; set; }
            public string IdempotencyKey { get; set; }
            public string CorrelationId { get; set; }
            public int IndexerId { get; set; }
            public string ProcessInstanceId { get; set; }
            public long StartedAtUnixMs { get; set; }
            public long? EndedAtUnixMs { get; set; }
            public string State { get; set; }
            public string FailureReason { get; set; }
            public int ResultCount { get; set; }
            public string AttemptsJson { get; set; }
        }

        private sealed class AttemptProgressRow
        {
            public string AttemptId { get; set; }
            public string Stage { get; set; }
            public string AttemptJson { get; set; }
        }
    }
}
