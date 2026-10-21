using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace NzbDrone.Core.IndexerSearch
{
    public sealed class D8SearchOperationJournalEntry
    {
        public string OperationId { get; set; }
        public string CallerIdentity { get; set; }
        public string IdempotencyKey { get; set; }
        public string CorrelationId { get; set; }
        public int IndexerId { get; set; }
        public string ProcessInstanceId { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public DateTimeOffset? EndedAt { get; set; }
        public string State { get; set; }
        public string FailureReason { get; set; }
        public int ResultCount { get; set; }
        public IReadOnlyList<ObservedAttempt> Attempts { get; set; }
    }

    public sealed record D8SearchOperationStartResult(D8SearchOperationJournalEntry Entry, bool Created);
    public sealed record D8SearchInitiationResult(bool Started, D8SearchOperationJournalEntry Operation, IndexerSearchTerminalRecord TerminalRecord);

    public static class D8SearchJournalFailure
    {
        public const string BusyLocked = "busy-locked";
        public const string Rejected = "rejected";
        public const string Constraint = "constraint";
        public const string Schema = "schema";
        public const string Corrupt = "corrupt";
        public const string Internal = "internal";

        public static bool IsKnown(string category) => category is BusyLocked or Rejected or Constraint or Schema or Corrupt or Internal;

        public static string FromSqliteError(SQLiteErrorCode resultCode) => ((SQLiteErrorCode)((int)resultCode & 0xff)) switch
        {
            SQLiteErrorCode.Busy or SQLiteErrorCode.Locked => BusyLocked,
            SQLiteErrorCode.Constraint => Constraint,
            SQLiteErrorCode.Schema => Schema,
            SQLiteErrorCode.Corrupt or SQLiteErrorCode.NotADb => Corrupt,
            _ => Internal
        };
    }

    public class D8StorageException : Exception
    {
        public D8StorageException(string category)
            : base("D8 storage operation failed (" + (D8SearchJournalFailure.IsKnown(category) ? category : D8SearchJournalFailure.Internal) + ")")
        {
            Category = D8SearchJournalFailure.IsKnown(category) ? category : D8SearchJournalFailure.Internal;
        }

        public string Category { get; }
    }

    public sealed class D8SearchJournalException : D8StorageException
    {
        public D8SearchJournalException(string category)
            : base(category)
        {
        }
    }

    public sealed class D8SearchDeadline : IDisposable
    {
        private readonly CancellationToken _callerToken;
        private readonly CancellationTokenSource _source;
        private readonly CancellationTokenSource _cleanupSource;
        private readonly long _startedAt;
        private readonly long _durationTicks;
        private readonly long _cleanupReserveTicks;

        public D8SearchDeadline(CancellationToken callerToken, TimeSpan? duration = null)
        {
            var limit = duration ?? TimeSpan.FromSeconds(120);
            if (limit <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(duration));
            }

            _callerToken = callerToken;
            _source = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
            _cleanupSource = new CancellationTokenSource();
            _durationTicks = (long)(limit.TotalSeconds * Stopwatch.Frequency);
            _cleanupReserveTicks = (long)(TimeSpan.FromSeconds(2).TotalSeconds * Stopwatch.Frequency);
            _startedAt = Stopwatch.GetTimestamp();
            var workTicks = Math.Max(0, _durationTicks - _cleanupReserveTicks);
            _source.CancelAfter(TimeSpan.FromSeconds((double)workTicks / Stopwatch.Frequency));
            _cleanupSource.CancelAfter(limit);
        }

        public CancellationToken Token => _source.Token;
        public CancellationToken CleanupToken => _cleanupSource.Token;
        public bool CallerCancelled => _callerToken.IsCancellationRequested;
        public bool Expired => Remaining <= TimeSpan.Zero;
        public TimeSpan Remaining
        {
            get
            {
                var ticks = _durationTicks - (Stopwatch.GetTimestamp() - _startedAt);
                return ticks <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds((double)ticks / Stopwatch.Frequency);
            }
        }

        public TimeSpan WorkRemaining
        {
            get
            {
                var ticks = _durationTicks - _cleanupReserveTicks - (Stopwatch.GetTimestamp() - _startedAt);
                return ticks <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds((double)ticks / Stopwatch.Frequency);
            }
        }

        public void ThrowIfExpired()
        {
            if (Expired && !_source.IsCancellationRequested)
            {
                _source.Cancel();
            }

            _source.Token.ThrowIfCancellationRequested();
        }

        public CancellationTokenSource CreateBoundedAttempt(TimeSpan maximum)
        {
            var remaining = WorkRemaining;
            var limit = remaining < maximum ? remaining : maximum;
            var attempt = CancellationTokenSource.CreateLinkedTokenSource(_source.Token);
            if (limit <= TimeSpan.Zero)
            {
                attempt.Cancel();
            }
            else
            {
                attempt.CancelAfter(limit);
            }

            return attempt;
        }

        public void Dispose()
        {
            _source.Dispose();
            _cleanupSource.Dispose();
        }
    }

    public interface ID8SearchOperationJournal
    {
        // Idempotency keys are retained for the lifetime of their operation rows. There is no
        // automatic expiry: an ambiguous retry must never become a new search after restart.
        Task<D8SearchOperationStartResult> StartAsync(string callerIdentity, string idempotencyKey, int indexerId, string operationId, string processInstanceId, string correlationId, DateTimeOffset startedAt, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task RecordAttemptIntentAsync(string operationId, ObservedAttempt attempt, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task RecordPreDispatchBoundaryAsync(string operationId, Guid attemptId, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task TerminalizeAttemptAsync(string operationId, ObservedAttempt attempt, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task<IndexerSearchTerminalRecord> CompleteAsync(string operationId, IndexerSearchTerminalRecord terminalRecord, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task<IndexerSearchTerminalRecord> FailAsync(string operationId, string callerIdentity, string correlationId, DateTimeOffset endedAt, string failureReason, TimeSpan remainingDeadline, CancellationToken cancellationToken);
        Task RecoverInterruptedAsync(TimeSpan remainingDeadline, CancellationToken cancellationToken);
    }
}
