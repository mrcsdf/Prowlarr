using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NzbDrone.Core.IndexerSearch;

namespace NzbDrone.Host
{
    /// <summary>
    /// Production journal adapter. All journal SQLite mutations run in a supervised D8 worker
    /// process; callers supply the remaining budget from their single operation deadline.
    /// </summary>
    public sealed class D8SearchOperationJournalWorkerClient : ID8SearchOperationJournal
    {
        private readonly TerminalEvidenceWorkerClient _workerClient;

        public D8SearchOperationJournalWorkerClient(TerminalEvidenceWorkerClient workerClient)
        {
            _workerClient = workerClient;
        }

        public async Task<D8SearchOperationStartResult> StartAsync(
            string callerIdentity,
            string idempotencyKey,
            int indexerId,
            string operationId,
            string processInstanceId,
            string correlationId,
            DateTimeOffset startedAt,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            var response = await ExecuteAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "start",
                CallerIdentity = callerIdentity,
                IdempotencyKey = idempotencyKey,
                IndexerId = indexerId,
                OperationId = operationId,
                ProcessInstanceId = processInstanceId,
                CorrelationId = correlationId,
                StartedAtUnixMs = startedAt.ToUnixTimeMilliseconds()
                },
                remainingDeadline,
                cancellationToken).ConfigureAwait(false);
            return JsonSerializer.Deserialize<D8SearchOperationStartResult>(RequirePayload(response));
        }

        public Task RecordAttemptIntentAsync(
            string operationId,
            ObservedAttempt attempt,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken) =>
            ExecuteVoidAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "attempt-intent",
                OperationId = operationId,
                Attempt = attempt
                },
                remainingDeadline,
                cancellationToken);

        public Task RecordPreDispatchBoundaryAsync(
            string operationId,
            Guid attemptId,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken) =>
            ExecuteVoidAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "pre-dispatch-boundary",
                OperationId = operationId,
                AttemptId = attemptId.ToString("D")
                },
                remainingDeadline,
                cancellationToken);

        public Task TerminalizeAttemptAsync(
            string operationId,
            ObservedAttempt attempt,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken) =>
            ExecuteVoidAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "attempt-terminal",
                OperationId = operationId,
                Attempt = attempt
                },
                remainingDeadline,
                cancellationToken);

        public async Task<IndexerSearchTerminalRecord> CompleteAsync(
            string operationId,
            IndexerSearchTerminalRecord terminalRecord,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            var response = await ExecuteAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "finalize",
                OperationId = operationId,
                TerminalRecord = terminalRecord
                },
                remainingDeadline,
                cancellationToken).ConfigureAwait(false);
            var record = Newtonsoft.Json.JsonConvert.DeserializeObject<IndexerSearchTerminalRecord>(RequirePayload(response));
            if (record == null || record.OperationId != operationId)
            {
                throw new InvalidOperationException("D8 journal worker returned an invalid terminal record");
            }

            record.CommitAcknowledged = true;
            return record;
        }

        public async Task<IndexerSearchTerminalRecord> FailAsync(
            string operationId,
            string callerIdentity,
            string correlationId,
            DateTimeOffset endedAt,
            string failureReason,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            var response = await ExecuteAsync(
                new D8SearchJournalWorkerRequest
                {
                Version = 1,
                Operation = "fail",
                OperationId = operationId,
                CallerIdentity = callerIdentity,
                CorrelationId = correlationId,
                EndedAtUnixMs = endedAt.ToUnixTimeMilliseconds(),
                FailureReason = failureReason
                },
                remainingDeadline,
                cancellationToken).ConfigureAwait(false);
            var record = Newtonsoft.Json.JsonConvert.DeserializeObject<IndexerSearchTerminalRecord>(RequirePayload(response));
            if (record == null || record.OperationId != operationId)
            {
                throw new InvalidOperationException("D8 journal worker returned an invalid failed terminal record");
            }

            record.CommitAcknowledged = true;
            return record;
        }

        public Task RecoverInterruptedAsync(
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken) =>
            ExecuteVoidAsync(
                new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                remainingDeadline,
                cancellationToken);

        private async Task ExecuteVoidAsync(
            D8SearchJournalWorkerRequest request,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            _ = await ExecuteAsync(request, remainingDeadline, cancellationToken).ConfigureAwait(false);
        }

        private async Task<D8SearchJournalWorkerResponse> ExecuteAsync(
            D8SearchJournalWorkerRequest request,
            TimeSpan remainingDeadline,
            CancellationToken cancellationToken)
        {
            var response = await _workerClient.RunJournalCommandAsync(request, remainingDeadline, cancellationToken).ConfigureAwait(false);
            if (response.ResultCode != "ok")
            {
                throw new D8SearchJournalException(response.ResultCode);
            }

            return response;
        }

        private static string RequirePayload(D8SearchJournalWorkerResponse response) =>
            !string.IsNullOrWhiteSpace(response.PayloadJson)
                ? response.PayloadJson
                : throw new InvalidOperationException("D8 journal worker response is missing its payload");
    }
}
