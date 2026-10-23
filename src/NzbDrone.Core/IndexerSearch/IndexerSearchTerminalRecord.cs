using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace NzbDrone.Core.IndexerSearch
{
    public static class IndexerSearchTerminalContract
    {
        public const string SchemaVersion = "prowlarr-indexer-search-terminal/v1";
        public const string OperationType = "newznab-basic-search";
        public const int MaximumAttempts = 64;
        public const int MaximumRecordBytes = 64 * 1024;
        public const int RetentionDays = 30;
    }

    public sealed class IndexerSearchTerminalRecord
    {
        public string OperationId { get; set; }
        public string SchemaVersion { get; set; }
        public string ApplicationVersion { get; set; }
        public string ProcessInstanceId { get; set; }
        public string RequestCorrelationId { get; set; }
        public int IndexerId { get; set; }
        public string OperationType { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public DateTimeOffset EndedAt { get; set; }
        public string Outcome { get; set; }
        public string FailureReason { get; set; }
        public int ResultCount { get; set; }
        public IReadOnlyList<ObservedAttempt> Attempts { get; set; }

        [JsonIgnore]
        public bool CommitAcknowledged { get; set; }
    }

    /// <summary>Bounded terminal evidence persistence contract for the optional isolated retrieval endpoint.</summary>
    public interface IIndexerSearchTerminalRecordRepository
    {
        IndexerSearchTerminalRecord Save(IndexerSearchTerminalRecord record);
        IndexerSearchTerminalRecord Get(string operationId);
        IndexerSearchTerminalRecord Get(string operationId, CancellationToken cancellationToken);
        Task<IndexerSearchTerminalRecord> GetAsync(string operationId, CancellationToken cancellationToken);
    }
}
