using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NzbDrone.Common.EnvironmentInfo;
using NzbDrone.Common.Http;
using NzbDrone.Common.Instrumentation.Extensions;
using NzbDrone.Core.Exceptions;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Indexers.Events;
using NzbDrone.Core.Indexers.Newznab;
using NzbDrone.Core.IndexerSearch.Definitions;
using NzbDrone.Core.Messaging.Events;
using NzbDrone.Core.Parser;
using NzbDrone.Core.Parser.Model;

namespace NzbDrone.Core.IndexerSearch
{
    public interface IReleaseSearchService
    {
        Task<NewznabResults> Search(NewznabRequest request, List<int> indexerIds, bool interactiveSearch);
        Task<D8SearchInitiationResult> InitiateD8ValidationSearchAsync(int indexerId, string callerIdentity, string idempotencyKey, D8SearchDeadline deadline);
        Task RecoverD8InterruptedSearchesAsync(TimeSpan remainingDeadline, CancellationToken cancellationToken);
    }

    public class ReleaseSearchService : IReleaseSearchService
    {
        private readonly IIndexerLimitService _indexerLimitService;
        private readonly IEventAggregator _eventAggregator;
        private readonly IIndexerFactory _indexerFactory;
        private readonly Logger _logger;
        private readonly IIndexerSearchTerminalRecordRepository _terminalRecords;
        private readonly ID8SearchOperationJournal _operationJournal;
        private static readonly string ProcessInstanceId = Guid.NewGuid().ToString("D");

        public ReleaseSearchService(IEventAggregator eventAggregator,
                                IIndexerFactory indexerFactory,
                                IIndexerLimitService indexerLimitService,
                                IIndexerSearchTerminalRecordRepository terminalRecords,
                                ID8SearchOperationJournal operationJournal,
                                Logger logger)
        {
            _eventAggregator = eventAggregator;
            _indexerFactory = indexerFactory;
            _indexerLimitService = indexerLimitService;
            _terminalRecords = terminalRecords;
            _operationJournal = operationJournal;
            _logger = logger;
        }

        public Task<NewznabResults> Search(NewznabRequest request, List<int> indexerIds, bool interactiveSearch)
        {
            return request.t switch
            {
                "movie" => MovieSearch(request, indexerIds, interactiveSearch),
                "music" => MusicSearch(request, indexerIds, interactiveSearch),
                "tvsearch" => TvSearch(request, indexerIds, interactiveSearch),
                "book" => BookSearch(request, indexerIds, interactiveSearch),
                _ => BasicSearch(request, indexerIds, interactiveSearch)
            };
        }

        /// <summary>
        /// Executes the single-indexer, basic Newznab search used by the offline evidence-producer work.
        /// This entrypoint deliberately accepts no caller-provided operation delegate or query string.
        /// It is disabled unless the caller explicitly enables the feature.
        /// </summary>
        public async Task<ObservedNewznabSearchResult> SearchObserved(int indexerId, bool featureEnabled = false, CancellationToken cancellationToken = default)
        {
            using var deadline = new D8SearchDeadline(cancellationToken);
            return await ExecuteObservedSearchAsync(indexerId, featureEnabled, deadline, null, null, null, null, true).ConfigureAwait(false);
        }

        public async Task<D8SearchInitiationResult> InitiateD8ValidationSearchAsync(int indexerId, string callerIdentity, string idempotencyKey, D8SearchDeadline deadline)
        {
            deadline.ThrowIfExpired();
            var candidate = GetObservedIndexer(indexerId);
            deadline.ThrowIfExpired();
            var operationId = Guid.NewGuid().ToString("D");
            var correlationToken = CreateCorrelationToken();
            var startedAt = DateTimeOffset.UtcNow;
            D8SearchOperationStartResult start;
            try
            {
                start = await _operationJournal.StartAsync(
                    callerIdentity,
                    idempotencyKey,
                    indexerId,
                    operationId,
                    ProcessInstanceId,
                    correlationToken,
                    startedAt,
                    deadline.WorkRemaining,
                    deadline.Token).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                try
                {
                    await _operationJournal.FailAsync(
                        operationId,
                        callerIdentity,
                        correlationToken,
                        DateTimeOffset.UtcNow,
                        exception.GetType().Name,
                        deadline.Remaining,
                        deadline.CleanupToken).ConfigureAwait(false);
                }
                catch (Exception persistenceException)
                {
                    _logger.Error(
                        "D8 operation {0} could not be reconciled after start failure ({1})",
                        operationId,
                        GetSafeFailureCategory(persistenceException));
                }

                throw;
            }

            if (!start.Created)
            {
                deadline.ThrowIfExpired();
                var existingTerminal = _terminalRecords.Get(start.Entry.OperationId, deadline.Token);
                deadline.ThrowIfExpired();
                if (existingTerminal == null && start.Entry.State is not ("RUNNING" or "FINALIZING"))
                {
                    throw new InvalidOperationException("D8 idempotency record has a terminal state without terminal evidence");
                }

                return new D8SearchInitiationResult(false, start.Entry, existingTerminal);
            }

            try
            {
                deadline.ThrowIfExpired();
                var result = await ExecuteObservedSearchAsync(indexerId, true, deadline, start.Entry.OperationId, start.Entry.CorrelationId, start.Entry.StartedAt, candidate, false).ConfigureAwait(false);
                var completedEntry = new D8SearchOperationJournalEntry
                {
                    OperationId = start.Entry.OperationId,
                    CallerIdentity = start.Entry.CallerIdentity,
                    IdempotencyKey = start.Entry.IdempotencyKey,
                    CorrelationId = start.Entry.CorrelationId,
                    IndexerId = start.Entry.IndexerId,
                    ProcessInstanceId = start.Entry.ProcessInstanceId,
                    StartedAt = start.Entry.StartedAt,
                    EndedAt = result.TerminalRecord.EndedAt,
                    State = result.TerminalRecord.Outcome,
                    FailureReason = result.TerminalRecord.FailureReason,
                    ResultCount = result.TerminalRecord.ResultCount,
                    Attempts = result.TerminalRecord.Attempts
                };
                return new D8SearchInitiationResult(true, completedEntry, result.TerminalRecord);
            }
            catch (Exception exception)
            {
                try
                {
                    await _operationJournal.FailAsync(
                        start.Entry.OperationId,
                        start.Entry.CallerIdentity,
                        start.Entry.CorrelationId,
                        DateTimeOffset.UtcNow,
                        exception.GetType().Name,
                        deadline.Remaining,
                        deadline.CleanupToken).ConfigureAwait(false);
                }
                catch (Exception persistenceException)
                {
                    _logger.Error(
                        "D8 operation {0} could not be terminalized after {1}; startup recovery is required ({2})",
                        start.Entry.OperationId,
                        GetSafeFailureCategory(exception),
                        GetSafeFailureCategory(persistenceException));
                }

                throw;
            }
        }

        public Task RecoverD8InterruptedSearchesAsync(TimeSpan remainingDeadline, CancellationToken cancellationToken) =>
            _operationJournal.RecoverInterruptedAsync(remainingDeadline, cancellationToken);

        private static string GetSafeFailureCategory(Exception exception) => exception switch
        {
            D8StorageException storageException => storageException.Category,
            OperationCanceledException => "cancelled-or-deadline",
            _ => "internal"
        };

        private async Task<ObservedNewznabSearchResult> ExecuteObservedSearchAsync(
            int indexerId,
            bool featureEnabled,
            D8SearchDeadline deadline,
            string operationId,
            string correlationToken,
            DateTimeOffset? operationStartedAt,
            IIndexer fixedIndexer,
            bool rethrowOperationException)
        {
            deadline.ThrowIfExpired();
            if (!featureEnabled)
            {
                throw new InvalidOperationException("Instrumented search is disabled");
            }

            var candidates = new List<IIndexer> { fixedIndexer ?? GetObservedIndexer(indexerId) };
            var operationGuid = operationId == null ? Guid.NewGuid() : Guid.ParseExact(operationId, "D");
            var token = correlationToken ?? CreateCorrelationToken();
            var startedAt = operationStartedAt ?? DateTimeOffset.UtcNow;
            var request = new NewznabRequest { t = "search", q = token };
            using var indexerDeadline = deadline.CreateBoundedAttempt(TimeSpan.FromSeconds(30));
            var scope = SearchOperationObservation.Begin(
                (Newznab)candidates[0],
                new BasicSearchCriteria
                {
                    SearchType = "search",
                    SearchTerm = token,
                    IndexerIds = new List<int> { indexerId },
                    Categories = Array.Empty<int>()
                },
                featureEnabled,
                indexerDeadline.Token,
                operationGuid,
                attemptStartingAsync: operationId == null ? null : async (attempt, token) =>
                {
                    await _operationJournal.RecordAttemptIntentAsync(
                        operationId, attempt, deadline.WorkRemaining, token).ConfigureAwait(false);
                    token.ThrowIfCancellationRequested();
                    await _operationJournal.RecordPreDispatchBoundaryAsync(
                        operationId, attempt.Id, deadline.WorkRemaining, token).ConfigureAwait(false);
                },
                attemptFinishingAsync: operationId == null ? null : (attempt, _) => _operationJournal.TerminalizeAttemptAsync(
                    operationId, attempt, deadline.Remaining, deadline.CleanupToken));

            using (scope)
            {
                NewznabResults results = null;
                Exception operationException = null;
                try
                {
                    // Use the real ReleaseSearchService path but pin dispatch to the exact instance
                    // validated above, avoiding a second factory lookup or a concurrent replacement.
                    results = await BasicSearch(request, new List<int> { indexerId }, false, candidates).ConfigureAwait(false);
                    indexerDeadline.Token.ThrowIfCancellationRequested();
                    deadline.ThrowIfExpired();
                    scope.OrchestratorCompleted();
                }
                catch (Exception exception)
                {
                    scope.Observation.MarkIncomplete("observed_orchestration_exception");
                    operationException = exception;
                }

                var observation = scope.Close();
                var endedAt = DateTimeOffset.UtcNow;
                var successful = operationException == null && observation.InternallyObservedSuccess;
                var outcome = observation.Attempts.Count == 0 ? "NO_SEARCH" : successful ? "SUCCEEDED" :
                    deadline.CallerCancelled ? "CANCELLED" :
                    operationException is OperationCanceledException || indexerDeadline.IsCancellationRequested || deadline.Expired || observation.Attempts.Any(attempt => attempt.ExceptionType == "RequestTimeoutException") ? "TIMED_OUT" : "FAILED";
                var failureReason = successful ? null : operationException?.GetType().Name ??
                    string.Join(",", observation.ReasonCodes).TrimEnd(',');
                if (!successful && string.IsNullOrWhiteSpace(failureReason))
                {
                    failureReason = "incomplete_observation";
                }

                if (failureReason?.Length > 128)
                {
                    failureReason = failureReason.Substring(0, 128);
                }

                var terminalRecord = new IndexerSearchTerminalRecord
                {
                    OperationId = observation.OperationId.ToString("D"),
                    SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                    ApplicationVersion = BuildInfo.Release,
                    ProcessInstanceId = ProcessInstanceId,
                    RequestCorrelationId = token,
                    IndexerId = indexerId,
                    OperationType = IndexerSearchTerminalContract.OperationType,
                    StartedAt = startedAt,
                    EndedAt = endedAt,
                    Outcome = outcome,
                    FailureReason = failureReason,
                    ResultCount = results?.Releases?.Count ?? 0,
                    Attempts = observation.Attempts
                };

                var terminal = operationId == null
                    ? _terminalRecords.Save(terminalRecord)
                    : await _operationJournal.CompleteAsync(operationId, terminalRecord, deadline.Remaining, deadline.CleanupToken).ConfigureAwait(false);

                if (!terminal.CommitAcknowledged)
                {
                    throw new InvalidOperationException("Terminal evidence was not committed");
                }

                deadline.ThrowIfExpired();

                if (operationException != null && rethrowOperationException)
                {
                    ExceptionDispatchInfo.Capture(operationException).Throw();
                }

                return new ObservedNewznabSearchResult(results, observation, terminal);
            }
        }

        private IIndexer GetObservedIndexer(int indexerId)
        {
            var candidates = _indexerFactory.Enabled()
                .Where(indexer => ((IndexerDefinition)indexer.Definition).Id == indexerId)
                .ToList();
            if (candidates.Count != 1 || candidates[0].GetType() != typeof(Newznab))
            {
                throw new NotSupportedException("Only one enabled Newznab indexer can be observed");
            }

            return candidates[0];
        }

        private static string CreateCorrelationToken()
        {
            var payload = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            return "d8b3-" + payload;
        }

        private async Task<NewznabResults> MovieSearch(NewznabRequest request, List<int> indexerIds, bool interactiveSearch)
        {
            var searchSpec = Get<MovieSearchCriteria>(request, indexerIds, interactiveSearch);

            var imdbId = ParseUtil.GetImdbId(request.imdbid);

            searchSpec.ImdbId = imdbId?.ToString("D7");
            searchSpec.TmdbId = request.tmdbid;
            searchSpec.TraktId = request.traktid;
            searchSpec.DoubanId = request.doubanid;
            searchSpec.Year = request.year;
            searchSpec.Genre = request.genre;

            var releases = await Dispatch(indexer => indexer.Fetch(searchSpec), searchSpec);

            return new NewznabResults { Releases = DeDupeReleases(releases) };
        }

        private async Task<NewznabResults> MusicSearch(NewznabRequest request, List<int> indexerIds, bool interactiveSearch)
        {
            var searchSpec = Get<MusicSearchCriteria>(request, indexerIds, interactiveSearch);

            searchSpec.Artist = request.artist;
            searchSpec.Album = request.album;
            searchSpec.Label = request.label;
            searchSpec.Genre = request.genre;
            searchSpec.Track = request.track;
            searchSpec.Year = request.year;

            var releases = await Dispatch(indexer => indexer.Fetch(searchSpec), searchSpec);

            return new NewznabResults { Releases = DeDupeReleases(releases) };
        }

        private async Task<NewznabResults> TvSearch(NewznabRequest request, List<int> indexerIds, bool interactiveSearch)
        {
            var searchSpec = Get<TvSearchCriteria>(request, indexerIds, interactiveSearch);

            var imdbId = ParseUtil.GetImdbId(request.imdbid);

            searchSpec.ImdbId = imdbId?.ToString("D7");
            searchSpec.Season = request.season;
            searchSpec.Episode = request.ep;
            searchSpec.TvdbId = request.tvdbid;
            searchSpec.TraktId = request.traktid;
            searchSpec.TmdbId = request.tmdbid;
            searchSpec.DoubanId = request.doubanid;
            searchSpec.RId = request.rid;
            searchSpec.TvMazeId = request.tvmazeid;
            searchSpec.Year = request.year;
            searchSpec.Genre = request.genre;

            var releases = await Dispatch(indexer => indexer.Fetch(searchSpec), searchSpec);

            return new NewznabResults { Releases = DeDupeReleases(releases) };
        }

        private async Task<NewznabResults> BookSearch(NewznabRequest request, List<int> indexerIds, bool interactiveSearch)
        {
            var searchSpec = Get<BookSearchCriteria>(request, indexerIds, interactiveSearch);

            searchSpec.Author = request.author;
            searchSpec.Title = request.title;
            searchSpec.Publisher = request.publisher;
            searchSpec.Year = request.year;
            searchSpec.Genre = request.genre;

            var releases = await Dispatch(indexer => indexer.Fetch(searchSpec), searchSpec);

            return new NewznabResults { Releases = DeDupeReleases(releases) };
        }

        private async Task<NewznabResults> BasicSearch(NewznabRequest request, List<int> indexerIds, bool interactiveSearch, List<IIndexer> fixedIndexers = null)
        {
            var searchSpec = Get<BasicSearchCriteria>(request, indexerIds, interactiveSearch);

            var releases = await Dispatch(indexer => indexer.Fetch(searchSpec), searchSpec, fixedIndexers);

            return new NewznabResults { Releases = DeDupeReleases(releases) };
        }

        private TSpec Get<TSpec>(NewznabRequest query, List<int> indexerIds, bool interactiveSearch)
            where TSpec : SearchCriteriaBase, new()
        {
            var spec = new TSpec
            {
                InteractiveSearch = interactiveSearch
            };

            spec.Categories = query.cat != null ? query.cat.Split(',').Where(s => !string.IsNullOrWhiteSpace(s)).Select(int.Parse).ToArray() : Array.Empty<int>();

            spec.SearchTerm = query.q?.Trim();
            spec.SearchType = query.t;
            spec.Limit = query.limit;
            spec.Offset = query.offset;
            spec.MinAge = query.minage;
            spec.MaxAge = query.maxage;
            spec.MinSize = query.minsize;
            spec.MaxSize = query.maxsize;
            spec.Source = query.source;
            spec.Host = query.host;

            spec.IndexerIds = indexerIds;

            return spec;
        }

        private async Task<IList<ReleaseInfo>> Dispatch(Func<IIndexer, Task<IndexerPageableQueryResult>> searchAction, SearchCriteriaBase criteriaBase, List<IIndexer> fixedIndexers = null)
        {
            var indexers = fixedIndexers ?? _indexerFactory.Enabled();

            if (criteriaBase.IndexerIds is { Count: > 0 })
            {
                indexers = indexers.Where(i => criteriaBase.IndexerIds.Contains(i.Definition.Id) ||
                    (criteriaBase.IndexerIds.Contains(-1) && i.Protocol == DownloadProtocol.Usenet) ||
                    (criteriaBase.IndexerIds.Contains(-2) && i.Protocol == DownloadProtocol.Torrent))
                    .ToList();

                if (criteriaBase.InteractiveSearch && indexers.Count == 0)
                {
                    _logger.Debug("Search failed due to all selected indexers being unavailable: {0}", string.Join(", ", criteriaBase.IndexerIds));

                    throw new SearchFailedException("Search failed due to all selected indexers being unavailable");
                }
            }

            if (criteriaBase.Categories is { Length: > 0 })
            {
                // Only query supported indexers
                indexers = indexers.Where(i => i.GetCapabilities().Categories.SupportedCategories(criteriaBase.Categories).Any()).ToList();

                if (indexers.Count == 0)
                {
                    _logger.Debug("All provided categories are unsupported by selected indexers: {0}", string.Join(", ", criteriaBase.Categories));

                    return Array.Empty<ReleaseInfo>();
                }
            }

            _logger.ProgressInfo("Searching indexer(s): [{0}] for {1}", string.Join(", ", indexers.Select(i => i.Definition.Name).ToList()), criteriaBase.ToString());

            var tasks = indexers.Select(x => DispatchIndexer(searchAction, x, criteriaBase));

            var batch = await Task.WhenAll(tasks);

            var reports = batch.SelectMany(x => x).ToList();

            _logger.ProgressDebug("Total of {0} reports were found for {1} from {2} indexer(s)", reports.Count, criteriaBase, indexers.Count);

            return reports;
        }

        private async Task<IList<ReleaseInfo>> DispatchIndexer(Func<IIndexer, Task<IndexerPageableQueryResult>> searchAction, IIndexer indexer, SearchCriteriaBase criteriaBase)
        {
            if (_indexerLimitService.AtQueryLimit((IndexerDefinition)indexer.Definition))
            {
                return Array.Empty<ReleaseInfo>();
            }

            try
            {
                var indexerReports = await searchAction(indexer);

                var releases = indexerReports.Releases;

                //Filter results to only those in searched categories
                if (criteriaBase.Categories.Length > 0)
                {
                    var expandedQueryCats = indexer.GetCapabilities().Categories.ExpandTorznabQueryCategories(criteriaBase.Categories);

                    releases = releases.Where(result => result.Categories?.Any() != true || expandedQueryCats.Intersect(result.Categories.Select(c => c.Id)).Any()).ToList();

                    if (releases.Count != indexerReports.Releases.Count)
                    {
                        _logger.Trace("{0} releases from {1} ({2}) which didn't contain search categories [{3}] were filtered", indexerReports.Releases.Count - releases.Count, ((IndexerDefinition)indexer.Definition).Name, indexer.Name, string.Join(", ", expandedQueryCats));
                    }
                }

                if (criteriaBase.MinAge is > 0)
                {
                    var cutoffDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(criteriaBase.MinAge.Value));

                    releases = releases.Where(r => r.PublishDate <= cutoffDate).ToList();
                }

                if (criteriaBase.MaxAge is > 0)
                {
                    var cutoffDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(criteriaBase.MaxAge.Value));

                    releases = releases.Where(r => r.PublishDate >= cutoffDate).ToList();
                }

                if (criteriaBase.MinSize is > 0)
                {
                    var minSize = criteriaBase.MinSize.Value;

                    releases = releases.Where(r => r.Size >= minSize).ToList();
                }

                if (criteriaBase.MaxSize is > 0)
                {
                    var maxSize = criteriaBase.MaxSize.Value;

                    releases = releases.Where(r => r.Size <= maxSize).ToList();
                }

                foreach (var query in indexerReports.Queries)
                {
                    _eventAggregator.PublishEvent(new IndexerQueryEvent(indexer.Definition.Id, criteriaBase, query));
                }

                return releases;
            }
            catch (Exception e)
            {
                HttpRequestExecutionContext.MarkIncomplete("indexer_result_processing_exception");
                _eventAggregator.PublishEvent(new IndexerQueryEvent(indexer.Definition.Id, criteriaBase, new IndexerQueryResult()));
                _logger.Error(e, "Error while searching for {0}", criteriaBase);
                HttpRequestExecutionContext.CancellationToken.ThrowIfCancellationRequested();
            }

            return Array.Empty<ReleaseInfo>();
        }

        private List<ReleaseInfo> DeDupeReleases(IList<ReleaseInfo> releases)
        {
            // De-dupe reports by guid so duplicate results aren't returned. Pick the one with the higher indexer priority.
            return releases.GroupBy(r => r.Guid)
                .Select(r => r.OrderBy(v => v.IndexerPriority).First())
                .ToList();
        }
    }
}
