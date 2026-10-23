using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using NzbDrone.Common.Http;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Indexers.Newznab;
using NzbDrone.Core.IndexerSearch.Definitions;

namespace NzbDrone.Core.IndexerSearch
{
    /// <summary>
    /// In-memory, non-durable observation for the narrowly scoped Newznab basic-search path.
    /// It is deliberately not an acceptance record and cannot authorize D8.
    /// </summary>
    public sealed class SearchOperationObservation : IHttpRequestExecutionObserver, IAsyncHttpRequestExecutionObserver
    {
        public const int MaximumAttempts = 64;
        public const int MaximumObservationBytes = 64 * 1024;
        private static readonly Regex CorrelationTokenPattern = new("^d8b3-(?:[0-9a-f]{32}|[A-Za-z0-9_-]{43})$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private readonly object _gate = new();
        private readonly List<Attempt> _attempts = new();
        private readonly HashSet<string> _reasonCodes = new(StringComparer.Ordinal);
        private readonly Uri _approvedOrigin;
        private readonly string _approvedApiPath;
        private readonly string _correlationToken;
        private readonly Func<Guid> _attemptIdFactory;
        private readonly CancellationToken _cancellationToken;
        private readonly Action<ObservedAttempt> _attemptStarted;
        private readonly Action<ObservedAttempt> _attemptFinished;
        private readonly Func<ObservedAttempt, CancellationToken, Task> _attemptStartingAsync;
        private readonly Func<ObservedAttempt, CancellationToken, Task> _attemptFinishingAsync;
        private bool _incomplete;
        private bool _closed;
        private bool _orchestratorCompleted;

        private SearchOperationObservation(Uri approvedOrigin, string apiPath, string correlationToken, Func<Guid> attemptIdFactory = null, CancellationToken cancellationToken = default, Guid? operationId = null, Action<ObservedAttempt> attemptStarted = null, Action<ObservedAttempt> attemptFinished = null, Func<ObservedAttempt, CancellationToken, Task> attemptStartingAsync = null, Func<ObservedAttempt, CancellationToken, Task> attemptFinishingAsync = null)
        {
            OperationId = operationId ?? Guid.NewGuid();
            _approvedOrigin = approvedOrigin;
            _approvedApiPath = apiPath;
            _correlationToken = correlationToken;
            _attemptIdFactory = attemptIdFactory ?? (() => Guid.NewGuid());
            _cancellationToken = cancellationToken;
            _attemptStarted = attemptStarted;
            _attemptFinished = attemptFinished;
            _attemptStartingAsync = attemptStartingAsync;
            _attemptFinishingAsync = attemptFinishingAsync;
        }

        public CancellationToken CancellationToken => _cancellationToken;

        public Guid OperationId { get; }

        internal static SearchOperationObservationScope Begin(Newznab indexer, BasicSearchCriteria criteria, bool featureEnabled, CancellationToken cancellationToken = default, Guid? operationId = null, Action<ObservedAttempt> attemptStarted = null, Action<ObservedAttempt> attemptFinished = null, Func<ObservedAttempt, CancellationToken, Task> attemptStartingAsync = null, Func<ObservedAttempt, CancellationToken, Task> attemptFinishingAsync = null)
        {
            if (!featureEnabled)
            {
                throw new InvalidOperationException("Instrumented search is disabled");
            }

            var definition = indexer?.Definition as IndexerDefinition;
            var settings = definition?.Settings as NewznabSettings;
            if (definition == null || settings == null || definition.Tags is { Count: > 0 } || !IsSupportedOperation(indexer.GetType(), definition.Id, criteria) ||
                !CorrelationTokenPattern.IsMatch(criteria.SearchTerm) ||
                !string.IsNullOrWhiteSpace(settings.AdditionalParameters) ||
                criteria.Categories is not { Length: 0 } || criteria.Limit.HasValue || criteria.Offset.HasValue || criteria.MinAge.HasValue || criteria.MaxAge.HasValue || criteria.MinSize.HasValue || criteria.MaxSize.HasValue)
            {
                throw new NotSupportedException("Only one exact Newznab indexer basic-search operation is supported");
            }

            if (HttpRequestExecutionContext.Current != null)
            {
                throw new InvalidOperationException("Nested instrumented operations are unsupported");
            }

            if (!Uri.TryCreate(settings.BaseUrl, UriKind.Absolute, out var approvedOrigin) ||
                approvedOrigin.Scheme != Uri.UriSchemeHttps || !string.IsNullOrEmpty(approvedOrigin.UserInfo) ||
                !string.IsNullOrEmpty(approvedOrigin.Query) || !string.IsNullOrEmpty(approvedOrigin.Fragment) || string.IsNullOrWhiteSpace(settings.ApiPath))
            {
                throw new NotSupportedException("The configured Newznab origin must be credential-free HTTPS");
            }

            var apiPath = new Uri(settings.BaseUrl.TrimEnd('/') + "/" + settings.ApiPath.Trim('/')).AbsolutePath.TrimEnd('/');

            return new SearchOperationObservationScope(new SearchOperationObservation(approvedOrigin, apiPath, criteria.SearchTerm, cancellationToken: cancellationToken, operationId: operationId, attemptStarted: attemptStarted, attemptFinished: attemptFinished, attemptStartingAsync: attemptStartingAsync, attemptFinishingAsync: attemptFinishingAsync));
        }

        public static bool IsSupportedOperation(Type adapterType, int indexerId, BasicSearchCriteria criteria) =>
            adapterType == typeof(Newznab) && indexerId > 0 && criteria != null && criteria.SearchType == "search" &&
            !string.IsNullOrWhiteSpace(criteria.SearchTerm) && criteria.IndexerIds is { Count: 1 } && criteria.IndexerIds[0] == indexerId;

        internal static SearchOperationObservationScope CreateForTest(
            Func<Guid> attemptIdFactory = null,
            Action<ObservedAttempt> attemptStarted = null,
            Action<ObservedAttempt> attemptFinished = null) =>
            new(new SearchOperationObservation(
                new Uri("https://indexer.example/"),
                "/api",
                "d8b3-00000000000000000000000000000000",
                attemptIdFactory,
                attemptStarted: attemptStarted,
                attemptFinished: attemptFinished));

        public IHttpRequestAttemptScope BeginAttempt(string method, Uri requestUri, DateTimeOffset startedAt)
        {
            lock (_gate)
            {
                var attempt = CreateAttemptLocked(method, requestUri, startedAt);
                if (attempt.IsSearchRequest)
                {
                    _attemptStarted?.Invoke(attempt.ToObservedAttempt());
                }

                _attempts.Add(attempt);
                return new AttemptScope(this, attempt);
            }
        }

        public async Task<IAsyncHttpRequestAttemptScope> BeginAttemptAsync(string method, Uri requestUri, DateTimeOffset startedAt, CancellationToken cancellationToken)
        {
            Attempt attempt;
            lock (_gate)
            {
                attempt = CreateAttemptLocked(method, requestUri, startedAt);
                _attempts.Add(attempt);
            }

            if (attempt.IsSearchRequest)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_attemptStartingAsync != null)
                    {
                        await _attemptStartingAsync(attempt.ToObservedAttempt(), cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        _attemptStarted?.Invoke(attempt.ToObservedAttempt());
                    }
                }
                catch
                {
                    MarkIncomplete("durable_attempt_intent_failed");
                    throw;
                }
            }

            return new AttemptScope(this, attempt);
        }

        private Attempt CreateAttemptLocked(string method, Uri requestUri, DateTimeOffset startedAt)
        {
            if (_closed || _attempts.Count >= MaximumAttempts || requestUri == null ||
                requestUri.Scheme != Uri.UriSchemeHttps || requestUri.Host.Length > 253 ||
                !string.Equals(requestUri.IdnHost, _approvedOrigin.IdnHost, StringComparison.OrdinalIgnoreCase) || requestUri.Port != _approvedOrigin.Port ||
                !string.Equals(requestUri.AbsolutePath.TrimEnd('/'), _approvedApiPath, StringComparison.Ordinal) ||
                method is not ("GET" or "POST"))
            {
                MarkIncompleteLocked("unsupported_or_unbounded_dispatch");
                throw new InvalidOperationException("Observed dispatch is outside the supported bounded request contract");
            }

            var attempt = new Attempt(_attemptIdFactory(), method, requestUri.IdnHost, startedAt);
            if (_attempts.Any(a => a.Id == attempt.Id))
            {
                MarkIncompleteLocked("duplicate_attempt_identity");
                throw new InvalidOperationException("Duplicate application-generated attempt identity");
            }

            if (!TryClassifyRequest(requestUri, out var isSearchRequest))
            {
                MarkIncompleteLocked("unsupported_or_uncorrelated_request");
                throw new InvalidOperationException("Observed request is not a supported correlated Newznab search");
            }

            attempt.IsSearchRequest = isSearchRequest;
            return attempt;
        }

        public void MarkIncomplete(string reasonCode)
        {
            lock (_gate)
            {
                MarkIncompleteLocked(reasonCode);
            }
        }

        private void MarkIncompleteLocked(string reasonCode)
        {
            _incomplete = true;
            if (_reasonCodes.Count < 16 && !string.IsNullOrWhiteSpace(reasonCode))
            {
                _reasonCodes.Add(reasonCode.Length <= 64 ? reasonCode : reasonCode.Substring(0, 64));
            }
        }

        internal void OrchestratorCompleted()
        {
            lock (_gate)
            {
                if (_closed || _orchestratorCompleted)
                {
                    MarkIncompleteLocked("invalid_orchestrator_terminalization");
                    return;
                }

                _orchestratorCompleted = true;
            }
        }

        internal ObservationResult Close()
        {
            lock (_gate)
            {
                if (_closed)
                {
                    MarkIncompleteLocked("repeated_terminalization");
                    return CreateResultLocked(false);
                }

                if (!_orchestratorCompleted)
                {
                    MarkIncompleteLocked("orchestrator_not_completed");
                }

                _closed = true;
                return CreateResultLocked(_orchestratorCompleted);
            }
        }

        private ObservationResult CreateResultLocked(bool orchestratorCompleted)
        {
            var completeAttempts = _attempts.All(a => a.Terminal);
            if (_attempts.Count == 0)
            {
                MarkIncompleteLocked("no_observed_dispatch");
            }

            if (!completeAttempts)
            {
                MarkIncompleteLocked("attempt_missing_terminal_outcome");
            }

            var hasCorrelatedSearch = _attempts.Any(a => a.IsSearchRequest && a.StatusCode is >= 200 and < 300);
            if (!hasCorrelatedSearch)
            {
                MarkIncompleteLocked("no_correlated_search_response");
            }

            var attempts = _attempts.Where(a => a.IsSearchRequest)
                .Select(a => new ObservedAttempt(a.Id, a.Method, a.Host, a.StatusCode, a.ExceptionType, a.StartedAt, a.CompletedAt)).ToArray();
            var reasons = _reasonCodes.OrderBy(r => r, StringComparer.Ordinal).ToArray();
            var estimatedBytes = 512 + attempts.Sum(a => 512 + Encoding.UTF8.GetByteCount(a.Host) + Encoding.UTF8.GetByteCount(a.ExceptionType ?? string.Empty)) +
                reasons.Sum(Encoding.UTF8.GetByteCount);
            if (estimatedBytes > MaximumObservationBytes)
            {
                MarkIncompleteLocked("observation_size_limit");
                reasons = _reasonCodes.OrderBy(r => r, StringComparer.Ordinal).ToArray();
            }

            var successful = orchestratorCompleted && !_incomplete && _attempts.Count > 0 && completeAttempts &&
                hasCorrelatedSearch &&
                _attempts.All(a => a.StatusCode is >= 200 and < 300);
            return new ObservationResult(
                OperationId,
                _correlationToken,
                successful,
                _incomplete || !completeAttempts || _attempts.Count == 0,
                attempts,
                reasons);
        }

        private bool TryClassifyRequest(Uri uri, out bool isSearchRequest)
        {
            isSearchRequest = false;
            var values = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
                .Select(part => part.Split('=', 2))
                .Select(parts => (Key: WebUtility.UrlDecode(parts[0]), Value: parts.Length == 2 ? WebUtility.UrlDecode(parts[1]) : string.Empty))
                .ToList();
            var typeValues = values.Where(p => p.Key == "t").Select(p => p.Value).ToList();
            if (typeValues.Count != 1)
            {
                return false;
            }

            if (values.Any(p => p.Key is not ("t" or "q" or "extended" or "apikey")))
            {
                return false;
            }

            if (values.Count(p => p.Key == "apikey") > 1 || values.Count(p => p.Key == "extended") > 1)
            {
                return false;
            }

            if (typeValues[0] == "caps")
            {
                return !values.Any(p => p.Key == "q" || p.Key == "extended");
            }

            var queryValues = values.Where(p => p.Key == "q").Select(p => p.Value).ToList();
            var extendedValues = values.Where(p => p.Key == "extended").Select(p => p.Value).ToList();
            if (typeValues[0] != "search" || queryValues.Count != 1 || queryValues[0] != _correlationToken ||
                extendedValues.Count != 1 || extendedValues[0] != "1")
            {
                return false;
            }

            isSearchRequest = true;
            return true;
        }

        private void Finish(Attempt attempt, int? statusCode, string exceptionType, DateTimeOffset completedAt)
        {
            lock (_gate)
            {
                if (attempt.Terminal || _closed)
                {
                    MarkIncompleteLocked("duplicate_attempt_terminal");
                    return;
                }

                attempt.StatusCode = statusCode;
                if (exceptionType?.Length > 128)
                {
                    MarkIncompleteLocked("exception_type_too_long");
                    exceptionType = exceptionType.Substring(0, 128);
                }

                attempt.ExceptionType = exceptionType;
                attempt.CompletedAt = completedAt;
                attempt.Terminal = true;
                if (completedAt < attempt.StartedAt)
                {
                    MarkIncompleteLocked("invalid_attempt_time");
                }

                if (exceptionType != null || statusCode is < 200 or >= 300)
                {
                    MarkIncompleteLocked("failed_or_transitional_attempt");
                }

                if (attempt.IsSearchRequest)
                {
                    _attemptFinished?.Invoke(attempt.ToObservedAttempt());
                }
            }
        }

        private async Task FinishAsync(Attempt attempt, int? statusCode, string exceptionType, DateTimeOffset completedAt, CancellationToken cancellationToken)
        {
            ObservedAttempt terminal = null;
            lock (_gate)
            {
                if (attempt.Terminal || _closed)
                {
                    MarkIncompleteLocked("duplicate_attempt_terminal");
                    return;
                }

                attempt.StatusCode = statusCode;
                if (exceptionType?.Length > 128)
                {
                    MarkIncompleteLocked("exception_type_too_long");
                    exceptionType = exceptionType.Substring(0, 128);
                }

                attempt.ExceptionType = exceptionType;
                attempt.CompletedAt = completedAt;
                attempt.Terminal = true;
                if (completedAt < attempt.StartedAt)
                {
                    MarkIncompleteLocked("invalid_attempt_time");
                }

                if (exceptionType != null || statusCode is < 200 or >= 300)
                {
                    MarkIncompleteLocked("failed_or_transitional_attempt");
                }

                if (attempt.IsSearchRequest)
                {
                    terminal = attempt.ToObservedAttempt();
                }
            }

            if (terminal != null)
            {
                try
                {
                    if (_attemptFinishingAsync != null)
                    {
                        await _attemptFinishingAsync(terminal, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        _attemptFinished?.Invoke(terminal);
                    }
                }
                catch
                {
                    MarkIncomplete("durable_attempt_terminalization_failed");
                    throw;
                }
            }
        }

        private sealed class Attempt
        {
            public Attempt(Guid id, string method, string host, DateTimeOffset startedAt)
            {
                Id = id;
                Method = method;
                Host = host;
                StartedAt = startedAt;
            }

            public Guid Id { get; }
            public string Method { get; }
            public string Host { get; }
            public DateTimeOffset StartedAt { get; }
            public DateTimeOffset? CompletedAt { get; set; }
            public int? StatusCode { get; set; }
            public string ExceptionType { get; set; }
            public bool Terminal { get; set; }
            public bool IsSearchRequest { get; set; }
            public ObservedAttempt ToObservedAttempt() => new(Id, Method, Host, StatusCode, ExceptionType, StartedAt, CompletedAt);
        }

        private sealed class AttemptScope : IHttpRequestAttemptScope, IAsyncHttpRequestAttemptScope
        {
            private readonly SearchOperationObservation _owner;
            private readonly Attempt _attempt;
            private bool _finished;
            public AttemptScope(SearchOperationObservation owner, Attempt attempt)
            {
                _owner = owner;
                _attempt = attempt;
            }

            public void Complete(int statusCode, DateTimeOffset completedAt)
            {
                if (_finished)
                {
                    _owner.MarkIncomplete("duplicate_attempt_terminal");
                    return;
                }

                _finished = true;
                _owner.Finish(_attempt, statusCode, null, completedAt);
            }

            public void Fail(string exceptionType, DateTimeOffset completedAt)
            {
                if (_finished)
                {
                    _owner.MarkIncomplete("duplicate_attempt_terminal");
                    return;
                }

                _finished = true;
                _owner.Finish(_attempt, null, exceptionType, completedAt);
            }

            public async Task CompleteAsync(int statusCode, DateTimeOffset completedAt, CancellationToken cancellationToken)
            {
                if (_finished)
                {
                    _owner.MarkIncomplete("duplicate_attempt_terminal");
                    return;
                }

                _finished = true;
                await _owner.FinishAsync(_attempt, statusCode, null, completedAt, cancellationToken).ConfigureAwait(false);
            }

            public async Task FailAsync(string exceptionType, DateTimeOffset completedAt, CancellationToken cancellationToken)
            {
                if (_finished)
                {
                    _owner.MarkIncomplete("duplicate_attempt_terminal");
                    return;
                }

                _finished = true;
                await _owner.FinishAsync(_attempt, null, exceptionType, completedAt, cancellationToken).ConfigureAwait(false);
            }

            public void Dispose()
            {
                if (!_finished)
                {
                    _finished = true;
                    _owner.MarkIncomplete("attempt_without_terminal_outcome");
                    _owner.Finish(_attempt, null, "AttemptOutcomeMissing", DateTimeOffset.UtcNow);
                }
            }

            public async ValueTask DisposeAsync()
            {
                if (!_finished)
                {
                    _finished = true;
                    _owner.MarkIncomplete("attempt_without_terminal_outcome");
                    await _owner.FinishAsync(_attempt, null, "AttemptOutcomeMissing", DateTimeOffset.UtcNow, CancellationToken.None).ConfigureAwait(false);
                }
            }
        }

        internal void Abandon()
        {
            lock (_gate)
            {
                if (!_closed)
                {
                    MarkIncompleteLocked("scope_disposed_without_close");
                }

                _closed = true;
            }
        }
    }

    public sealed class SearchOperationObservationScope : IDisposable
    {
        private readonly IDisposable _context;
        private bool _disposed;
        internal SearchOperationObservationScope(SearchOperationObservation observation)
        {
            Observation = observation;
            _context = HttpRequestExecutionContext.Begin(observation);
        }

        internal SearchOperationObservation Observation { get; }
        internal ObservationResult Close() => Observation.Close();
        internal void OrchestratorCompleted() => Observation.OrchestratorCompleted();
        internal ObservationResult CloseForTest(bool orchestratorCompleted = true)
        {
            if (orchestratorCompleted)
            {
                Observation.OrchestratorCompleted();
            }

            return Close();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Observation.Abandon();
            _context.Dispose();
            _disposed = true;
        }
    }

    public sealed record ObservationResult(Guid OperationId, string CorrelationToken, bool InternallyObservedSuccess, bool Incomplete, IReadOnlyList<ObservedAttempt> Attempts, IReadOnlyList<string> ReasonCodes);
    public sealed record ObservedAttempt(Guid Id, string Method, string Host, int? StatusCode, string ExceptionType, DateTimeOffset StartedAt, DateTimeOffset? CompletedAt);
    public sealed record ObservedNewznabSearchResult(NewznabResults Results, ObservationResult Observation, IndexerSearchTerminalRecord TerminalRecord);
}
