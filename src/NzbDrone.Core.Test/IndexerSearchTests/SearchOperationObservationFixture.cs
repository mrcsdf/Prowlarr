using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using NzbDrone.Common;
using NzbDrone.Common.Cache;
using NzbDrone.Common.EnvironmentInfo;
using NzbDrone.Common.Http;
using NzbDrone.Common.Http.Dispatchers;
using NzbDrone.Common.Http.Proxy;
using NzbDrone.Common.Messaging;
using NzbDrone.Common.TPL;
using NzbDrone.Core.Configuration;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.Download;
using NzbDrone.Core.IndexerProxies;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Indexers.Newznab;
using NzbDrone.Core.IndexerSearch;
using NzbDrone.Core.IndexerSearch.Definitions;
using NzbDrone.Core.Messaging.Events;
using NzbDrone.Core.Test.Framework;
using NzbDrone.Test.Common;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class SearchOperationObservationFixture
    {
        private static readonly Uri RequestUri = new("https://indexer.example/api?t=search&extended=1&q=d8b3-00000000000000000000000000000000");

        [Test]
        public void should_require_enabled_exact_adapter_and_supported_operation()
        {
            var criteria = new BasicSearchCriteria { SearchType = "search", SearchTerm = "token" };
            criteria.IndexerIds = new System.Collections.Generic.List<int> { 42 };
            SearchOperationObservation.IsSupportedOperation(typeof(Newznab), 42, criteria).Should().BeTrue();
            SearchOperationObservation.IsSupportedOperation(typeof(object), 42, criteria).Should().BeFalse();
            SearchOperationObservation.IsSupportedOperation(typeof(Newznab), 43, criteria).Should().BeFalse();
            SearchOperationObservation.IsSupportedOperation(typeof(Newznab), 42, new BasicSearchCriteria { SearchType = "tvsearch", SearchTerm = "token", IndexerIds = new System.Collections.Generic.List<int> { 42 } }).Should().BeFalse();
            FluentActions.Invoking(() => SearchOperationObservation.Begin(null, criteria, false)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_report_success_only_for_one_fully_observed_successful_dispatch()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            attempt.Complete(200, DateTimeOffset.UtcNow);
            attempt.Dispose();

            var result = scope.CloseForTest();
            result.InternallyObservedSuccess.Should().BeTrue();
            result.Incomplete.Should().BeFalse();
            result.Attempts.Should().ContainSingle();
            result.CorrelationToken.Should().Be("d8b3-00000000000000000000000000000000");
            result.Attempts.Select(a => a.Id).Distinct().Should().HaveCount(1);
        }

        [Test]
        public void capabilities_fetch_without_correlated_search_is_not_success()
        {
            var started = new List<ObservedAttempt>();
            var finished = new List<ObservedAttempt>();
            using var scope = SearchOperationObservation.CreateForTest(attemptStarted: started.Add, attemptFinished: finished.Add);
            var capabilities = HttpRequestExecutionContext.Current.BeginAttempt("GET", new Uri("https://indexer.example/api?t=caps"), DateTimeOffset.UtcNow);
            capabilities.Complete(200, DateTimeOffset.UtcNow);
            capabilities.Dispose();
            var result = scope.CloseForTest(false);
            result.InternallyObservedSuccess.Should().BeFalse();
            result.Incomplete.Should().BeTrue();
            result.Attempts.Should().BeEmpty("capability negotiation is not an indexer search dispatch");
            started.Should().BeEmpty();
            finished.Should().BeEmpty();
        }

        [Test]
        public void durable_attempt_callbacks_include_only_correlated_search_dispatches()
        {
            var started = new List<ObservedAttempt>();
            var finished = new List<ObservedAttempt>();
            using var scope = SearchOperationObservation.CreateForTest(attemptStarted: started.Add, attemptFinished: finished.Add);
            var capabilities = HttpRequestExecutionContext.Current.BeginAttempt("GET", new Uri("https://indexer.example/api?t=caps"), DateTimeOffset.UtcNow);
            capabilities.Complete(200, DateTimeOffset.UtcNow);
            capabilities.Dispose();
            var search = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            search.Complete(200, DateTimeOffset.UtcNow);
            search.Dispose();

            var result = scope.CloseForTest();
            result.InternallyObservedSuccess.Should().BeTrue();
            result.Attempts.Should().ContainSingle();
            started.Should().ContainSingle().Which.Id.Should().Be(result.Attempts[0].Id);
            finished.Should().ContainSingle().Which.Id.Should().Be(result.Attempts[0].Id);
        }

        [Test]
        public void should_fail_closed_when_indexer_base_marks_a_swallowed_failure()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var first = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            first.Complete(200, DateTimeOffset.UtcNow);
            first.Dispose();

            // Unit-level invariant only; this is not the requested application-path B1-F01 regression.
            HttpRequestExecutionContext.MarkIncomplete("indexer_search_exception");
            var result = scope.CloseForTest(false);
            result.InternallyObservedSuccess.Should().BeFalse();
            result.Incomplete.Should().BeTrue();
        }

        [Test]
        public void should_not_recover_to_success_after_failed_retry()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var failed = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            failed.Complete(503, DateTimeOffset.UtcNow);
            failed.Dispose();
            var retry = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            retry.Complete(200, DateTimeOffset.UtcNow);
            retry.Dispose();

            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void should_reject_dispatch_to_an_unapproved_host()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            FluentActions.Invoking(() => HttpRequestExecutionContext.Current.BeginAttempt("GET", new Uri("https://other.example/api"), DateTimeOffset.UtcNow))
                .Should().Throw<InvalidOperationException>();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void should_reject_dispatch_with_a_different_correlation_token()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            FluentActions.Invoking(() => HttpRequestExecutionContext.Current.BeginAttempt("GET",
                    new Uri("https://indexer.example/api?t=search&q=d8b3-11111111111111111111111111111111"),
                    DateTimeOffset.UtcNow))
                .Should().Throw<InvalidOperationException>();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void should_reject_duplicate_attempt_identity()
        {
            using var scope = SearchOperationObservation.CreateForTest(() => Guid.Empty);
            var first = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            first.Complete(200, DateTimeOffset.UtcNow);
            first.Dispose();
            FluentActions.Invoking(() => HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow))
                .Should().Throw<InvalidOperationException>();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [TestCase(302)]
        [TestCase(401)]
        public void should_fail_closed_for_redirect_or_authentication_transition(int status)
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            attempt.Complete(status, DateTimeOffset.UtcNow);
            attempt.Dispose();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void should_reject_missing_and_unterminated_attempts()
        {
            using (var empty = SearchOperationObservation.CreateForTest())
            {
                empty.CloseForTest().Incomplete.Should().BeTrue();
            }

            using var scope = SearchOperationObservation.CreateForTest();
            HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow).Dispose();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [TestCase("exception_before_dispatch")]
        [TestCase("timeout")]
        [TestCase("cancellation")]
        public void should_fail_closed_when_operation_ends_without_complete_coverage(string reason)
        {
            using var scope = SearchOperationObservation.CreateForTest();
            HttpRequestExecutionContext.MarkIncomplete(reason);
            var result = scope.CloseForTest(false);
            result.InternallyObservedSuccess.Should().BeFalse();
            result.Incomplete.Should().BeTrue();
        }

        [Test]
        public void premature_close_must_not_authorize_success()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            attempt.Complete(200, DateTimeOffset.UtcNow);
            attempt.Dispose();

            scope.Close().InternallyObservedSuccess.Should().BeFalse();
            scope.Close().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void closing_with_outstanding_attempt_and_late_completion_must_remain_incomplete()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);

            var result = scope.CloseForTest();
            result.InternallyObservedSuccess.Should().BeFalse();
            result.Incomplete.Should().BeTrue();

            attempt.Complete(200, DateTimeOffset.UtcNow);
            attempt.Dispose();
            scope.Close().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public void should_enforce_attempt_limit()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            for (var i = 0; i < SearchOperationObservation.MaximumAttempts; i++)
            {
                var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
                attempt.Complete(200, DateTimeOffset.UtcNow);
                attempt.Dispose();
            }

            FluentActions.Invoking(() => HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow))
                .Should().Throw<InvalidOperationException>();
            scope.CloseForTest().InternallyObservedSuccess.Should().BeFalse();
        }

        [Test]
        public async Task should_isolate_concurrent_operation_contexts()
        {
            async Task<ObservationResult> Run()
            {
                using var scope = SearchOperationObservation.CreateForTest();
                await Task.Yield();
                var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
                attempt.Complete(200, DateTimeOffset.UtcNow);
                attempt.Dispose();
                return scope.CloseForTest();
            }

            var results = await Task.WhenAll(Run(), Run());
            results.Select(r => r.OperationId).Distinct().Should().HaveCount(2);
            results.Should().OnlyContain(r => r.InternallyObservedSuccess && r.Attempts.Count == 1);
        }

        [Test]
        public async Task arbitrary_unjoined_child_can_outlive_observation_and_is_not_a_supported_path()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var childCaptured = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowChildDispatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var child = Task.Run(async () =>
            {
                childCaptured.SetResult();
                await allowChildDispatch.Task;
                return FluentActions.Invoking(() => HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow))
                    .Should().Throw<InvalidOperationException>();
            });

            await childCaptured.Task;
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", RequestUri, DateTimeOffset.UtcNow);
            attempt.Complete(200, DateTimeOffset.UtcNow);
            attempt.Dispose();
            var alreadyReturned = scope.CloseForTest();

            allowChildDispatch.SetResult();
            await child;

            alreadyReturned.InternallyObservedSuccess.Should().BeTrue();
        }
    }

    [TestFixture]
    public class ObservedEventContextIsolationFixture : CoreTest
    {
        [Test]
        public async Task sync_and_async_event_subscriber_work_does_not_inherit_search_context()
        {
            using var scope = SearchOperationObservation.CreateForTest();
            var attempt = HttpRequestExecutionContext.Current.BeginAttempt("GET", new Uri("https://indexer.example/api?t=search&extended=1&q=d8b3-00000000000000000000000000000000"), DateTimeOffset.UtcNow);
            attempt.Complete(200, DateTimeOffset.UtcNow);
            attempt.Dispose();

            var releaseChildren = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var syncCapture = new TaskCompletionSource<IHttpRequestExecutionObserver>(TaskCreationOptions.RunContinuationsAsynchronously);
            var asyncCapture = new TaskCompletionSource<IHttpRequestExecutionObserver>(TaskCreationOptions.RunContinuationsAsynchronously);
            var syncHandler = new CapturingSyncHandler(syncCapture, releaseChildren.Task);
            var asyncHandler = new CapturingAsyncHandler(asyncCapture, releaseChildren.Task);
            var services = new Mock<IServiceFactory>();
            services.Setup(factory => factory.BuildAll<IHandle<ObservationProbeEvent>>()).Returns(new IHandle<ObservationProbeEvent>[] { syncHandler });
            services.Setup(factory => factory.BuildAll<IHandleAsync<IEvent>>()).Returns(new IHandleAsync<IEvent>[] { asyncHandler });
            services.Setup(factory => factory.BuildAll<IHandleAsync<ObservationProbeEvent>>()).Returns(Array.Empty<IHandleAsync<ObservationProbeEvent>>());

            var runtimeInfo = new Mock<IRuntimeInfo>();
            runtimeInfo.SetupGet(info => info.IsExiting).Returns(false);
            var aggregator = new EventAggregator(TestLogger, services.Object, runtimeInfo.Object);
            aggregator.PublishEvent(new ObservationProbeEvent());

            var syncObserver = await syncCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var asyncObserver = await asyncCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var completedResult = scope.CloseForTest();

            releaseChildren.SetResult();
            await Task.WhenAll(syncHandler.ChildCompleted, asyncHandler.ChildCompleted).WaitAsync(TimeSpan.FromSeconds(5));

            syncObserver.Should().BeNull();
            asyncObserver.Should().BeNull();
            completedResult.InternallyObservedSuccess.Should().BeTrue();
        }

        private sealed class ObservationProbeEvent : IEvent
        {
        }

        private sealed class CapturingSyncHandler : IHandle<ObservationProbeEvent>
        {
            private readonly TaskCompletionSource<IHttpRequestExecutionObserver> _captured;
            private readonly Task _release;
            private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public CapturingSyncHandler(TaskCompletionSource<IHttpRequestExecutionObserver> captured, Task release)
            {
                _captured = captured;
                _release = release;
            }

            public Task ChildCompleted => _completed.Task;

            public void Handle(ObservationProbeEvent message)
            {
                Task.Run(async () =>
                {
                    var observer = HttpRequestExecutionContext.Current;
                    _captured.TrySetResult(observer);
                    await _release;
                    observer?.BeginAttempt("GET", new Uri("https://indexer.example/api?t=search&extended=1&q=d8b3-00000000000000000000000000000000"), DateTimeOffset.UtcNow);
                    _completed.TrySetResult();
                });
            }
        }

        private sealed class CapturingAsyncHandler : IHandleAsync<IEvent>
        {
            private readonly TaskCompletionSource<IHttpRequestExecutionObserver> _captured;
            private readonly Task _release;
            private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public CapturingAsyncHandler(TaskCompletionSource<IHttpRequestExecutionObserver> captured, Task release)
            {
                _captured = captured;
                _release = release;
            }

            public Task ChildCompleted => _completed.Task;

            public void HandleAsync(IEvent message)
            {
                Task.Run(async () =>
                {
                    var observer = HttpRequestExecutionContext.Current;
                    _captured.TrySetResult(observer);
                    await _release;
                    observer?.BeginAttempt("GET", new Uri("https://indexer.example/api?t=search&extended=1&q=d8b3-00000000000000000000000000000000"), DateTimeOffset.UtcNow);
                    _completed.TrySetResult();
                });
            }
        }
    }

    [TestFixture]
    public class ObservedProxyIsolationFixture : CoreTest
    {
        [Test]
        public void observed_handler_disables_system_proxy_but_ordinary_handler_keeps_default()
        {
            var variables = new[] { "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy" };
            var saved = variables.ToDictionary(name => name, Environment.GetEnvironmentVariable);
            try
            {
                foreach (var name in variables)
                {
                    Environment.SetEnvironmentVariable(name, "http://127.0.0.1:9");
                }

                var dispatcher = new InspectableManagedHttpDispatcher(
                    Mocker.GetMock<IHttpProxySettingsProvider>().Object,
                    Mocker.GetMock<ICreateManagedWebProxy>().Object,
                    Mocker.GetMock<ICertificateValidationService>().Object,
                    Mocker.Resolve<UserAgentBuilder>(),
                    Mocker.Resolve<CacheManager>(),
                    TestLogger);

                Mocker.GetMock<IHttpProxySettingsProvider>()
                    .Setup(provider => provider.GetProxySettings(It.IsAny<HttpUri>()))
                    .Returns((HttpProxySettings)null);

                dispatcher.CreateForTest(true);
                dispatcher.CreatedHandlers.Should().ContainSingle().Which.UseProxy.Should().BeFalse();

                dispatcher.CreateForTest(false);
                dispatcher.CreatedHandlers.Should().HaveCount(2);
                dispatcher.CreatedHandlers[1].UseProxy.Should().BeTrue();
            }
            finally
            {
                foreach (var variable in variables)
                {
                    Environment.SetEnvironmentVariable(variable, saved[variable]);
                }
            }
        }

        [TestCase(SslPolicyErrors.None, true)]
        [TestCase(SslPolicyErrors.RemoteCertificateChainErrors, false)]
        [TestCase(SslPolicyErrors.RemoteCertificateNameMismatch, false)]
        public void observed_handler_requires_strict_certificate_validation(SslPolicyErrors errors, bool expected)
        {
            var certificateValidation = Mocker.GetMock<ICertificateValidationService>();
            certificateValidation
                .Setup(service => service.ShouldByPassValidationError(It.IsAny<object>(), It.IsAny<System.Security.Cryptography.X509Certificates.X509Certificate>(), It.IsAny<System.Security.Cryptography.X509Certificates.X509Chain>(), It.IsAny<SslPolicyErrors>()))
                .Returns(true);

            var dispatcher = new InspectableManagedHttpDispatcher(
                Mocker.GetMock<IHttpProxySettingsProvider>().Object,
                Mocker.GetMock<ICreateManagedWebProxy>().Object,
                certificateValidation.Object,
                Mocker.Resolve<UserAgentBuilder>(),
                Mocker.Resolve<CacheManager>(),
                TestLogger);

            dispatcher.CreateForTest(true);
            var callback = dispatcher.CreatedHandlers.Single().SslOptions.RemoteCertificateValidationCallback;

            callback(null, null, null, errors).Should().Be(expected);
            certificateValidation.Verify(service => service.ShouldByPassValidationError(
                It.IsAny<object>(),
                It.IsAny<System.Security.Cryptography.X509Certificates.X509Certificate>(),
                It.IsAny<System.Security.Cryptography.X509Certificates.X509Chain>(),
                It.IsAny<SslPolicyErrors>()),
                Times.Never);
        }

        private sealed class InspectableManagedHttpDispatcher : ManagedHttpDispatcher
        {
            public List<SocketsHttpHandler> CreatedHandlers { get; } = new();

            public InspectableManagedHttpDispatcher(IHttpProxySettingsProvider proxySettingsProvider,
                ICreateManagedWebProxy createManagedWebProxy,
                ICertificateValidationService certificateValidationService,
                IUserAgentBuilder userAgentBuilder,
                ICacheManager cacheManager,
                NLog.Logger logger)
                : base(proxySettingsProvider, createManagedWebProxy, certificateValidationService, userAgentBuilder, cacheManager, logger)
            {
            }

            public void CreateForTest(bool observed)
            {
                GetClient(new HttpUri("https://indexer.example/api"), null, observed);
            }

            protected override SocketsHttpHandler CreateHandler(HttpProxySettings proxySettings, bool observed)
            {
                var handler = base.CreateHandler(proxySettings, observed);
                CreatedHandlers.Add(handler);
                return handler;
            }
        }
    }

    [TestFixture]
    public class ObservedNewznabExecutionFixture : DbTest<ReleaseSearchService, NzbDrone.Core.Jobs.ScheduledTask>
    {
        private FakeResponseHandler _transport;
        private FakeManagedHttpDispatcher _dispatcher;
        private Newznab _indexer;

        private void SetUpObservedSearch(HttpStatusCode statusCode, string body)
        {
            SetUpObservedSearch(_ => new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/rss+xml")
            });
        }

        private void SetUpObservedSearch(Func<HttpRequestMessage, HttpResponseMessage> responseFactory, bool useMockRepository = false)
        {
            Mocker.SetConstant<ITerminalEvidenceSqliteConnectionFactory>(
                new TerminalEvidenceSqliteConnectionFactory(Mocker.Resolve<IConnectionStringFactory>()));

            if (useMockRepository)
            {
                Mocker.GetMock<IIndexerSearchTerminalRecordRepository>();
            }
            else
            {
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(Mocker.Resolve<IndexerSearchTerminalRecordRepository>());
            }

            _transport = new FakeResponseHandler(responseFactory);
            _dispatcher = new FakeManagedHttpDispatcher(
                Mocker.GetMock<IHttpProxySettingsProvider>().Object,
                Mocker.GetMock<ICreateManagedWebProxy>().Object,
                Mocker.GetMock<ICertificateValidationService>().Object,
                Mocker.Resolve<UserAgentBuilder>(),
                Mocker.Resolve<CacheManager>(),
                TestLogger,
                _transport);
            Mocker.SetConstant<IHttpDispatcher>(_dispatcher);
            Mocker.SetConstant<IIndexerHttpClient>(new IndexerHttpClient(
                Mocker.GetMock<IIndexerProxyFactory>().Object,
                Array.Empty<IHttpRequestInterceptor>(),
                Mocker.Resolve<CacheManager>(),
                Mocker.Resolve<IRateLimitService>(),
                _dispatcher,
                TestLogger));

            _indexer = Mocker.Resolve<Newznab>();
            _indexer.Definition = new IndexerDefinition
            {
                Id = 42,
                Name = "Test Newznab",
                Enable = true,
                Implementation = nameof(Newznab),
                Settings = new NewznabSettings { BaseUrl = "https://indexer.example", ApiPath = "/api" }
            };

            var capabilities = new IndexerCapabilities { LimitsDefault = 100 };
            capabilities.Categories.AddCategoryMapping(5000, NewznabStandardCategory.TV, "TV");
            capabilities.Categories.AddCategoryMapping(5040, NewznabStandardCategory.TVHD, "TV/HD");
            Mocker.GetMock<INewznabCapabilitiesProvider>()
                .Setup(provider => provider.GetCapabilities(It.IsAny<NewznabSettings>(), It.IsAny<NzbDrone.Core.ThingiProvider.ProviderDefinition>()))
                .Returns(capabilities);
            Mocker.GetMock<IIndexerFactory>()
                .Setup(factory => factory.Enabled(It.IsAny<bool>()))
                .Returns(new List<IIndexer> { _indexer });
            Mocker.GetMock<IIndexerLimitService>()
                .Setup(service => service.AtQueryLimit(It.IsAny<IndexerDefinition>()))
                .Returns(false);
        }

        [Test]
        public async Task actual_search_path_observes_successful_newznab_dispatch()
        {
            SetUpObservedSearch(HttpStatusCode.OK, ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"));

            var result = await Subject.SearchObserved(42, true);

            result.Observation.InternallyObservedSuccess.Should().BeTrue();
            result.TerminalRecord.Outcome.Should().Be("SUCCEEDED");
            result.TerminalRecord.CommitAcknowledged.Should().BeTrue();
            Mocker.Resolve<IIndexerSearchTerminalRecordRepository>().Get(result.TerminalRecord.OperationId)
                .Outcome.Should().Be("SUCCEEDED", "the result is returned only after the real SQLite commit");
            result.Observation.Attempts.Should().ContainSingle().Which.StatusCode.Should().Be(200);
            result.Results.Releases.Should().HaveCount(100, "the fixture response fills the configured Newznab page size");
            _transport.RequestUris.Should().ContainSingle();
            _transport.RequestUris.First().Query.Should().Contain("t=search").And.Contain(result.Observation.CorrelationToken);
        }

        [Test]
        public async Task successful_empty_result_set_is_terminal_success()
        {
            SetUpObservedSearch(HttpStatusCode.OK, "<rss version=\"2.0\"><channel><title>empty</title></channel></rss>");

            var result = await Subject.SearchObserved(42, true);

            result.Results.Releases.Should().BeEmpty();
            result.TerminalRecord.ResultCount.Should().Be(0);
            result.TerminalRecord.Outcome.Should().Be("SUCCEEDED");
            result.TerminalRecord.CommitAcknowledged.Should().BeTrue();
        }

        [Test]
        public async Task persistence_failure_prevents_instrumented_search_acknowledgment()
        {
            SetUpObservedSearch(_ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"), Encoding.UTF8, "application/rss+xml")
            },
            true);
            Mocker.GetMock<IIndexerSearchTerminalRecordRepository>()
                .Setup(repository => repository.Save(It.IsAny<IndexerSearchTerminalRecord>()))
                .Throws(new InvalidOperationException("mock persistence failure"));

            await FluentActions.Awaiting(() => Subject.SearchObserved(42, true))
                .Should().ThrowAsync<InvalidOperationException>();
        }

        [Test]
        public async Task actual_httpindexerbase_failure_handler_makes_observation_incomplete()
        {
            SetUpObservedSearch(HttpStatusCode.NotFound, string.Empty);

            var result = await Subject.SearchObserved(42, true);

            result.Observation.InternallyObservedSuccess.Should().BeFalse();
            result.TerminalRecord.Outcome.Should().Be("FAILED");
            result.Observation.Incomplete.Should().BeTrue();
            result.Observation.Attempts.Should().ContainSingle().Which.StatusCode.Should().Be(404);
            result.Observation.ReasonCodes.Should().Contain("failed_or_transitional_attempt");
            result.Observation.ReasonCodes.Should().Contain("indexer_search_exception");
            _transport.RequestUris.Should().ContainSingle();
            Mocker.Resolve<IIndexerSearchTerminalRecordRepository>().Get(result.TerminalRecord.OperationId)
                .Outcome.Should().Be("FAILED");
            ExceptionVerification.ExpectedErrors(1);
            ExceptionVerification.ExpectedWarns(1);
        }

        [Test]
        public async Task malformed_indexer_response_cannot_be_persisted_as_success()
        {
            SetUpObservedSearch(HttpStatusCode.OK, "<rss><channel><item>");

            var result = await Subject.SearchObserved(42, true);

            result.Observation.InternallyObservedSuccess.Should().BeFalse();
            result.TerminalRecord.Outcome.Should().Be("FAILED");
            Mocker.Resolve<IIndexerSearchTerminalRecordRepository>().Get(result.TerminalRecord.OperationId)
                .Outcome.Should().Be("FAILED");
            ExceptionVerification.ExpectedErrors(1);
        }

        [Test]
        public void supported_basic_search_generator_yields_exactly_one_physical_request()
        {
            SetUpObservedSearch(HttpStatusCode.OK, ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"));
            var generator = _indexer.GetRequestGenerator();
            var criteria = new BasicSearchCriteria
            {
                SearchType = "search",
                SearchTerm = "d8b3-00000000000000000000000000000000",
                IndexerIds = new List<int> { 42 },
                Categories = Array.Empty<int>()
            };

            var pageable = generator.GetSearchRequests(criteria);
            var tiers = pageable.GetAllTiers().ToList();
            tiers.Should().ContainSingle();
            tiers[0].Should().ContainSingle();
        }

        [Test]
        public void observed_search_rejects_indexer_proxy_tags_before_dispatch()
        {
            SetUpObservedSearch(HttpStatusCode.OK, ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"));
            ((IndexerDefinition)_indexer.Definition).Tags.Add(7);

            FluentActions.Awaiting(() => Subject.SearchObserved(42, true)).Should().ThrowAsync<NotSupportedException>();
            _transport.RequestUris.Should().BeEmpty();
        }

        [Test]
        public async Task actual_search_transport_exception_is_not_successful()
        {
            SetUpObservedSearch(_ => throw new HttpRequestException("mock transport failure"));

            var result = await Subject.SearchObserved(42, true);

            result.Observation.InternallyObservedSuccess.Should().BeFalse();
            result.Observation.Incomplete.Should().BeTrue();
            result.Observation.Attempts.Should().ContainSingle().Which.ExceptionType.Should().Be(nameof(HttpRequestException));
            result.Observation.ReasonCodes.Should().Contain("indexer_search_exception");
            _transport.RequestUris.Should().ContainSingle();
            ExceptionVerification.ExpectedWarns(1);
        }

        [Test]
        public async Task actual_search_failed_retry_followed_by_success_remains_ineligible()
        {
            var requests = 0;
            var body = ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml");
            SetUpObservedSearch(_ =>
            {
                requests++;
                return new HttpResponseMessage(requests == 1 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/rss+xml")
                };
            });

            var result = await Subject.SearchObserved(42, true);

            result.Observation.InternallyObservedSuccess.Should().BeFalse();
            result.Observation.Attempts.Should().HaveCount(2);
            result.Observation.Attempts.Select(attempt => attempt.StatusCode).Should().Contain((int)HttpStatusCode.ServiceUnavailable).And.Contain((int)HttpStatusCode.OK);
            result.TerminalRecord.Outcome.Should().Be("FAILED");
            Mocker.Resolve<IIndexerSearchTerminalRecordRepository>().Get(result.TerminalRecord.OperationId)
                .Outcome.Should().Be("FAILED");
            ExceptionVerification.ExpectedWarns(1);
        }

        [Test]
        public async Task concurrent_observed_and_ordinary_searches_remain_isolated()
        {
            SetUpObservedSearch(HttpStatusCode.OK, ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"));

            var observedOneTask = Subject.SearchObserved(42, true);
            var observedTwoTask = Subject.SearchObserved(42, true);
            var ordinaryTask = Subject.Search(new NewznabRequest { t = "search", q = "ordinary query" }, new List<int> { 42 }, false);
            await Task.WhenAll(observedOneTask, observedTwoTask, ordinaryTask);

            var observedOne = await observedOneTask;
            var observedTwo = await observedTwoTask;
            var ordinary = await ordinaryTask;

            observedOne.Observation.InternallyObservedSuccess.Should().BeTrue();
            observedTwo.Observation.InternallyObservedSuccess.Should().BeTrue();
            observedOne.Observation.OperationId.Should().NotBe(observedTwo.Observation.OperationId);
            observedOne.Observation.CorrelationToken.Should().NotBe(observedTwo.Observation.CorrelationToken);
            observedOne.Observation.CorrelationToken.Should().HaveLength(48).And.StartWith("d8b3-");
            observedTwo.Observation.CorrelationToken.Should().HaveLength(48).And.StartWith("d8b3-");
            Convert.FromBase64String(observedOne.Observation.CorrelationToken[5..].Replace('-', '+').Replace('_', '/') + "=").Should().HaveCount(32);
            observedOne.Observation.Attempts.Should().ContainSingle();
            observedTwo.Observation.Attempts.Should().ContainSingle();
            ordinary.Releases.Should().HaveCount(100);
            _transport.RequestUris.Should().HaveCount(3);
            _dispatcher.ObservedDispatches.Should().BeEquivalentTo(new[] { true, true, false });
            _transport.RequestUris.Count(uri => uri.Query.Contains("ordinary%20query", StringComparison.OrdinalIgnoreCase) || uri.Query.Contains("ordinary+query", StringComparison.OrdinalIgnoreCase)).Should().Be(1);
        }

        [Test]
        public async Task pageable_base_retains_success_record_but_fails_b1_f01_after_later_page_error()
        {
            var requests = 0;
            var body = ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml");
            SetUpObservedSearch(_ =>
            {
                requests++;
                if (requests == 2)
                {
                    throw new HttpRequestException("mock second-page failure");
                }

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/rss+xml")
                };
            });

            var pageableIndexer = new PageableNewznab(
                Mocker.Resolve<INewznabCapabilitiesProvider>(),
                Mocker.Resolve<IIndexerHttpClient>(),
                Mocker.Resolve<IEventAggregator>(),
                Mocker.Resolve<IIndexerStatusService>(),
                Mocker.Resolve<IConfigService>(),
                Mocker.Resolve<IValidateNzbs>(),
                TestLogger)
            {
                Definition = _indexer.Definition
            };
            var request = new IndexerRequest("https://indexer.example/api?t=search&extended=1&q=d8b3-00000000000000000000000000000000", HttpAccept.Rss);
            var criteria = new BasicSearchCriteria
            {
                SearchType = "search",
                SearchTerm = "d8b3-00000000000000000000000000000000",
                IndexerIds = new List<int> { 42 },
                Categories = Array.Empty<int>()
            };

            using var scope = SearchOperationObservation.CreateForTest();
            var result = await pageableIndexer.FetchWithPages(request, criteria);
            var observation = scope.CloseForTest();

            requests.Should().Be(2);
            result.Queries.Should().ContainSingle().Which.Response.StatusCode.Should().Be(HttpStatusCode.OK);
            observation.InternallyObservedSuccess.Should().BeFalse();
            observation.Incomplete.Should().BeTrue();
            observation.ReasonCodes.Should().Contain("indexer_search_exception");
            observation.Attempts.Should().HaveCount(2);
            observation.Attempts.Should().Contain(attempt => attempt.StatusCode == (int)HttpStatusCode.OK);
            observation.Attempts.Should().Contain(attempt => attempt.ExceptionType == nameof(HttpRequestException));
            ExceptionVerification.ExpectedWarns(1);
        }

        private sealed class FakeResponseHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, HttpResponseMessage> _responseFactory;

            public FakeResponseHandler(Func<HttpRequestMessage, HttpResponseMessage> responseFactory)
            {
                _responseFactory = responseFactory;
            }

            public ConcurrentQueue<Uri> RequestUris { get; } = new();

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                RequestUris.Enqueue(request.RequestUri);
                await Task.Yield();
                var response = _responseFactory(request);
                response.RequestMessage = request;
                return response;
            }
        }

        private sealed class PageableNewznab : Newznab
        {
            public PageableNewznab(INewznabCapabilitiesProvider capabilitiesProvider,
                IIndexerHttpClient httpClient,
                IEventAggregator eventAggregator,
                IIndexerStatusService indexerStatusService,
                IConfigService configService,
                IValidateNzbs validateNzbs,
                NLog.Logger logger)
                : base(capabilitiesProvider, httpClient, eventAggregator, indexerStatusService, configService, validateNzbs, logger)
            {
            }

            public override TimeSpan RateLimit => TimeSpan.Zero;

            public Task<IndexerPageableQueryResult> FetchWithPages(IndexerRequest request, SearchCriteriaBase criteria)
            {
                var requests = new IndexerPageableRequestChain();
                requests.Add(new[] { request, request });
                return FetchReleases(_ => requests, criteria);
            }
        }

        private sealed class FakeManagedHttpDispatcher : ManagedHttpDispatcher
        {
            private readonly System.Net.Http.HttpClient _client;
            public ConcurrentQueue<bool> ObservedDispatches { get; } = new();

            public FakeManagedHttpDispatcher(IHttpProxySettingsProvider proxySettingsProvider,
                ICreateManagedWebProxy createManagedWebProxy,
                ICertificateValidationService certificateValidationService,
                IUserAgentBuilder userAgentBuilder,
                ICacheManager cacheManager,
                NLog.Logger logger,
                HttpMessageHandler handler)
                : base(proxySettingsProvider, createManagedWebProxy, certificateValidationService, userAgentBuilder, cacheManager, logger)
            {
                _client = new System.Net.Http.HttpClient(handler, false) { Timeout = Timeout.InfiniteTimeSpan };
            }

            protected override System.Net.Http.HttpClient GetClient(HttpUri uri, HttpProxySettings requestProxy, bool observed = false)
            {
                requestProxy.Should().BeNull("the supported test path has no explicit proxy");
                ObservedDispatches.Enqueue(observed);
                return _client;
            }
        }
    }
}
