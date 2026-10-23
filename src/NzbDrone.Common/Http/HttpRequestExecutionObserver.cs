using System;
using System.Threading;
using System.Threading.Tasks;

namespace NzbDrone.Common.Http
{
    /// <summary>Observes application dispatches only; it does not claim packet or proxy-internal visibility.</summary>
    public interface IHttpRequestExecutionObserver
    {
        IHttpRequestAttemptScope BeginAttempt(string method, Uri requestUri, DateTimeOffset startedAt);
        void MarkIncomplete(string reasonCode);
        CancellationToken CancellationToken { get; }
    }

    /// <summary>Awaited observation for operations that must durably gate transport dispatch.</summary>
    public interface IAsyncHttpRequestExecutionObserver
    {
        Task<IAsyncHttpRequestAttemptScope> BeginAttemptAsync(string method, Uri requestUri, DateTimeOffset startedAt, CancellationToken cancellationToken);
    }

    public interface IAsyncHttpRequestAttemptScope
    {
        Task CompleteAsync(int statusCode, DateTimeOffset completedAt, CancellationToken cancellationToken);
        Task FailAsync(string exceptionType, DateTimeOffset completedAt, CancellationToken cancellationToken);
        ValueTask DisposeAsync();
    }

    public interface IHttpRequestAttemptScope : IDisposable
    {
        void Complete(int statusCode, DateTimeOffset completedAt);
        void Fail(string exceptionType, DateTimeOffset completedAt);
    }

    public static class HttpRequestExecutionContext
    {
        private static readonly AsyncLocal<IHttpRequestExecutionObserver> CurrentObserver = new();

        public static IHttpRequestExecutionObserver Current => CurrentObserver.Value;

        public static CancellationToken CancellationToken => CurrentObserver.Value?.CancellationToken ?? System.Threading.CancellationToken.None;

        public static IDisposable IsolateOperationContext()
        {
            var prior = CurrentObserver.Value;
            CurrentObserver.Value = null;
            return new RestoreScope(prior);
        }

        public static IDisposable Begin(IHttpRequestExecutionObserver observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var prior = CurrentObserver.Value;
            CurrentObserver.Value = observer;
            return new RestoreScope(prior);
        }

        public static void MarkIncomplete(string reasonCode)
        {
            CurrentObserver.Value?.MarkIncomplete(reasonCode);
        }

        private sealed class RestoreScope : IDisposable
        {
            private readonly IHttpRequestExecutionObserver _prior;
            private bool _disposed;

            public RestoreScope(IHttpRequestExecutionObserver prior) => _prior = prior;

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }

                CurrentObserver.Value = _prior;
                _disposed = true;
            }
        }
    }
}
