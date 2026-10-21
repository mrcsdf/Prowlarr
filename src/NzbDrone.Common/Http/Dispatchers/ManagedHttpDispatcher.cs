using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NzbDrone.Common.Cache;
using NzbDrone.Common.Extensions;
using NzbDrone.Common.Http.HappyEyeballs;
using NzbDrone.Common.Http.Proxy;

namespace NzbDrone.Common.Http.Dispatchers
{
    public class ManagedHttpDispatcher : IHttpDispatcher
    {
        private const string NO_PROXY_KEY = "no-proxy";

        private readonly IHttpProxySettingsProvider _proxySettingsProvider;
        private readonly ICreateManagedWebProxy _createManagedWebProxy;
        private readonly ICertificateValidationService _certificateValidationService;
        private readonly IUserAgentBuilder _userAgentBuilder;
        private readonly ICached<System.Net.Http.HttpClient> _httpClientCache;
        private readonly ICached<CredentialCache> _credentialCache;
        private readonly HttpHappyEyeballs _httpHappyEyeballs;

        public ManagedHttpDispatcher(IHttpProxySettingsProvider proxySettingsProvider,
            ICreateManagedWebProxy createManagedWebProxy,
            ICertificateValidationService certificateValidationService,
            IUserAgentBuilder userAgentBuilder,
            ICacheManager cacheManager,
            Logger logger)
        {
            _proxySettingsProvider = proxySettingsProvider;
            _createManagedWebProxy = createManagedWebProxy;
            _certificateValidationService = certificateValidationService;
            _userAgentBuilder = userAgentBuilder;

            _httpClientCache = cacheManager.GetCache<System.Net.Http.HttpClient>(typeof(ManagedHttpDispatcher));
            _credentialCache = cacheManager.GetCache<CredentialCache>(typeof(ManagedHttpDispatcher), "credentialcache");

            _httpHappyEyeballs = new HttpHappyEyeballs(logger);
        }

        public async Task<HttpResponse> GetResponseAsync(HttpRequest request, CookieContainer cookies)
        {
            var observer = HttpRequestExecutionContext.Current;

            // Handler-managed challenges cannot be enumerated as application dispatches.
            if (observer != null && request.Credentials != null && request.Credentials is not BasicNetworkCredential)
            {
                observer.MarkIncomplete("handler_authentication_unobservable");
                throw new NotSupportedException("Handler-managed authentication is unsupported for observed requests");
            }

            using var requestMessage = new HttpRequestMessage(request.Method, (Uri)request.Url)
            {
                Version = HttpVersion.Version20,
                VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
            };
            requestMessage.Headers.UserAgent.ParseAdd(_userAgentBuilder.GetUserAgent(request.UseSimplifiedUserAgent));
            requestMessage.Headers.ConnectionClose = !request.ConnectionKeepAlive;

            var cookieHeader = cookies.GetCookieHeader((Uri)request.Url);
            if (observer != null && !string.IsNullOrWhiteSpace(cookieHeader))
            {
                observer.MarkIncomplete("cookie_authentication_unscoped");
                throw new NotSupportedException("Cookie-authenticated requests are unsupported for observed searches");
            }

            if (cookieHeader.IsNotNullOrWhiteSpace())
            {
                requestMessage.Headers.Add("Cookie", cookieHeader);
            }

            var operationToken = HttpRequestExecutionContext.CancellationToken;
            using var cts = operationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(operationToken)
                : new CancellationTokenSource();
            var requestTimeout = request.RequestTimeout == TimeSpan.Zero
                ? TimeSpan.FromSeconds(100) // The default for System.Net.Http.HttpClient
                : request.RequestTimeout;
            if (observer != null && requestTimeout > TimeSpan.FromSeconds(30))
            {
                requestTimeout = TimeSpan.FromSeconds(30);
            }

            cts.CancelAfter(requestTimeout);

            if (request.Credentials != null)
            {
                if (request.Credentials is BasicNetworkCredential bc)
                {
                    // Manually set header to avoid initial challenge response
                    var authInfo = bc.UserName + ":" + bc.Password;
                    authInfo = Convert.ToBase64String(Encoding.UTF8.GetBytes(authInfo));
                    requestMessage.Headers.Add("Authorization", "Basic " + authInfo);
                }
                else if (request.Credentials is NetworkCredential nc)
                {
                    var creds = GetCredentialCache();
                    foreach (var authtype in new[] { "Basic", "Digest" })
                    {
                        creds.Remove((Uri)request.Url, authtype);
                        creds.Add((Uri)request.Url, authtype, nc);
                    }
                }
            }

            if (request.ContentData != null)
            {
                requestMessage.Content = new ByteArrayContent(request.ContentData);
            }

            if (request.Headers != null)
            {
                if (observer != null && request.Headers.Any(h => string.Equals(h.Key, "Authorization", StringComparison.OrdinalIgnoreCase)))
                {
                    observer.MarkIncomplete("authorization_header_unscoped");
                    throw new NotSupportedException("Caller-provided Authorization headers are unsupported for observed searches");
                }

                AddRequestHeaders(requestMessage, request.Headers);
            }

            var httpClient = GetClient(request.Url, request.ProxySettings, observer != null);

            var sw = new Stopwatch();

            sw.Start();

            IHttpRequestAttemptScope attempt = null;
            IAsyncHttpRequestAttemptScope asyncAttempt = null;
            if (observer is IAsyncHttpRequestExecutionObserver asyncObserver)
            {
                asyncAttempt = await asyncObserver.BeginAttemptAsync(
                    request.Method.Method,
                    (Uri)request.Url,
                    DateTimeOffset.UtcNow,
                    cts.Token).ConfigureAwait(false);
            }
            else
            {
                attempt = observer?.BeginAttempt(request.Method.Method, (Uri)request.Url, DateTimeOffset.UtcNow);
            }

            try
            {
                using var responseMessage = await httpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                {
                    byte[] data = null;

                    try
                    {
                        if (request.ResponseStream != null && responseMessage.StatusCode == HttpStatusCode.OK)
                        {
                            await responseMessage.Content.CopyToAsync(request.ResponseStream, null, cts.Token);
                        }
                        else
                        {
                            data = await responseMessage.Content.ReadAsByteArrayAsync(cts.Token);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new WebException("Failed to read complete http response", ex, WebExceptionStatus.ReceiveFailure, null);
                    }

                    var headers = responseMessage.Headers.ToNameValueCollection();

                    headers.Add(responseMessage.Content.Headers.ToNameValueCollection());

                    var responseCookies = new CookieContainer();

                    if (responseMessage.Headers.TryGetValues("Set-Cookie", out var cookieHeaders))
                    {
                        foreach (var responseCookieHeader in cookieHeaders)
                        {
                            try
                            {
                                cookies.SetCookies(responseMessage.RequestMessage.RequestUri, responseCookieHeader);
                            }
                            catch
                            {
                                // Ignore invalid cookies
                            }
                        }
                    }

                    var cookieCollection = cookies.GetCookies(responseMessage.RequestMessage.RequestUri);

                    sw.Stop();

                    var response = new HttpResponse(request, new HttpHeader(headers), cookieCollection, data, sw.ElapsedMilliseconds, responseMessage.StatusCode, responseMessage.Version);
                    if (asyncAttempt != null)
                    {
                        await asyncAttempt.CompleteAsync((int)responseMessage.StatusCode, DateTimeOffset.UtcNow, cts.Token).ConfigureAwait(false);
                    }
                    else
                    {
                        attempt?.Complete((int)responseMessage.StatusCode, DateTimeOffset.UtcNow);
                    }

                    return response;
                }
            }
            catch (OperationCanceledException ex) when (cts.IsCancellationRequested)
            {
                var timeoutException = new WebException("Http request timed out", ex, WebExceptionStatus.Timeout, null);
                if (asyncAttempt != null)
                {
                    await asyncAttempt.FailAsync("RequestTimeoutException", DateTimeOffset.UtcNow, cts.Token).ConfigureAwait(false);
                }
                else
                {
                    attempt?.Fail("RequestTimeoutException", DateTimeOffset.UtcNow);
                }

                throw timeoutException;
            }
            catch (Exception ex)
            {
                if (asyncAttempt != null)
                {
                    await asyncAttempt.FailAsync(ex.GetType().Name, DateTimeOffset.UtcNow, cts.Token).ConfigureAwait(false);
                }
                else
                {
                    attempt?.Fail(ex.GetType().Name, DateTimeOffset.UtcNow);
                }

                throw;
            }
            finally
            {
                if (asyncAttempt != null)
                {
                    await asyncAttempt.DisposeAsync().ConfigureAwait(false);
                }
                else
                {
                    attempt?.Dispose();
                }
            }
        }

        protected virtual System.Net.Http.HttpClient GetClient(HttpUri uri, HttpProxySettings requestProxy, bool observed = false)
        {
            var proxySettings = requestProxy ?? _proxySettingsProvider.GetProxySettings(uri);

            // The observation contract has no independently approved proxy identity and cannot
            // account for proxy-side retries, redirects or authentication. Reject every proxy.
            if (observed && proxySettings != null)
            {
                HttpRequestExecutionContext.MarkIncomplete("proxy_path_unapproved");
                throw new NotSupportedException("Proxy-routed observed requests are unsupported");
            }

            var key = (observed ? "observed:" : string.Empty) + (proxySettings?.Key ?? NO_PROXY_KEY);

            return _httpClientCache.Get(key, () => CreateHttpClient(proxySettings, observed));
        }

        protected virtual System.Net.Http.HttpClient CreateHttpClient(HttpProxySettings proxySettings, bool observed = false)
        {
            var handler = CreateHandler(proxySettings, observed);

            var client = new System.Net.Http.HttpClient(handler)
            {
                DefaultRequestVersion = HttpVersion.Version20,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
                Timeout = Timeout.InfiniteTimeSpan
            };

            return client;
        }

        protected virtual SocketsHttpHandler CreateHandler(HttpProxySettings proxySettings, bool observed)
        {
            var handler = new SocketsHttpHandler()
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Brotli,
                UseCookies = false, // sic - we don't want to use a shared cookie container
                UseProxy = !observed,
                AllowAutoRedirect = false,
                Credentials = observed ? null : GetCredentialCache(),
                PreAuthenticate = !observed,
                MaxConnectionsPerServer = 12,
                ConnectCallback = Socket.OSSupportsIPv6 ? _httpHappyEyeballs.OnConnect : null,
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                SslOptions = new SslClientAuthenticationOptions
                {
                    // Evidence observations must not inherit Prowlarr's configurable
                    // certificate-validation bypasses (including loopback exemptions).
                    RemoteCertificateValidationCallback = observed
                        ? (_, _, _, sslPolicyErrors) => sslPolicyErrors == SslPolicyErrors.None
                        : _certificateValidationService.ShouldByPassValidationError
                }
            };

            if (proxySettings != null)
            {
                handler.Proxy = _createManagedWebProxy.GetWebProxy(proxySettings);
            }

            return handler;
        }

        protected virtual void AddRequestHeaders(HttpRequestMessage webRequest, HttpHeader headers)
        {
            foreach (var header in headers)
            {
                switch (header.Key)
                {
                    case "Accept":
                        webRequest.Headers.Accept.ParseAdd(header.Value);
                        break;
                    case "Connection":
                        webRequest.Headers.Connection.Clear();
                        webRequest.Headers.Connection.Add(header.Value);
                        break;
                    case "Content-Length":
                        AddContentHeader(webRequest, "Content-Length", header.Value);
                        break;
                    case "Content-Type":
                        AddContentHeader(webRequest, "Content-Type", header.Value);
                        break;
                    case "Date":
                        webRequest.Headers.Remove("Date");
                        webRequest.Headers.Date = HttpHeader.ParseDateTime(header.Value);
                        break;
                    case "Expect":
                        webRequest.Headers.Expect.ParseAdd(header.Value);
                        break;
                    case "Host":
                        webRequest.Headers.Host = header.Value;
                        break;
                    case "If-Modified-Since":
                        webRequest.Headers.IfModifiedSince = HttpHeader.ParseDateTime(header.Value);
                        break;
                    case "Range":
                        throw new NotImplementedException();
                    case "Referer":
                        webRequest.Headers.Add("Referer", header.Value);
                        break;
                    case "Transfer-Encoding":
                        webRequest.Headers.TransferEncoding.ParseAdd(header.Value);
                        break;
                    case "User-Agent":
                        webRequest.Headers.UserAgent.Clear();
                        webRequest.Headers.UserAgent.ParseAdd(header.Value);
                        break;
                    case "Proxy-Connection":
                        throw new NotImplementedException();
                    default:
                        webRequest.Headers.Add(header.Key, header.Value);
                        break;
                }
            }
        }

        private static void AddContentHeader(HttpRequestMessage request, string header, string value)
        {
            var headers = request.Content?.Headers;
            if (headers == null)
            {
                return;
            }

            headers.Remove(header);
            headers.Add(header, value);
        }

        private CredentialCache GetCredentialCache()
        {
            return _credentialCache.Get("credentialCache", () => new CredentialCache());
        }
    }
}
