using System;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using NzbDrone.Core.IndexerSearch;

namespace NzbDrone.Host
{
    /// <summary>
    /// Optional, isolated, mTLS-only listener. It is deliberately not part of Startup's
    /// administrative API pipeline, so API-key fallback and public routes cannot reach it.
    /// </summary>
    public sealed class TerminalEvidenceListener
    {
        private static readonly TimeSpan StartupRecoveryBudget = TimeSpan.FromSeconds(15);
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private readonly IConfiguration _configuration;
        private readonly TerminalEvidenceWorkerClient _workerClient;
        private readonly IReleaseSearchService _releaseSearchService;
        private readonly SemaphoreSlim _concurrency = new(8, 8);
        private readonly object _rateGate = new();
        private IWebHost _host;
        private int _requestsInWindow;
        private long _windowStartedAt;
        private volatile bool _recoveryReady = true;
        private X509Certificate2 _clientCa;
        private HashSet<string> _allowedClientCertificates;
        private HashSet<string> _allowedInitiatorClientCertificates = new(StringComparer.Ordinal);
        private HashSet<string> _allowedInitiatorUriSans = new(StringComparer.Ordinal);
        private HashSet<int> _allowedInitiatorIndexerIds = new();
        private string _issuer;
        private string _audience;
        private string _keyId;
        private string _publicKeyPem;
        private int _requestsPerMinute;
        private int _timeoutMilliseconds;

        public TerminalEvidenceListener(
            IConfiguration configuration,
            TerminalEvidenceWorkerClient workerClient,
            IReleaseSearchService releaseSearchService = null)
        {
            _configuration = configuration;
            _workerClient = workerClient;
            _releaseSearchService = releaseSearchService;
        }

        public bool IsRecoveryReady => _recoveryReady;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_releaseSearchService != null)
            {
                _recoveryReady = false;
                try
                {
                    await _releaseSearchService.RecoverD8InterruptedSearchesAsync(StartupRecoveryBudget, cancellationToken).ConfigureAwait(false);
                    _recoveryReady = true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    Logger.Error("D8 startup recovery did not complete; listener requests are disabled ({0})", GetSafeFailureCategory(exception));
                }
            }

            if (!_configuration.GetValue<bool>("Prowlarr:D8TerminalEvidence:Enabled"))
            {
                return;
            }

            if (!OperatingSystem.IsLinux())
            {
                throw new InvalidOperationException("D8 process-isolated terminal evidence is supported only on Linux");
            }

            (_workerClient ?? throw new InvalidOperationException("D8 worker client is unavailable")).ValidateConfiguration();

            var section = _configuration.GetSection("Prowlarr:D8TerminalEvidence");
            var bindAddress = ParseInternalAddress(section["BindAddress"]);
            var port = section.GetValue<int>("Port", 9697);
            var apiPort = _configuration.GetValue<int?>("Prowlarr:Server:Port") ?? 9696;
            var sslPort = _configuration.GetValue<int?>("Prowlarr:Server:SslPort") ?? 6969;
            if (port is < 1 or > 65535 || port == apiPort || port == sslPort)
            {
                throw new InvalidOperationException("D8 terminal evidence listener port is invalid or overlaps the application listener");
            }

            var certificatePath = RequiredPath(section["ServerCertificatePath"]);
            var certificatePassword = section["ServerCertificatePassword"];
            var caPath = RequiredPath(section["ClientCaCertificatePath"]);
            var publicKeyPath = RequiredPath(section["CapabilityPublicKeyPath"]);
            if (new System.IO.FileInfo(publicKeyPath).Length is < 100 or > 16384)
            {
                throw new InvalidOperationException("D8 terminal evidence capability public key size is outside policy");
            }

            _publicKeyPem = System.IO.File.ReadAllText(publicKeyPath);
            _issuer = Required(section["CapabilityIssuer"]);
            _audience = Required(section["CapabilityAudience"]);
            _keyId = Required(section["CapabilityKeyId"]);
            _allowedClientCertificates = ParseCertificateAllowlist(section["AllowedClientCertificateSha256"]);
            _allowedInitiatorClientCertificates = ParseOptionalCertificateAllowlist(section["AllowedInitiatorCertificateSha256"]);
            _allowedInitiatorUriSans = ParseUriAllowlist(section["AllowedInitiatorUriSans"]);
            _allowedInitiatorIndexerIds = ParseIndexerAllowlist(section["AllowedInitiatorIndexerIds"]);
            _requestsPerMinute = section.GetValue<int>("RequestsPerMinute", 60);
            _timeoutMilliseconds = section.GetValue<int>("RequestTimeoutMilliseconds", 3000);
            if (_requestsPerMinute is < 1 or > 600 || _timeoutMilliseconds is < 100 or > 10000)
            {
                throw new InvalidOperationException("D8 terminal evidence rate or timeout setting is outside the allowed range");
            }

            var serverCertificate = new X509Certificate2(certificatePath, certificatePassword, X509KeyStorageFlags.EphemeralKeySet);
            _clientCa = new X509Certificate2(caPath);
            if (!serverCertificate.HasPrivateKey || !_clientCa.Extensions.OfType<X509BasicConstraintsExtension>().Any(extension => extension.CertificateAuthority))
            {
                serverCertificate.Dispose();
                _clientCa.Dispose();
                throw new InvalidOperationException("D8 terminal evidence requires a server private key and a CA certificate for client authentication");
            }

            _host = new WebHostBuilder()
                .UseKestrel(options =>
                {
                    options.Limits.MaxRequestBodySize = 1024;
                    options.Limits.MaxRequestLineSize = 2048;
                    options.Limits.MaxRequestHeadersTotalSize = 8192;
                    options.Limits.MaxConcurrentConnections = 32;
                    options.Listen(bindAddress, port, listen => listen.UseHttps(serverCertificate, https =>
                    {
                        https.ClientCertificateMode = Microsoft.AspNetCore.Server.Kestrel.Https.ClientCertificateMode.RequireCertificate;
                        https.ClientCertificateValidation = ValidateClientCertificate;
                    }));
                })
                .Configure(app => app.Run(HandleRequestAsync))
                .Build();

            await _host.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (_workerClient != null)
                {
                    await _workerClient.StopAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (_host != null)
                {
                    await _host.StopAsync(cancellationToken).ConfigureAwait(false);
                    _host.Dispose();
                    _host = null;
                }

                _clientCa?.Dispose();
            }
        }

        private async Task HandleRequestAsync(HttpContext context)
        {
            context.Response.Headers.CacheControl = "no-store";
            context.Response.Headers.Pragma = "no-cache";
            if (!_recoveryReady)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                return;
            }

            if (!TryTakeRateSlot() || !await _concurrency.WaitAsync(0, context.RequestAborted).ConfigureAwait(false))
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                return;
            }

            try
            {
                if (context.Request.Headers.Count > 32 || context.Connection.ClientCertificate == null)
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                var path = context.Request.Path.Value;
                if (string.Equals(context.Request.Method, "POST", StringComparison.Ordinal) && path == "/v1/search-operations")
                {
                    await HandleInitiationAsync(context).ConfigureAwait(false);
                    return;
                }

                if (!string.Equals(context.Request.Method, "GET", StringComparison.Ordinal) ||
                    context.Request.QueryString.HasValue || context.Request.ContentLength.GetValueOrDefault() != 0 ||
                    context.Request.Headers.TransferEncoding.Count != 0)
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                var prefix = "/v1/terminal-records/";
                if (path == null || !path.StartsWith(prefix, StringComparison.Ordinal) ||
                    path.Length != prefix.Length + 36 || !Guid.TryParseExact(path.Substring(prefix.Length), "D", out _))
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                var authorization = context.Request.Headers.Authorization.ToString();
                const string scheme = "Capability ";
                if (!authorization.StartsWith(scheme, StringComparison.Ordinal) || authorization.Length == scheme.Length)
                {
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                var certificateFingerprint = Convert.ToHexString(SHA256.HashData(context.Connection.ClientCertificate.RawData)).ToLowerInvariant();
                if (!_allowedClientCertificates.Contains(certificateFingerprint))
                {
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return;
                }

                var capability = TerminalEvidenceCapabilityVerifier.Verify(
                    authorization.Substring(scheme.Length),
                    certificateFingerprint,
                    path.Substring(prefix.Length),
                    _issuer,
                    _audience,
                    _keyId,
                    _publicKeyPem,
                    DateTimeOffset.UtcNow);
                if (capability == null)
                {
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
                timeout.CancelAfter(_timeoutMilliseconds);
                var workerResult = await _workerClient.ReadAndRedeemAsync(
                    capability.OperationId,
                    capability.CorrelationId,
                    capability.TokenId,
                    capability.ExpiresAtUnixMs,
                    timeout.Token).ConfigureAwait(false);
                if (!workerResult.Redeemed)
                {
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                var record = workerResult.Record;
                if (record == null || record.RequestCorrelationId != capability.CorrelationId || !record.CommitAcknowledged)
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(record));
                if (bytes.Length > IndexerSearchTerminalContract.MaximumRecordBytes)
                {
                    context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                    return;
                }

                context.Response.ContentType = "application/json; charset=utf-8";
                context.Response.ContentLength = bytes.Length;
                await context.Response.Body.WriteAsync(bytes, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                if (!context.RequestAborted.IsCancellationRequested)
                {
                    context.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                }
            }
            catch (Exception exception)
            {
                Logger.Warn("D8 evidence request failed ({0})", GetSafeFailureCategory(exception));
                if (!context.Response.HasStarted)
                {
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                }
            }
            finally
            {
                _concurrency.Release();
            }
        }

        private static string GetSafeFailureCategory(Exception exception) => exception switch
        {
            D8StorageException storageException => storageException.Category,
            OperationCanceledException => "cancelled-or-deadline",
            _ => "internal"
        };

        private async Task HandleInitiationAsync(HttpContext context)
        {
            using var deadline = new D8SearchDeadline(context.RequestAborted);
            deadline.ThrowIfExpired();
            if (context.Request.QueryString.HasValue || context.Request.Headers.TransferEncoding.Count != 0 ||
                context.Request.Headers.Authorization.Count != 0 || context.Request.ContentLength is < 2 or > 1024)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            if (_releaseSearchService == null || _allowedInitiatorUriSans.Count == 0 || _allowedInitiatorIndexerIds.Count == 0)
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            var callerIdentity = GetAuthorizedInitiatorUri(context.Connection.ClientCertificate);
            deadline.ThrowIfExpired();
            if (callerIdentity == null)
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            var request = await ReadInitiationRequestAsync(context.Request, deadline.Token).ConfigureAwait(false);
            deadline.ThrowIfExpired();
            if (request == null)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            if (!_allowedInitiatorIndexerIds.Contains(request.IndexerId))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            deadline.ThrowIfExpired();
            var result = await _releaseSearchService.InitiateD8ValidationSearchAsync(
                request.IndexerId,
                callerIdentity,
                request.IdempotencyKey,
                deadline).ConfigureAwait(false);
            deadline.ThrowIfExpired();
            var response = new
            {
                version = 1,
                operationId = result.Operation.OperationId,
                correlationId = result.Operation.CorrelationId,
                state = result.Operation.State,
                outcome = result.TerminalRecord?.Outcome,
                started = result.Started
            };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response));
            deadline.ThrowIfExpired();
            context.Response.StatusCode = !result.Started && (result.Operation.State is "RUNNING" or "FINALIZING")
                ? StatusCodes.Status202Accepted
                : StatusCodes.Status200OK;
            context.Response.ContentType = "application/json; charset=utf-8";
            context.Response.ContentLength = bytes.Length;
            await context.Response.Body.WriteAsync(bytes, deadline.Token).ConfigureAwait(false);
        }

        private string GetAuthorizedInitiatorUri(X509Certificate certificate)
        {
            using var client = new X509Certificate2(certificate);
            var fingerprint = Convert.ToHexString(SHA256.HashData(client.RawData)).ToLowerInvariant();
            if (!_allowedInitiatorClientCertificates.Contains(fingerprint))
            {
                return null;
            }

            var subjectAlternativeName = client.Extensions.Cast<X509Extension>().SingleOrDefault(extension => extension.Oid?.Value == "2.5.29.17");
            if (subjectAlternativeName == null)
            {
                return null;
            }

            var matches = ReadUriSubjectAlternativeNames(subjectAlternativeName)
                .Where(_allowedInitiatorUriSans.Contains)
                .Distinct(StringComparer.Ordinal)
                .Take(2)
                .ToArray();
            return matches.Length == 1 ? matches[0] : null;
        }

        private static string[] ReadUriSubjectAlternativeNames(X509Extension extension)
        {
            try
            {
                var outer = new AsnReader(extension.RawData, AsnEncodingRules.DER);
                var names = outer.ReadSequence();
                var uris = new List<string>();
                while (names.HasData)
                {
                    var tag = names.PeekTag();
                    if (tag.TagClass == TagClass.ContextSpecific && tag.TagValue == 6)
                    {
                        uris.Add(names.ReadCharacterString(UniversalTagNumber.IA5String, new Asn1Tag(TagClass.ContextSpecific, 6)));
                    }
                    else
                    {
                        names.ReadEncodedValue();
                    }
                }

                return outer.HasData ? Array.Empty<string>() : uris.ToArray();
            }
            catch (AsnContentException)
            {
                return Array.Empty<string>();
            }
        }

        private static async Task<D8SearchInitiationRequest> ReadInitiationRequestAsync(HttpRequest request, CancellationToken cancellationToken)
        {
            if (!request.ContentLength.HasValue || request.ContentLength.Value is < 2 or > 1024 ||
                !string.Equals(request.ContentType?.Split(';')[0], "application/json", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            var body = new byte[(int)request.ContentLength.Value];
            var read = 0;
            while (read < body.Length)
            {
                var count = await request.Body.ReadAsync(body.AsMemory(read), cancellationToken).ConfigureAwait(false);
                if (count == 0)
                {
                    return null;
                }

                read += count;
            }

            try
            {
                var json = new UTF8Encoding(false, true).GetString(body);
                using var textReader = new StringReader(json);
                using var reader = new JsonTextReader(textReader) { DateParseHandling = DateParseHandling.None, MaxDepth = 2 };
                var value = JObject.Load(reader, new JsonLoadSettings { DuplicatePropertyNameHandling = DuplicatePropertyNameHandling.Error });
                if (reader.Read() || value.Properties().Any(property => property.Name is not ("version" or "indexerId" or "idempotencyKey")) ||
                    value["version"]?.Type != JTokenType.Integer || value["version"].Value<int>() != 1 ||
                    value["indexerId"]?.Type != JTokenType.Integer || value["indexerId"].Value<int>() <= 0 ||
                    value["idempotencyKey"]?.Type != JTokenType.String)
                {
                    return null;
                }

                var parsed = new D8SearchInitiationRequest(value["indexerId"].Value<int>(), (string)value["idempotencyKey"]);
                return parsed.IdempotencyKey.Length is >= 1 and <= 128 &&
                    parsed.IdempotencyKey.All(character => char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '~' or '-')
                    ? parsed
                    : null;
            }
            catch (Exception exception) when (exception is JsonException or DecoderFallbackException or ArgumentException)
            {
                return null;
            }
        }

        private bool ValidateClientCertificate(X509Certificate certificate, X509Chain chain, System.Net.Security.SslPolicyErrors errors)
        {
            if (certificate == null)
            {
                return false;
            }

            using var client = new X509Certificate2(certificate);
            if (DateTime.UtcNow < client.NotBefore || DateTime.UtcNow > client.NotAfter)
            {
                return false;
            }

            var fingerprint = Convert.ToHexString(SHA256.HashData(client.RawData)).ToLowerInvariant();
            if (!_allowedClientCertificates.Contains(fingerprint) && !_allowedInitiatorClientCertificates.Contains(fingerprint))
            {
                return false;
            }

            if (!client.Extensions.OfType<X509EnhancedKeyUsageExtension>()
                    .Any(extension => extension.EnhancedKeyUsages.Cast<Oid>().Any(oid => oid.Value == "1.3.6.1.5.5.7.3.2")))
            {
                return false;
            }

            using var clientChain = new X509Chain();
            clientChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            clientChain.ChainPolicy.CustomTrustStore.Add(_clientCa);
            clientChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            clientChain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;
            return clientChain.Build(client);
        }

        private bool TryTakeRateSlot()
        {
            lock (_rateGate)
            {
                var now = Environment.TickCount64;
                if (now - _windowStartedAt >= 60000)
                {
                    _windowStartedAt = now;
                    _requestsInWindow = 0;
                }

                if (_requestsInWindow >= _requestsPerMinute)
                {
                    return false;
                }

                _requestsInWindow++;
                return true;
            }
        }

        private static IPAddress ParseInternalAddress(string value)
        {
            if (!IPAddress.TryParse(value, out var address) || !IsInternalAddress(address))
            {
                throw new InvalidOperationException("D8 terminal evidence listener must bind to an explicit loopback or private address");
            }

            return address;
        }

        private static bool IsInternalAddress(IPAddress address)
        {
            if (IPAddress.IsLoopback(address))
            {
                return true;
            }

            var bytes = address.GetAddressBytes();
            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            {
                return bytes[0] == 10 || (bytes[0] == 172 && bytes[1] is >= 16 and <= 31) || (bytes[0] == 192 && bytes[1] == 168);
            }

            return address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 && (bytes[0] & 0xfe) == 0xfc;
        }

        private static string RequiredPath(string value)
        {
            var path = Required(value);
            return System.IO.Path.GetFullPath(path);
        }

        private static string Required(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException("Required D8 terminal evidence configuration is missing");
            }

            return value;
        }

        private static HashSet<string> ParseCertificateAllowlist(string value)
        {
            var fingerprints = (value ?? string.Empty)
                .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (fingerprints.Length == 0 || fingerprints.Any(fingerprint => fingerprint.Length != 64 ||
                fingerprint.Any(character => !Uri.IsHexDigit(character)) || fingerprint != fingerprint.ToLowerInvariant()))
            {
                throw new InvalidOperationException("D8 terminal evidence requires a lowercase SHA-256 client certificate allowlist");
            }

            return new HashSet<string>(fingerprints, StringComparer.Ordinal);
        }

        private static HashSet<string> ParseOptionalCertificateAllowlist(string value)
        {
            var fingerprints = (value ?? string.Empty).Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (fingerprints.Any(fingerprint => fingerprint.Length != 64 || fingerprint.Any(character => !Uri.IsHexDigit(character)) ||
                fingerprint != fingerprint.ToLowerInvariant()))
            {
                throw new InvalidOperationException("D8 initiator certificate allowlist requires lowercase SHA-256 fingerprints");
            }

            return new HashSet<string>(fingerprints, StringComparer.Ordinal);
        }

        private static HashSet<string> ParseUriAllowlist(string value)
        {
            var uris = (value ?? string.Empty).Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (uris.Any(item => !Uri.TryCreate(item, UriKind.Absolute, out var uri) ||
                !string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment) ||
                !string.Equals(uri.AbsoluteUri, item, StringComparison.Ordinal)))
            {
                throw new InvalidOperationException("D8 initiator URI-SAN allowlist contains an invalid exact URI");
            }

            return new HashSet<string>(uris, StringComparer.Ordinal);
        }

        private static HashSet<int> ParseIndexerAllowlist(string value)
        {
            var entries = (value ?? string.Empty).Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            var ids = new HashSet<int>();
            foreach (var entry in entries)
            {
                if (!int.TryParse(entry, out var id) || id <= 0 || !ids.Add(id))
                {
                    throw new InvalidOperationException("D8 initiator indexer allowlist contains an invalid or duplicate identifier");
                }
            }

            return ids;
        }

        private sealed record D8SearchInitiationRequest(int IndexerId, string IdempotencyKey);
    }
}
