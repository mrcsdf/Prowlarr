using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace NzbDrone.Core.IndexerSearch
{
    public sealed class TerminalEvidenceCapability
    {
        public string TokenId { get; init; }
        public string OperationId { get; init; }
        public string CorrelationId { get; init; }
        public string CertificateSha256 { get; init; }
        public long ExpiresAtUnixMs { get; init; }
    }

    public static class TerminalEvidenceCapabilityVerifier
    {
        private const int MaximumTokenBytes = 4096;
        private const int MaximumLifetimeSeconds = 60;
        private static readonly TimeSpan ClockSkew = TimeSpan.FromSeconds(5);

        public static TerminalEvidenceCapability Verify(
            string token,
            string certificateSha256,
            string operationId,
            string issuer,
            string audience,
            string keyId,
            string publicKeyPem,
            DateTimeOffset now)
        {
            if (string.IsNullOrWhiteSpace(token) || Encoding.UTF8.GetByteCount(token) > MaximumTokenBytes ||
                !Guid.TryParseExact(operationId, "D", out _) || string.IsNullOrWhiteSpace(certificateSha256))
            {
                return null;
            }

            var parts = token.Split('.');
            if (parts.Length != 3)
            {
                return null;
            }

            try
            {
                var headerBytes = Decode(parts[0]);
                var payloadBytes = Decode(parts[1]);
                var signature = Decode(parts[2]);
                using var header = JsonDocument.Parse(headerBytes);
                using var payload = JsonDocument.Parse(payloadBytes);
                var headerRoot = header.RootElement;
                var claims = payload.RootElement;
                if (!HasExactProperties(headerRoot, "alg", "typ", "kid") ||
                    !HasExactProperties(claims, "iss", "aud", "scope", "operation_id", "correlation_id", "certificate_sha256", "jti", "iat", "exp") ||
                    headerRoot.GetProperty("alg").GetString() != "RS256" ||
                    headerRoot.GetProperty("typ").GetString() != "JWT" ||
                    headerRoot.GetProperty("kid").GetString() != keyId ||
                    claims.GetProperty("iss").GetString() != issuer ||
                    claims.GetProperty("aud").GetString() != audience ||
                    claims.GetProperty("scope").GetString() != "terminal-record:read" ||
                    claims.GetProperty("operation_id").GetString() != operationId ||
                    claims.GetProperty("certificate_sha256").GetString() != certificateSha256)
                {
                    return null;
                }

                var issuedAt = claims.GetProperty("iat").GetInt64();
                var expiresAt = claims.GetProperty("exp").GetInt64();
                var nowSeconds = now.ToUnixTimeSeconds();
                if (issuedAt > nowSeconds + (long)ClockSkew.TotalSeconds ||
                    expiresAt <= nowSeconds || expiresAt <= issuedAt ||
                    expiresAt - issuedAt > MaximumLifetimeSeconds ||
                    nowSeconds - issuedAt > MaximumLifetimeSeconds + (long)ClockSkew.TotalSeconds)
                {
                    return null;
                }

                var tokenId = claims.GetProperty("jti").GetString();
                var correlationId = claims.GetProperty("correlation_id").GetString();
                if (!Guid.TryParseExact(tokenId, "D", out var parsedTokenId) ||
                    parsedTokenId.ToString("D") != tokenId ||
                    !IsValidCorrelationId(correlationId))
                {
                    return null;
                }

                using var rsa = RSA.Create();
                rsa.ImportFromPem(publicKeyPem);
                var signingInput = Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]);
                if (!rsa.VerifyData(signingInput, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
                {
                    return null;
                }

                return new TerminalEvidenceCapability
                {
                    TokenId = tokenId,
                    OperationId = operationId,
                    CorrelationId = correlationId,
                    CertificateSha256 = certificateSha256,
                    ExpiresAtUnixMs = DateTimeOffset.FromUnixTimeSeconds(expiresAt).ToUnixTimeMilliseconds()
                };
            }
            catch (Exception exception) when (exception is FormatException or JsonException or InvalidOperationException or KeyNotFoundException or CryptographicException or IOException or ArgumentException or OverflowException)
            {
                return null;
            }
        }

        private static byte[] Decode(string value)
        {
            if (string.IsNullOrEmpty(value) || value.Any(character =>
                !(character is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_')))
            {
                throw new FormatException("Invalid base64url encoding");
            }

            var base64 = value.Replace('-', '+').Replace('_', '/');
            var paddingLength = (4 - (base64.Length % 4)) % 4;
            base64 = base64.PadRight(base64.Length + paddingLength, '=');
            var decoded = Convert.FromBase64String(base64);
            if (Encode(decoded) != value)
            {
                throw new FormatException("Non-canonical base64url encoding");
            }

            return decoded;
        }

        private static string Encode(byte[] value)
        {
            return Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        }

        private static bool HasExactProperties(JsonElement element, params string[] names)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            var expected = new HashSet<string>(names, StringComparer.Ordinal);
            foreach (var property in element.EnumerateObject())
            {
                if (!expected.Remove(property.Name))
                {
                    return false;
                }
            }

            return expected.Count == 0;
        }

        public static bool IsValidCorrelationId(string correlationId)
        {
            if (correlationId == null || !correlationId.StartsWith("d8b3-", StringComparison.Ordinal))
            {
                return false;
            }

            var payload = correlationId.Substring(5);
            if (payload.Length != 43)
            {
                return false;
            }

            try
            {
                return Decode(payload).Length == 32;
            }
            catch (FormatException)
            {
                return false;
            }
        }
    }
}
