using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using NUnit.Framework;
using NzbDrone.Core.IndexerSearch;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class TerminalEvidenceCapabilityFixture
    {
        private const string Issuer = "urn:prowlarr:d8-capability-issuer:test";
        private const string Audience = "prowlarr-terminal-evidence:test";
        private const string KeyId = "test-key-1";
        private const string CertificateFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        private const string CorrelationId = "d8b3-" + "AAAAAAAAAAAAAAAAAAAA" + "AAAAAAAAAAAAAAAAAAAAAAA";
        private RSA _signingKey;
        private string _publicKeyPath;

        [SetUp]
        public void SetUp()
        {
            _signingKey = RSA.Create(2048);
            _publicKeyPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, "d8-capability-" + Guid.NewGuid().ToString("N") + ".pem");
            File.WriteAllText(_publicKeyPath, _signingKey.ExportSubjectPublicKeyInfoPem());
        }

        [TearDown]
        public void TearDown()
        {
            _signingKey.Dispose();
            File.Delete(_publicKeyPath);
        }

        [Test]
        public void should_verify_operation_correlation_and_certificate_bound_short_lived_token()
        {
            var now = DateTimeOffset.UtcNow;
            var operationId = Guid.NewGuid().ToString("D");
            var token = CreateToken(operationId, CertificateFingerprint, now, TimeSpan.FromSeconds(30));

            var capability = TerminalEvidenceCapabilityVerifier.Verify(
                token,
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now);

            capability.Should().NotBeNull();
            capability.TokenId.Should().NotBeNullOrWhiteSpace();
            capability.OperationId.Should().Be(operationId);
            capability.CorrelationId.Should().Be(CorrelationId);
        }

        [Test]
        public void should_accept_only_canonical_256_bit_correlation_identifiers()
        {
            TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(CorrelationId).Should().BeTrue();

            var invalid = new[]
            {
                null,
                string.Empty,
                "d8b3-",
                "d8b3-" + new string('A', 42),
                "d8b3-" + new string('A', 44),
                "d8b3-" + new string('A', 42) + "=",
                "d8b3-" + new string('A', 42) + "B",
                "d8b3-" + new string('A', 42) + " ",
                "d8b3-0123456789abcdef0123456789abcdef"
            };

            foreach (var correlationId in invalid)
            {
                TerminalEvidenceCapabilityVerifier.IsValidCorrelationId(correlationId).Should().BeFalse(correlationId);
            }
        }

        [Test]
        public void should_reject_wrong_operation_certificate_audience_and_modified_signature()
        {
            var now = DateTimeOffset.UtcNow;
            var operationId = Guid.NewGuid().ToString("D");
            var token = CreateToken(operationId, CertificateFingerprint, now, TimeSpan.FromSeconds(30));

            TerminalEvidenceCapabilityVerifier.Verify(
                token,
                CertificateFingerprint,
                Guid.NewGuid().ToString("D"),
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            TerminalEvidenceCapabilityVerifier.Verify(
                token,
                new string('a', 64),
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            TerminalEvidenceCapabilityVerifier.Verify(
                token,
                CertificateFingerprint,
                operationId,
                Issuer,
                "wrong-audience",
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            var tampered = token.Substring(0, token.Length - 2) + (token[^2] == 'a' ? "b" : "a") + token[^1];
            TerminalEvidenceCapabilityVerifier.Verify(
                tampered,
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            TerminalEvidenceCapabilityVerifier.Verify(
                token + "=",
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
        }

        [Test]
        public void should_reject_expired_future_dated_and_overlong_tokens()
        {
            var now = DateTimeOffset.UtcNow;
            var operationId = Guid.NewGuid().ToString("D");
            var expired = CreateToken(operationId, CertificateFingerprint, now.AddMinutes(-2), TimeSpan.FromSeconds(30));
            var future = CreateToken(operationId, CertificateFingerprint, now.AddMinutes(2), TimeSpan.FromSeconds(30));
            var overlong = CreateToken(operationId, CertificateFingerprint, now, TimeSpan.FromMinutes(2));

            TerminalEvidenceCapabilityVerifier.Verify(
                expired,
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            TerminalEvidenceCapabilityVerifier.Verify(
                future,
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
            TerminalEvidenceCapabilityVerifier.Verify(
                overlong,
                CertificateFingerprint,
                operationId,
                Issuer,
                Audience,
                KeyId,
                File.ReadAllText(_publicKeyPath),
                now).Should().BeNull();
        }

        private string CreateToken(string operationId, string certificateFingerprint, DateTimeOffset issuedAt, TimeSpan lifetime)
        {
            var header = JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT", kid = KeyId });
            var payload = JsonSerializer.SerializeToUtf8Bytes(new
            {
                iss = Issuer,
                aud = Audience,
                scope = "terminal-record:read",
                operation_id = operationId,
                correlation_id = CorrelationId,
                certificate_sha256 = certificateFingerprint,
                jti = Guid.NewGuid().ToString("D"),
                iat = issuedAt.ToUnixTimeSeconds(),
                exp = issuedAt.Add(lifetime).ToUnixTimeSeconds()
            });
            var signingInput = Encode(header) + "." + Encode(payload);
            var signature = _signingKey.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            return signingInput + "." + Encode(signature);
        }

        private static string Encode(byte[] value)
        {
            return Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        }
    }
}
