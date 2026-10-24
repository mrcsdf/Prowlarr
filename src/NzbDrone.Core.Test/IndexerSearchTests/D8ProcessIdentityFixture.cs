using System;
using System.IO;
using FluentAssertions;
using NUnit.Framework;
using NzbDrone.Core.IndexerSearch;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class D8ProcessIdentityFixture
    {
        private const string BootId = "0f8fad5b-d9cb-469f-a165-70867728950e";

        [Test]
        public void should_match_the_fss_v3_process_identity_vector()
        {
            D8ProcessIdentity.Derive(BootId, Stat(1234567), Status(1)).Should().Be("be45b835-7472-8817-a20e-9092b92a17fd");
        }

        [Test]
        public void should_change_identity_when_start_ticks_change_to_prevent_pid_reuse()
        {
            D8ProcessIdentity.Derive(BootId, Stat(1234567), Status(1)).Should().NotBe(D8ProcessIdentity.Derive(BootId, Stat(1234568), Status(1)));
        }

        [Test]
        public void should_change_identity_when_boot_identity_or_namespace_pid_changes()
        {
            var identity = D8ProcessIdentity.Derive(BootId, Stat(1234567), Status(1));
            identity.Should().NotBe(D8ProcessIdentity.Derive("0f8fad5b-d9cb-469f-a165-70867728950f", Stat(1234567), Status(1)));
            identity.Should().NotBe(D8ProcessIdentity.Derive(BootId, Stat(1234567), Status(2)));
        }

        [TestCase("not-a-uuid", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0F8FAD5B-D9CB-469F-A165-70867728950E", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "malformed", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\nNSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 01234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 -1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 9223372036854775808", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "x (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "Name:\tprowlarr\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t0\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t01\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\tx\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t4194305\n")]
        [TestCase("{0f8fad5b-d9cb-469f-a165-70867728950e}", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5bd9cb469fa16570867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase(null, "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", null, "NSpid:\t10\t1\n")]
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", "1 (prowlarr) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1234567", null)]
        public void should_reject_missing_or_malformed_runtime_metadata(string bootId, string stat, string status)
        {
            Assert.Throws<InvalidOperationException>(() => D8ProcessIdentity.Derive(bootId, stat, status));
        }

        [Test]
        public void should_parse_start_ticks_after_a_parenthesized_command_name()
        {
            D8ProcessIdentity.Derive(BootId, Stat(1234567, "prowlarr worker) name"), Status(1)).Should().Be("be45b835-7472-8817-a20e-9092b92a17fd");
        }

        [Test]
        public void should_use_the_only_namespace_pid_when_process_is_in_the_native_pid_namespace()
        {
            D8ProcessIdentity.Derive(BootId, Stat(1234567), "NSpid:\t1\n").Should().Be("be45b835-7472-8817-a20e-9092b92a17fd");
        }

        // Vectors produced by fedora_setup_tools.d8f_provenance.derive_process_instance (FSS PR #1779 head).
        [TestCase("0f8fad5b-d9cb-469f-a165-70867728950e", 1234567L, "NSpid:\t1\n", "be45b835-7472-8817-a20e-9092b92a17fd")]
        [TestCase("6b2c3f5e-8a1d-4e9b-b7c2-1d3e5f708192", 987654321L, "NSpid:\t88123\t4242\n", "c06e64f6-81e9-879f-8166-2268ffa5db07")]
        [TestCase("6b2c3f5e-8a1d-4e9b-b7c2-1d3e5f708192", 987654321L, "NSpid:\t88123\t5000\t4242\n", "c06e64f6-81e9-879f-8166-2268ffa5db07")]
        [TestCase("6b2c3f5e-8a1d-4e9b-b7c2-1d3e5f708192", 9223372036854775807L, "NSpid:\t4194304\n", "6e7a8ac6-b1b1-8402-bdd4-c88030eabcf3")]
        public void should_match_independent_fss_derivation_vectors(string bootId, long ticks, string status, string expected)
        {
            D8ProcessIdentity.Derive(bootId + "\n", Stat(ticks), "Name:\tprowlarr\n" + status).Should().Be(expected);
        }

        [Test]
        public void should_encode_rfc9562_version_8_and_variant_bits()
        {
            var identity = D8ProcessIdentity.Derive(BootId, Stat(1234567), Status(1));
            identity.Should().MatchRegex("^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
        }

        [Test]
        [Platform("Linux")]
        public void should_derive_current_identity_from_this_process_procfs()
        {
            var identity = D8ProcessIdentity.ReadCurrent();
            var expected = D8ProcessIdentity.Derive(
                File.ReadAllText("/proc/sys/kernel/random/boot_id"),
                File.ReadAllText("/proc/self/stat"),
                File.ReadAllText("/proc/self/status"));

            identity.Should().Be(expected);
            identity.Should().Be(D8ProcessIdentity.ReadCurrent());
            Guid.ParseExact(identity, "D").ToString("D").Should().Be(identity);
        }

        private static string Stat(long ticks, string name = "prowlarr") => $"1 ({name}) S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {ticks}";
        private static string Status(int pid) => $"Name:\tprowlarr\nNSpid:\t1000\t{pid}\n";
    }
}
