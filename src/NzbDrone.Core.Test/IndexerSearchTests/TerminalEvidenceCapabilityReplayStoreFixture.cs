using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using NUnit.Framework;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.IndexerSearch;
using NzbDrone.Core.Test.Framework;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class TerminalEvidenceCapabilityReplayStoreFixture : DbTest
    {
        private TerminalEvidenceCapabilityReplayStore _store;

        [SetUp]
        public void SetUpStore()
        {
            Mocker.SetConstant<ITerminalEvidenceSqliteConnectionFactory>(
                new TerminalEvidenceSqliteConnectionFactory(Mocker.Resolve<IConnectionStringFactory>()));
            _store = Mocker.Resolve<TerminalEvidenceCapabilityReplayStore>();
        }

        [Test]
        public void should_redeem_a_token_only_once()
        {
            var tokenId = Guid.NewGuid().ToString("D");
            var expiresAt = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
            _store.TryRedeem(tokenId, expiresAt).Should().BeTrue();
            var restartedStore = Mocker.Resolve<TerminalEvidenceCapabilityReplayStore>();
            restartedStore.TryRedeem(tokenId, expiresAt).Should().BeFalse();
        }

        [Test]
        public void should_reject_expired_and_malformed_token_ids()
        {
            _store.TryRedeem(Guid.NewGuid().ToString("D"), DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds()).Should().BeFalse();
            _store.TryRedeem("not-a-guid", DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds()).Should().BeFalse();
        }

        [Test]
        public void should_return_a_sanitized_busy_category_and_leave_capability_unconsumed_under_write_lock()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("SQLite lock contention fixture is backend-specific");
            }

            var tokenId = Guid.NewGuid().ToString("D");
            var expiresAt = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
            using var lockConnection = Db.OpenConnection();
            lockConnection.Execute("BEGIN IMMEDIATE");
            lockConnection.Execute("UPDATE \"D8TerminalCapabilityRedemptions\" SET \"ExpiresAtUnixMs\" = \"ExpiresAtUnixMs\"");

            var failure = FluentActions.Invoking(() => _store.TryRedeem(tokenId, expiresAt))
                .Should().Throw<D8StorageException>().Which;
            failure.Category.Should().Be(D8SearchJournalFailure.BusyLocked);
            failure.Message.Should().Be("D8 storage operation failed (busy-locked)");
            failure.Message.Should().NotContain("Data Source").And.NotContain("SQL").And.NotContain(tokenId);

            lockConnection.Execute("ROLLBACK");
            using var verification = Db.OpenConnection();
            verification.QuerySingle<int>(
                "SELECT COUNT(*) FROM \"D8TerminalCapabilityRedemptions\" WHERE \"TokenId\" = @TokenId", new { TokenId = tokenId })
                .Should().Be(0, "a busy failure must not consume the capability");
        }

        [Test]
        public void should_abort_replay_database_work_when_a_lock_exceeds_the_deadline()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("SQLite exclusive-lock cancellation fixture is backend-specific");
            }

            using var lockConnection = Db.OpenConnection();
            lockConnection.Execute("PRAGMA locking_mode=EXCLUSIVE");
            lockConnection.Execute("BEGIN EXCLUSIVE");

            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
            FluentActions.Invoking(() => _store.TryRedeem(
                    Guid.NewGuid().ToString("D"),
                    DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds(),
                    cancellation.Token))
                .Should().Throw<OperationCanceledException>();
        }

        [Test]
        public async Task should_abort_async_replay_database_work_when_a_lock_exceeds_deadline()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("SQLite exclusive-lock cancellation fixture is backend-specific");
            }

            using var lockConnection = Db.OpenConnection();
            lockConnection.Execute("PRAGMA locking_mode=EXCLUSIVE");
            lockConnection.Execute("BEGIN EXCLUSIVE");

            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
            var startedAt = DateTime.UtcNow;
            await FluentActions.Awaiting(() => _store.TryRedeemAsync(
                    Guid.NewGuid().ToString("D"),
                    DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds(),
                    cancellation.Token))
                .Should().ThrowAsync<OperationCanceledException>();
            (DateTime.UtcNow - startedAt).Should().BeLessThan(TimeSpan.FromSeconds(1));
        }

        [Test]
        public async Task concurrent_redemption_must_accept_at_most_one_request()
        {
            var tokenId = Guid.NewGuid().ToString("D");
            var expiresAt = DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds();
            var attempts = Enumerable.Range(0, 8)
                .Select(_ => Task.Run(() => _store.TryRedeem(tokenId, expiresAt)))
                .ToArray();

            var accepted = await Task.WhenAll(attempts);

            accepted.Count(result => result).Should().Be(1);
        }
    }
}
