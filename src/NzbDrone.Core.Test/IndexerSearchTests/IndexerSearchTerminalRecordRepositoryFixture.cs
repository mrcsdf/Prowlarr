using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Dapper;
using FluentAssertions;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.IndexerSearch;
using NzbDrone.Core.Test.Framework;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class IndexerSearchTerminalRecordRepositoryFixture : DbTest
    {
        private IIndexerSearchTerminalRecordRepository _repository;
        private D8SearchOperationJournalRepository _operationJournal;
        private IMainDatabase _mainDatabase;

        [SetUp]
        public void ResolveRepository()
        {
            Mocker.SetConstant<ITerminalEvidenceSqliteConnectionFactory>(
                new TerminalEvidenceSqliteConnectionFactory(Mocker.Resolve<IConnectionStringFactory>()));
            _mainDatabase = Mocker.Resolve<IMainDatabase>();
            _repository = Mocker.Resolve<IndexerSearchTerminalRecordRepository>();
            _operationJournal = new D8SearchOperationJournalRepository(_mainDatabase, _repository);
        }

        [Test]
        public void should_terminalize_a_started_operation_as_no_search_after_pre_dispatch_failure()
        {
            var operationId = Guid.NewGuid().ToString("D");
            var correlationId = "d8b3-" + new string('A', 43);
            var startedAt = DateTimeOffset.UtcNow;
            var started = _operationJournal.Start(
                "spiffe://test/d8",
                "before-dispatch",
                42,
                operationId,
                Guid.NewGuid().ToString("D"),
                correlationId,
                startedAt);

            var terminal = _operationJournal.Fail(
                operationId,
                started.Entry.CallerIdentity,
                correlationId,
                DateTimeOffset.UtcNow,
                "IndexerUnavailableException");

            started.Created.Should().BeTrue();
            terminal.Outcome.Should().Be("NO_SEARCH");
            terminal.FailureReason.Should().Be("IndexerUnavailableException");
            terminal.Attempts.Should().BeEmpty();
            _repository.Get(operationId).Outcome.Should().Be("NO_SEARCH");
        }

        [Test]
        public void should_recover_an_interrupted_durable_dispatch_as_indeterminate()
        {
            var operationId = Guid.NewGuid().ToString("D");
            var correlationId = "d8b3-" + new string('B', 43);
            var startedAt = DateTimeOffset.UtcNow;
            _operationJournal.Start(
                "spiffe://test/d8",
                "during-dispatch",
                42,
                operationId,
                Guid.NewGuid().ToString("D"),
                correlationId,
                startedAt);
            var attempt = new ObservedAttempt(
                Guid.NewGuid(),
                "GET",
                "indexer.example.net",
                null,
                null,
                startedAt,
                null);
            _operationJournal.AttemptStarted(operationId, attempt);

            _operationJournal.RecoverInterrupted();

            using (var connection = _mainDatabase.OpenConnection())
            {
                connection.ExecuteScalar<int>(
                    "SELECT count(*) FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @OperationId",
                    new { OperationId = operationId }).Should().Be(1);
            }

            var terminal = _repository.Get(operationId);
            terminal.Outcome.Should().Be("INDETERMINATE");
            terminal.FailureReason.Should().Be("process_interrupted");
            terminal.Attempts.Should().ContainSingle().Which.ExceptionType.Should().Be("ProcessInterrupted");
            terminal.Attempts[0].CompletedAt.Should().NotBeNull();
        }

        [Test]
        public void should_complete_zero_result_search_from_durable_dispatch_and_round_trip_timestamps()
        {
            var operationId = Guid.NewGuid().ToString("D");
            var correlationId = "d8b3-" + new string('C', 43);
            var startedAt = DateTimeOffset.UtcNow;
            var operation = _operationJournal.Start(
                "spiffe://test/d8",
                "zero-results",
                42,
                operationId,
                Guid.NewGuid().ToString("D"),
                correlationId,
                startedAt).Entry;
            var attemptStartedAt = startedAt.AddMilliseconds(1).AddTicks(1234);
            var attempt = new ObservedAttempt(
                Guid.NewGuid(),
                "GET",
                "indexer.example.net",
                null,
                null,
                attemptStartedAt,
                null);
            _operationJournal.AttemptStarted(operationId, attempt);
            var attemptFinished = attempt with { StatusCode = 200, CompletedAt = attemptStartedAt.AddMilliseconds(2).AddTicks(5678) };
            _operationJournal.AttemptFinished(operationId, attemptFinished);

            var terminal = _operationJournal.Complete(operationId, new IndexerSearchTerminalRecord
            {
                OperationId = operationId,
                SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                ApplicationVersion = "3.0.0-test",
                ProcessInstanceId = operation.ProcessInstanceId,
                RequestCorrelationId = correlationId,
                IndexerId = 42,
                OperationType = IndexerSearchTerminalContract.OperationType,
                StartedAt = operation.StartedAt,
                EndedAt = attemptFinished.CompletedAt.Value.AddTicks(1000),
                Outcome = "SUCCEEDED",
                ResultCount = 0,
                Attempts = Array.Empty<ObservedAttempt>()
            });

            terminal.Outcome.Should().Be("SUCCEEDED");
            terminal.ResultCount.Should().Be(0);
            terminal.Attempts.Should().ContainSingle();
            var loaded = _repository.Get(operationId);
            loaded.Should().NotBeNull();
            loaded.Outcome.Should().Be("SUCCEEDED");
            loaded.ResultCount.Should().Be(0);
            loaded.Attempts.Should().ContainSingle();
        }

        [Test]
        public async System.Threading.Tasks.Task concurrent_idempotency_retries_create_only_one_durable_operation()
        {
            const string caller = "spiffe://test/d8";
            const string idempotencyKey = "concurrent-start";
            var correlationIds = new[] { "d8b3-" + new string('D', 43), "d8b3-" + new string('E', 43) };
            var starts = await System.Threading.Tasks.Task.WhenAll(correlationIds.Select(correlationId =>
                System.Threading.Tasks.Task.Run(() => _operationJournal.Start(
                    caller,
                    idempotencyKey,
                    42,
                    Guid.NewGuid().ToString("D"),
                    Guid.NewGuid().ToString("D"),
                    correlationId,
                    DateTimeOffset.UtcNow))));

            starts.Count(result => result.Created).Should().Be(1);
            starts.Select(result => result.Entry.OperationId).Distinct().Should().ContainSingle();
            starts.Select(result => result.Entry.CorrelationId).Distinct().Should().ContainSingle();
            using var connection = _mainDatabase.OpenConnection();
            connection.ExecuteScalar<int>(
                "SELECT count(*) FROM \"D8SearchOperations\" WHERE \"CallerIdentity\" = @Caller AND \"IdempotencyKey\" = @Key",
                new { Caller = caller, Key = idempotencyKey }).Should().Be(1);
        }

        [Test]
        public void idempotency_key_is_required_and_bound_to_the_indexer()
        {
            var operationId = Guid.NewGuid().ToString("D");
            var correlationId = "d8b3-" + new string('F', 43);
            var caller = "spiffe://test/d8";
            var startedAt = DateTimeOffset.UtcNow;

            FluentActions.Invoking(() => _operationJournal.Start(
                    caller,
                    null,
                    42,
                    operationId,
                    Guid.NewGuid().ToString("D"),
                    correlationId,
                    startedAt))
                .Should().Throw<InvalidOperationException>();

            _operationJournal.Start(
                caller,
                "bound-indexer",
                42,
                operationId,
                Guid.NewGuid().ToString("D"),
                correlationId,
                startedAt);

            FluentActions.Invoking(() => _operationJournal.Start(
                    caller,
                    "bound-indexer",
                    43,
                    Guid.NewGuid().ToString("D"),
                    Guid.NewGuid().ToString("D"),
                    correlationId,
                    startedAt))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("D8 idempotency key is already bound to a different indexer");
        }

        [Test]
        public void should_commit_and_retrieve_one_bounded_aggregate_record()
        {
            var record = CreateRecord();

            var saved = _repository.Save(record);
            var loaded = _repository.Get(record.OperationId);

            saved.CommitAcknowledged.Should().BeTrue();
            loaded.Should().NotBeNull();
            loaded.OperationId.Should().Be(record.OperationId);
            loaded.Outcome.Should().Be("SUCCEEDED");
            loaded.Attempts.Should().ContainSingle().Which.Id.Should().Be(record.Attempts[0].Id);
            loaded.RequestCorrelationId.Should().Be(record.RequestCorrelationId);
        }

        [Test]
        public void should_honor_cancellation_before_database_access()
        {
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            FluentActions.Invoking(() => _repository.Get(Guid.NewGuid().ToString("D"), cancellation.Token))
                .Should().Throw<OperationCanceledException>();
        }

        [Test]
        public async System.Threading.Tasks.Task should_honor_cancellation_before_async_database_access()
        {
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await FluentActions.Awaiting(() => _repository.GetAsync(Guid.NewGuid().ToString("D"), cancellation.Token))
                .Should().ThrowAsync<OperationCanceledException>();
        }

        [Test]
        public void should_abort_when_an_exclusive_database_lock_exceeds_the_request_deadline()
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
            FluentActions.Invoking(() => _repository.Get(Guid.NewGuid().ToString("D"), cancellation.Token))
                .Should().Throw<OperationCanceledException>();
            (DateTime.UtcNow - startedAt).Should().BeLessThan(TimeSpan.FromSeconds(1));
        }

        [Test]
        public async System.Threading.Tasks.Task should_abort_async_lookup_when_database_lock_exceeds_deadline()
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
            await FluentActions.Awaiting(() => _repository.GetAsync(Guid.NewGuid().ToString("D"), cancellation.Token))
                .Should().ThrowAsync<OperationCanceledException>();
            (DateTime.UtcNow - startedAt).Should().BeLessThan(TimeSpan.FromSeconds(1));
        }

        [Test]
        public void should_not_serialize_commit_acknowledgment_as_terminal_source_evidence()
        {
            var record = _repository.Save(CreateRecord());
            var serialized = JObject.Parse(JsonConvert.SerializeObject(record));

            serialized.ContainsKey(nameof(IndexerSearchTerminalRecord.CommitAcknowledged)).Should().BeFalse();
        }

        [Test]
        public void should_reject_duplicate_operation_id_without_replacing_committed_record()
        {
            var record = CreateRecord();
            _repository.Save(record);
            var duplicate = CreateRecord(record.OperationId);
            duplicate.Outcome = "FAILED";
            duplicate.FailureReason = "duplicate_test";

            FluentActions.Invoking(() => _repository.Save(duplicate)).Should().Throw<Exception>();
            _repository.Get(record.OperationId).Outcome.Should().Be("SUCCEEDED");
        }

        [Test]
        public async System.Threading.Tasks.Task concurrent_duplicate_operations_can_commit_at_most_once()
        {
            var operationId = Guid.NewGuid().ToString("D");
            var records = new[] { CreateRecord(operationId), CreateRecord(operationId) };
            var tasks = new System.Threading.Tasks.Task<bool>[records.Length];
            for (var i = 0; i < records.Length; i++)
            {
                var record = records[i];
                tasks[i] = System.Threading.Tasks.Task.Run(() =>
                {
                    try
                    {
                        _repository.Save(record);
                        return true;
                    }
                    catch (Exception)
                    {
                        return false;
                    }
                });
            }

            var outcomes = await System.Threading.Tasks.Task.WhenAll(tasks);

            var committed = 0;
            foreach (var outcome in outcomes)
            {
                if (outcome)
                {
                    committed++;
                }
            }

            committed.Should().Be(1);
            _repository.Get(operationId).Should().NotBeNull();
        }

        [Test]
        public void should_not_expose_a_record_when_database_insert_fails()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("SQLite trigger failure fixture is backend-specific");
            }

            using (var connection = Db.OpenConnection())
            {
                connection.Execute("CREATE TRIGGER fail_terminal_insert BEFORE INSERT ON \"IndexerSearchTerminalRecords\" BEGIN SELECT RAISE(ABORT, 'injected write failure'); END");
            }

            var record = CreateRecord();
            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<Exception>();
            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void commit_constraint_failure_cannot_acknowledge_or_expose_successful_record()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("Deferred foreign-key commit fixture requires SQLite");
            }

            using (var connection = Db.OpenConnection())
            {
                connection.Execute("PRAGMA foreign_keys=ON");
                connection.Execute("CREATE TABLE \"TerminalCommitParent\" (\"Id\" INTEGER PRIMARY KEY)");
                connection.Execute("CREATE TABLE \"TerminalCommitChild\" (\"ParentId\" INTEGER NOT NULL REFERENCES \"TerminalCommitParent\" (\"Id\") DEFERRABLE INITIALLY DEFERRED)");
                connection.Execute("CREATE TRIGGER fail_terminal_commit AFTER INSERT ON \"IndexerSearchTerminalRecords\" BEGIN INSERT INTO \"TerminalCommitChild\" (\"ParentId\") VALUES (999); END");
            }

            var record = CreateRecord();
            var commitFailingDatabase = new CommitFailingMainDatabase(Db);
            var repository = new IndexerSearchTerminalRecordRepository(commitFailingDatabase, null);

            FluentActions.Invoking(() => repository.Save(record)).Should().Throw<Exception>();

            record.CommitAcknowledged.Should().BeFalse();
            repository.Get(record.OperationId).Should().BeNull();
        }

        [TestCase(65)]
        public void should_reject_missing_or_excessive_attempt_inventory(int attempts)
        {
            var record = CreateRecord();
            var attemptList = new List<ObservedAttempt>();
            for (var i = 0; i < attempts; i++)
            {
                var start = DateTimeOffset.UtcNow.AddSeconds(-1);
                attemptList.Add(new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example", 200, null, start, start.AddMilliseconds(5)));
            }

            record.Attempts = attemptList;

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_allow_persisted_failure_when_no_dispatch_started_but_never_success()
        {
            var failed = CreateRecord();
            failed.Outcome = "FAILED";
            failed.FailureReason = "exception_before_dispatch";
            failed.Attempts = new List<ObservedAttempt>();

            _repository.Save(failed);
            _repository.Get(failed.OperationId).Attempts.Should().BeEmpty();

            var success = CreateRecord();
            success.Attempts = new List<ObservedAttempt>();
            FluentActions.Invoking(() => _repository.Save(success)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_reject_successful_attempt_with_null_status_and_no_exception()
        {
            var record = CreateRecord();
            record.Attempts = new[] { record.Attempts[0] with { StatusCode = null, ExceptionType = null } };

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_preserve_failed_transport_attempt_without_http_status()
        {
            var record = CreateRecord();
            record.Outcome = "FAILED";
            record.FailureReason = "transport_exception";
            record.Attempts = new[] { record.Attempts[0] with { StatusCode = null, ExceptionType = "HttpRequestException" } };

            _repository.Save(record);
            var loaded = _repository.Get(record.OperationId);

            loaded.Outcome.Should().Be("FAILED");
            loaded.Attempts.Should().ContainSingle().Which.StatusCode.Should().BeNull();
            loaded.Attempts.Should().ContainSingle().Which.ExceptionType.Should().Be("HttpRequestException");
        }

        [Test]
        public void should_reject_failure_reason_that_is_not_a_safe_code()
        {
            var record = CreateRecord();
            record.Outcome = "FAILED";
            record.FailureReason = null;

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();

            record.OperationId = Guid.NewGuid().ToString("D");
            record.FailureReason = "https://indexer.example/?apikey=secret";

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_reject_non_opaque_or_secret_bearing_correlation_token()
        {
            var record = CreateRecord();
            record.RequestCorrelationId = "query?apikey=secret";

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_reject_attempt_host_containing_url_or_query_data()
        {
            var record = CreateRecord();
            record.Attempts = new[] { record.Attempts[0] with { Host = "indexer.example/?apikey=secret" } };

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_not_return_manually_corrupted_successful_row_with_null_status()
        {
            var record = CreateRecord();
            _repository.Save(record);
            record.Attempts = new[] { record.Attempts[0] with { StatusCode = null, ExceptionType = null } };
            using (var connection = Db.OpenConnection())
            {
                connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"AttemptsJson\" = @Attempts WHERE \"OperationId\" = @Id",
                    new { Attempts = JsonConvert.SerializeObject(record.Attempts), Id = record.OperationId });
            }

            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void should_not_materialize_manually_corrupted_oversized_attempt_inventory()
        {
            var record = CreateRecord();
            _repository.Save(record);
            using (var connection = Db.OpenConnection())
            {
                connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"AttemptsJson\" = @Attempts WHERE \"OperationId\" = @Id",
                    new { Attempts = new string('x', IndexerSearchTerminalContract.MaximumRecordBytes + 1), Id = record.OperationId });
            }

            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void should_not_materialize_manually_corrupted_oversized_text_column()
        {
            var record = CreateRecord();
            _repository.Save(record);
            using (var connection = Db.OpenConnection())
            {
                connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"ApplicationVersion\" = @Version WHERE \"OperationId\" = @Id",
                    new { Version = new string('v', IndexerSearchTerminalContract.MaximumRecordBytes + 1), Id = record.OperationId });
            }

            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void should_not_return_manually_corrupted_secret_bearing_correlation_token()
        {
            var record = CreateRecord();
            _repository.Save(record);
            using (var connection = Db.OpenConnection())
            {
                connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"RequestCorrelationId\" = @Token WHERE \"OperationId\" = @Id",
                    new { Token = "query?apikey=secret", Id = record.OperationId });
            }

            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void should_reject_sqlite_connection_not_using_full_synchronous_mode()
        {
            var database = new Mock<IMainDatabase>();
            database.SetupGet(d => d.DatabaseType).Returns(DatabaseType.SQLite);
            database.Setup(d => d.OpenConnection()).Returns(() =>
            {
                var connection = Db.OpenConnection();
                connection.Execute("PRAGMA synchronous=1");
                return connection;
            });
            var repository = new IndexerSearchTerminalRecordRepository(database.Object, null);

            FluentActions.Invoking(() => repository.Save(CreateRecord())).Should().Throw<NotSupportedException>()
                .WithMessage("Terminal evidence requires the active SQLite connection to use journal_mode=WAL and synchronous=FULL");
        }

        [Test]
        public void should_refuse_unverified_postgresql_evidence_backend()
        {
            var database = new Mock<IMainDatabase>();
            database.SetupGet(d => d.DatabaseType).Returns(DatabaseType.PostgreSQL);
            var repository = new IndexerSearchTerminalRecordRepository(database.Object, null);

            FluentActions.Invoking(() => repository.Save(CreateRecord())).Should().Throw<NotSupportedException>()
                .WithMessage("Terminal evidence is eligible only on verified SQLite WAL/FULL storage; PostgreSQL is not enabled for this contract");
        }

        [Test]
        public void should_reject_attempt_fields_exceeding_the_contract_bounds()
        {
            var record = CreateRecord();
            record.Attempts = new List<ObservedAttempt>
            {
                new(Guid.NewGuid(), "GET", "indexer.example", 200, new string('x', 129), DateTimeOffset.UtcNow.AddSeconds(-1), DateTimeOffset.UtcNow)
            };
            record.RequestCorrelationId = new string('c', 64);
            record.FailureReason = new string('r', 128);
            record.Outcome = "FAILED";

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>();
        }

        [Test]
        public void should_reject_a_serialized_record_over_64_kib()
        {
            var record = CreateRecord();
            var started = record.StartedAt.AddMilliseconds(1);
            var longHost = string.Join(".", new string('a', 63), new string('b', 63), new string('c', 63), new string('d', 61));
            var attempts = new List<ObservedAttempt>();
            for (var i = 0; i < IndexerSearchTerminalContract.MaximumAttempts; i++)
            {
                attempts.Add(new ObservedAttempt(Guid.NewGuid(), "GET", longHost, null, new string('\u0001', 128), started, started.AddMilliseconds(1)));
            }

            record.Outcome = "FAILED";
            record.FailureReason = "bounded_test_failure";
            record.Attempts = attempts;

            FluentActions.Invoking(() => _repository.Save(record)).Should().Throw<InvalidOperationException>()
                .WithMessage("Terminal evidence exceeds the bounded record size");
        }

        [Test]
        public void should_bound_lookup_to_exact_operation_id()
        {
            _repository.Get("not-an-operation-id").Should().BeNull();
            _repository.Get(Guid.NewGuid().ToString("D")).Should().BeNull();
        }

        [Test]
        public void should_not_return_expired_terminal_records()
        {
            var record = CreateRecord();
            _repository.Save(record);
            using (var connection = Db.OpenConnection())
            {
                connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"EndedAtUnixMs\" = @Expired WHERE \"OperationId\" = @Id",
                    new { Expired = DateTimeOffset.UtcNow.AddDays(-IndexerSearchTerminalContract.RetentionDays - 1).ToUnixTimeMilliseconds(), Id = record.OperationId });
            }

            _repository.Get(record.OperationId).Should().BeNull();
        }

        [Test]
        public void should_report_effective_sqlite_journal_and_synchronous_settings()
        {
            if (Db.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("SQLite PRAGMA inspection is backend-specific");
            }

            using var connection = Mocker.Resolve<ITerminalEvidenceSqliteConnectionFactory>().OpenConnection(CancellationToken.None);
            var journalMode = connection.ExecuteScalar<string>("PRAGMA journal_mode");
            var synchronous = connection.ExecuteScalar<int>("PRAGMA synchronous");
            var busyTimeout = connection.ExecuteScalar<int>("PRAGMA busy_timeout");
            TestContext.WriteLine($"journal_mode={journalMode}; synchronous={synchronous}; busy_timeout={busyTimeout}");
            journalMode.Should().Be("wal");
            synchronous.Should().Be(2);
            busyTimeout.Should().Be(1000);
        }

        private static IndexerSearchTerminalRecord CreateRecord(string operationId = null)
        {
            var started = DateTimeOffset.UtcNow.AddSeconds(-1);
            return new IndexerSearchTerminalRecord
            {
                OperationId = operationId ?? Guid.NewGuid().ToString("D"),
                SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                ApplicationVersion = "2.3.0.5236-b3-test",
                ProcessInstanceId = Guid.NewGuid().ToString("D"),
                RequestCorrelationId = "d8b3-0123456789abcdef0123456789abcdef",
                IndexerId = 42,
                OperationType = IndexerSearchTerminalContract.OperationType,
                StartedAt = started,
                EndedAt = started.AddMilliseconds(500),
                Outcome = "SUCCEEDED",
                ResultCount = 0,
                Attempts = new List<ObservedAttempt>
                {
                    new(Guid.NewGuid(), "GET", "indexer.example", 200, null, started, started.AddMilliseconds(400))
                }
            };
        }

        private sealed class CommitFailingMainDatabase : IMainDatabase
        {
            private readonly ITestDatabase _database;

            public CommitFailingMainDatabase(ITestDatabase database) => _database = database;

            public System.Data.IDbConnection OpenConnection()
            {
                var connection = _database.OpenConnection();
                connection.Execute("PRAGMA foreign_keys=ON");
                return connection;
            }

            public Version Version => new Version(3, 0);
            public int Migration => 45;
            public DatabaseType DatabaseType => _database.DatabaseType;
            public void Vacuum()
            {
            }
        }
    }
}
