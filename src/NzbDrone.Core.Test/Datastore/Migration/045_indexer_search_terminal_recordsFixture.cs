using System;
using Dapper;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.Datastore.Migration.Framework;
using NzbDrone.Core.IndexerSearch;
using NzbDrone.Core.Test.Framework;

namespace NzbDrone.Core.Test.Datastore.Migration
{
    [TestFixture]
    public class indexer_search_terminal_recordsFixture : DbTest
    {
        [SetUp]
        public override void SetupDb()
        {
            SetupContainer();
            Mocker.SetConstant<ITerminalEvidenceSqliteConnectionFactory>(
                new TerminalEvidenceSqliteConnectionFactory(Mocker.Resolve<IConnectionStringFactory>()));
        }

        [Test]
        public void should_apply_upstream_044_then_create_terminal_table_on_a_clean_database()
        {
            var db = CreateDatabaseAt43();
            AssertSqliteOnly();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);

            Migrate();
            Migrate();

            using var connection = db.OpenConnection();
            ReadWedgeSetting(connection).Should().Be(true);
            connection.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(45);
            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='table' AND \"name\"='IndexerSearchTerminalRecords'").Should().Be(1);
            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='index' AND \"name\"='IX_IndexerSearchTerminalRecords_EndedAtUnixMs'").Should().Be(1);
        }

        [Test]
        public void should_upgrade_genuine_upstream_044_history_without_a_prototype_table()
        {
            var db = CreateDatabaseAt43();
            InsertMyAnonamouseSetting(db, "{\"useFreeleechWedge\":true}");
            RecordMigration44(db);

            Migrate();

            using var connection = db.OpenConnection();
            ReadWedgeSetting(connection).Should().Be(true);
            connection.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(45);
            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='table' AND \"name\"='IndexerSearchTerminalRecords'").Should().Be(1);
        }

        [Test]
        public void should_preserve_prototype_044_records_and_run_skipped_upstream_conversion()
        {
            var db = CreateDatabaseAt43();
            AssertSqliteOnly();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);
            CreatePrototype044AndRecordVersion(db);

            Migrate();

            using var connection = db.OpenConnection();
            ReadWedgeSetting(connection).Should().Be(true);
            connection.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(45);
            Mocker.Resolve<IndexerSearchTerminalRecordRepository>().Get(PrototypeOperationId).Outcome.Should().Be("SUCCEEDED");
        }

        [Test]
        public void should_apply_upstream_conversion_when_prototype_044_was_recorded_but_terminal_table_is_missing()
        {
            var db = CreateDatabaseAt43();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);
            RecordMigration44(db);

            Migrate();

            using var connection = db.OpenConnection();
            ReadWedgeSetting(connection).Should().Be(true);
            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='table' AND \"name\"='IndexerSearchTerminalRecords'").Should().Be(1);
        }

        [Test]
        public void should_repair_missing_expiration_index_without_losing_existing_records()
        {
            var db = CreateDatabaseAt43();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);
            CreatePrototype044AndRecordVersion(db, false);

            Migrate();

            using var connection = db.OpenConnection();
            Mocker.Resolve<IndexerSearchTerminalRecordRepository>().Get(PrototypeOperationId).Outcome.Should().Be("SUCCEEDED");
            connection.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='index' AND \"name\"='IX_IndexerSearchTerminalRecords_EndedAtUnixMs'").Should().Be(1);
            ReadWedgeSetting(connection).Should().Be(true);
        }

        [Test]
        public void should_reject_same_named_expiration_index_on_wrong_column_without_changing_data()
        {
            var db = CreateDatabaseAt43();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);
            CreatePrototype044AndRecordVersion(db, true, "Outcome");

            FluentActions.Invoking(Migrate).Should().Throw<Exception>();

            using var verify = db.OpenConnection();
            verify.ExecuteScalar<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\"='00000000-0000-4000-8000-000000000045'").Should().Be("SUCCEEDED");
            JObject.Parse(verify.ExecuteScalar<string>("SELECT \"Settings\" FROM \"Indexers\" WHERE \"Id\"=1"))
                .Value<JToken>("useFreeleechWedge").Type.Should().Be(JTokenType.Integer);
            verify.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(44);
        }

        [Test]
        public void should_reject_incompatible_existing_terminal_table_without_changing_application_data()
        {
            var db = CreateDatabaseAt43();
            InsertMyAnonamouseWithLegacyIntegerSetting(db);
            using (var connection = db.OpenConnection())
            {
                connection.Execute("CREATE TABLE \"IndexerSearchTerminalRecords\" (\"OperationId\" varchar(36) PRIMARY KEY)");
            }

            RecordMigration44(db);

            FluentActions.Invoking(Migrate).Should().Throw<Exception>();

            using var verify = db.OpenConnection();
            JObject.Parse(verify.ExecuteScalar<string>("SELECT \"Settings\" FROM \"Indexers\" WHERE \"Id\"=1"))
                .Value<JToken>("useFreeleechWedge").Type.Should().Be(JTokenType.Integer);
            verify.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(44);
            verify.ExecuteScalar<int>("SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\"='table' AND \"name\"='IndexerSearchTerminalRecords'").Should().Be(1);
        }

        [Test]
        public void should_preserve_additive_schema_and_terminal_data_when_rolling_back_to_version_044()
        {
            var db = CreateDatabaseAt43();
            AssertSqliteOnly();
            CreatePrototype044AndRecordVersion(db);
            Migrate();

            Mocker.Resolve<IMigrationController>().Migrate(
                Mocker.Resolve<IConnectionStringFactory>().MainDbConnection.ConnectionString,
                new MigrationContext(MigrationType.Main, 44),
                DatabaseType.SQLite);

            using var connection = db.OpenConnection();
            connection.ExecuteScalar<long>("SELECT MAX(\"Version\") FROM \"VersionInfo\"").Should().Be(45);
            Mocker.Resolve<IndexerSearchTerminalRecordRepository>().Get(PrototypeOperationId).Outcome.Should().Be("SUCCEEDED");
        }

        private ITestDatabase CreateDatabaseAt43()
        {
            if (Mocker.Resolve<IConnectionStringFactory>().MainDbConnection.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("Migration compatibility fixture requires a disposable SQLite database");
            }

            var context = new MigrationContext(MigrationType.Main, 43)
            {
                // A BeforeMigration callback forces DbTest to create an isolated
                // database rather than reuse the cached fully-migrated fixture DB.
                BeforeMigration = _ => { }
            };

            return WithTestDb(context);
        }

        private void AssertSqliteOnly()
        {
            if (Mocker.Resolve<IConnectionStringFactory>().MainDbConnection.DatabaseType != DatabaseType.SQLite)
            {
                Assert.Ignore("Migration compatibility fixture requires a disposable SQLite database");
            }
        }

        private void Migrate()
        {
            Mocker.Resolve<IMigrationController>().Migrate(
                Mocker.Resolve<IConnectionStringFactory>().MainDbConnection.ConnectionString,
                new MigrationContext(MigrationType.Main, 45),
                DatabaseType.SQLite);
        }

        private static void InsertMyAnonamouseWithLegacyIntegerSetting(ITestDatabase db)
        {
            InsertMyAnonamouseSetting(db, "{\"useFreeleechWedge\":2}");
        }

        private static void InsertMyAnonamouseSetting(ITestDatabase db, string settings)
        {
            using var connection = db.OpenConnection();
            connection.Execute("""
                INSERT INTO "Indexers" ("Id", "Name", "Implementation", "Settings", "ConfigContract", "Enable", "Priority", "Added", "Redirect", "AppProfileId", "Tags", "DownloadClientId")
                VALUES (1, 'Test MAM', 'MyAnonamouse', @Settings, 'MyAnonamouseSettings', 1, 1, @Added, 0, 1, '[]', 0)
                """,
                new { Settings = settings, Added = DateTime.UtcNow });
        }

        private static bool? ReadWedgeSetting(System.Data.IDbConnection connection)
        {
            var settings = JObject.Parse(connection.ExecuteScalar<string>("SELECT \"Settings\" FROM \"Indexers\" WHERE \"Id\"=1"));
            return settings.Value<bool?>("useFreeleechWedge");
        }

        private static void CreatePrototype044AndRecordVersion(ITestDatabase db, bool createExpirationIndex = true, string indexColumn = "EndedAtUnixMs")
        {
            var started = DateTimeOffset.UtcNow.AddSeconds(-2);
            var attempt = new ObservedAttempt(Guid.Parse("00000000-0000-4000-8000-000000000046"), "GET", "indexer.example", 200, null, started, started.AddMilliseconds(500));
            var attemptsJson = JsonConvert.SerializeObject(new[] { attempt });
            using var connection = db.OpenConnection();
            connection.Execute("""
                CREATE TABLE "IndexerSearchTerminalRecords" (
                    "OperationId" varchar(36) NOT NULL PRIMARY KEY,
                    "SchemaVersion" varchar(64) NOT NULL,
                    "ApplicationVersion" varchar(64) NOT NULL,
                    "ProcessInstanceId" varchar(36) NOT NULL,
                    "RequestCorrelationId" varchar(64) NOT NULL,
                    "IndexerId" integer NOT NULL,
                    "OperationType" varchar(64) NOT NULL,
                    "StartedAtUnixMs" bigint NOT NULL,
                    "EndedAtUnixMs" bigint NOT NULL,
                    "Outcome" varchar(16) NOT NULL,
                    "FailureReason" varchar(128) NULL,
                    "ResultCount" integer NOT NULL,
                    "AttemptsJson" varchar(65536) NOT NULL
                )
                """);
            if (createExpirationIndex)
            {
                connection.Execute($"CREATE INDEX \"IX_IndexerSearchTerminalRecords_EndedAtUnixMs\" ON \"IndexerSearchTerminalRecords\" (\"{indexColumn}\" ASC)");
            }

            connection.Execute("""
                INSERT INTO "IndexerSearchTerminalRecords"
                VALUES (@OperationId, 'prowlarr-indexer-search-terminal/v1', 'prototype', '00000000-0000-4000-8000-000000000001', 'd8b3-0123456789abcdef0123456789abcdef', 1, 'newznab-basic-search', @Started, @Ended, 'SUCCEEDED', NULL, 0, @Attempts)
                """,
                new
            {
                OperationId = PrototypeOperationId,
                Started = started.ToUnixTimeMilliseconds(),
                Ended = started.AddSeconds(1).ToUnixTimeMilliseconds(),
                Attempts = attemptsJson
            });
            RecordMigration44(connection);
        }

        private const string PrototypeOperationId = "00000000-0000-4000-8000-000000000045";

        private static void RecordMigration44(ITestDatabase db)
        {
            using var connection = db.OpenConnection();
            RecordMigration44(connection);
        }

        private static void RecordMigration44(System.Data.IDbConnection connection)
        {
            connection.Execute("INSERT INTO \"VersionInfo\" (\"Version\") VALUES (44)");
        }
    }
}
