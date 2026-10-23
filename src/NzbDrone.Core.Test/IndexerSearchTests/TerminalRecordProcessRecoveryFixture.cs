using System;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using NUnit.Framework;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.IndexerSearch;

namespace NzbDrone.Core.Test.IndexerSearchTests
{
    [TestFixture]
    public class TerminalRecordProcessRecoveryFixture
    {
        private const string WorkerFilter = "FullyQualifiedName~TerminalRecordProcessWorkerFixture.worker";

        [Test]
        [Explicit("Starts and terminates a child test process against disposable SQLite storage")]
        public async Task uncommitted_repository_insert_is_absent_after_process_termination()
        {
            var environment = CreateDisposableDatabase("before-commit");
            try
            {
                using var child = StartWorker(environment, "before-commit");
                await WaitForSignalAndKill(child, environment.SignalPath);

                new IndexerSearchTerminalRecordRepository(new DisposableMainDatabase(environment.DatabasePath), null)
                    .Get(environment.OperationId).Should().BeNull();
            }
            finally
            {
                Directory.Delete(environment.Root, true);
            }
        }

        [Test]
        [Explicit("Starts and terminates a child test process against disposable SQLite storage")]
        public async Task committed_repository_record_survives_process_termination_and_reopen()
        {
            var environment = CreateDisposableDatabase("after-commit");
            try
            {
                using var child = StartWorker(environment, "after-commit");
                await WaitForSignalAndKill(child, environment.SignalPath);

                var loaded = new IndexerSearchTerminalRecordRepository(new DisposableMainDatabase(environment.DatabasePath), null)
                    .Get(environment.OperationId);
                loaded.Should().NotBeNull();
                loaded.Outcome.Should().Be("SUCCEEDED");
                loaded.CommitAcknowledged.Should().BeTrue();
            }
            finally
            {
                Directory.Delete(environment.Root, true);
            }
        }

        private static DisposableEnvironment CreateDisposableDatabase(string scenario)
        {
            var root = Path.Combine(TestContext.CurrentContext.WorkDirectory, "terminal-process-recovery-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            var databasePath = Path.Combine(root, "terminal.db");
            var operationId = Guid.NewGuid().ToString("D");
            var signalPath = Path.Combine(root, "worker.signal");
            using (var connection = OpenConnection(databasePath))
            {
                connection.Execute("""
                    CREATE TABLE "IndexerSearchTerminalRecords" (
                        "OperationId" varchar(36) PRIMARY KEY,
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
                connection.Execute("CREATE INDEX \"IX_IndexerSearchTerminalRecords_EndedAtUnixMs\" ON \"IndexerSearchTerminalRecords\" (\"EndedAtUnixMs\" ASC)");
                if (scenario == "before-commit")
                {
                    connection.Execute("CREATE TRIGGER block_terminal_insert BEFORE INSERT ON \"IndexerSearchTerminalRecords\" BEGIN SELECT b3b_block_before_commit(); END");
                }
            }

            return new DisposableEnvironment(root, databasePath, signalPath, operationId);
        }

        private static Process StartWorker(DisposableEnvironment environment, string scenario)
        {
            var dotnetPath = Environment.GetEnvironmentVariable("D8_B3_DOTNET");
            if (string.IsNullOrWhiteSpace(dotnetPath) || !File.Exists(dotnetPath))
            {
                Assert.Fail("Set D8_B3_DOTNET to the already installed dotnet host to run process-recovery integration tests");
            }

            var root = FindRepositoryRoot();
            var project = Path.Combine(root, "src", "NzbDrone.Core.Test", "Prowlarr.Core.Test.csproj");
            var start = new ProcessStartInfo(dotnetPath)
            {
                WorkingDirectory = root,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            start.ArgumentList.Add("test");
            start.ArgumentList.Add(project);
            start.ArgumentList.Add("--no-build");
            start.ArgumentList.Add("--no-restore");
            start.ArgumentList.Add("--filter");
            start.ArgumentList.Add(WorkerFilter);
            start.ArgumentList.Add("--logger");
            start.ArgumentList.Add("console;verbosity=minimal");
            start.Environment["B3B_TERMINAL_WORKER_MODE"] = scenario;
            start.Environment["B3B_TERMINAL_DATABASE_PATH"] = environment.DatabasePath;
            start.Environment["B3B_TERMINAL_SIGNAL_PATH"] = environment.SignalPath;
            start.Environment["B3B_TERMINAL_OPERATION_ID"] = environment.OperationId;

            var process = Process.Start(start);
            process.Should().NotBeNull();
            _ = process.StandardOutput.ReadToEndAsync();
            _ = process.StandardError.ReadToEndAsync();
            return process;
        }

        private static async Task WaitForSignalAndKill(Process process, string signalPath)
        {
            var timeout = DateTime.UtcNow.AddSeconds(45);
            while (!File.Exists(signalPath) && !process.HasExited && DateTime.UtcNow < timeout)
            {
                await Task.Delay(50);
            }

            if (!File.Exists(signalPath))
            {
                var exit = process.HasExited ? process.ExitCode.ToString() : "still-running";
                process.Kill(true);
                await process.WaitForExitAsync();
                Assert.Fail($"Child worker failed to reach the transaction signal (exit={exit})");
            }

            process.Kill(true);
            await process.WaitForExitAsync();
        }

        private static string FindRepositoryRoot()
        {
            var current = new DirectoryInfo(TestContext.CurrentContext.TestDirectory);
            while (current != null && !File.Exists(Path.Combine(current.FullName, "src", "NzbDrone.Core.Test", "Prowlarr.Core.Test.csproj")))
            {
                current = current.Parent;
            }

            return current?.FullName ?? throw new DirectoryNotFoundException("Unable to locate the Prowlarr test project from the test directory");
        }

        private static SQLiteConnection OpenConnection(string databasePath)
        {
            var builder = new SQLiteConnectionStringBuilder
            {
                DataSource = databasePath,
                Version = 3,
                Pooling = false,
                JournalMode = SQLiteJournalModeEnum.Wal
            };
            var connection = new SQLiteConnection(builder.ConnectionString);
            connection.Open();
            connection.Execute("PRAGMA synchronous=FULL");
            return connection;
        }

        private sealed record DisposableEnvironment(string Root, string DatabasePath, string SignalPath, string OperationId);

        private sealed class DisposableMainDatabase : IMainDatabase
        {
            private readonly string _databasePath;

            public DisposableMainDatabase(string databasePath) => _databasePath = databasePath;
            public IDbConnection OpenConnection() => TerminalRecordProcessRecoveryFixture.OpenConnection(_databasePath);
            public Version Version => new Version(3, 0);
            public int Migration => 45;
            public DatabaseType DatabaseType => DatabaseType.SQLite;
            public void Vacuum()
            {
            }
        }
    }

    [TestFixture]
    public class TerminalRecordProcessWorkerFixture
    {
        [Test]
        [Explicit("Launched by TerminalRecordProcessRecoveryFixture as a subprocess")]
        public void worker()
        {
            var mode = Environment.GetEnvironmentVariable("B3B_TERMINAL_WORKER_MODE");
            if (mode == null)
            {
                Assert.Ignore("Subprocess worker only");
            }

            var databasePath = Environment.GetEnvironmentVariable("B3B_TERMINAL_DATABASE_PATH");
            var signalPath = Environment.GetEnvironmentVariable("B3B_TERMINAL_SIGNAL_PATH");
            var operationId = Environment.GetEnvironmentVariable("B3B_TERMINAL_OPERATION_ID");
            if (mode == "before-commit")
            {
                SQLiteFunction.RegisterFunction(typeof(BlockBeforeCommitFunction));
            }

            var record = CreateRecord(operationId);
            new IndexerSearchTerminalRecordRepository(new WorkerMainDatabase(databasePath), null).Save(record);
            File.WriteAllText(signalPath, mode);
            Thread.Sleep(Timeout.Infinite);
        }

        private static IndexerSearchTerminalRecord CreateRecord(string operationId)
        {
            var start = DateTimeOffset.UtcNow.AddSeconds(-1);
            return new IndexerSearchTerminalRecord
            {
                OperationId = operationId,
                SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                ApplicationVersion = "b3-process-test",
                ProcessInstanceId = Guid.NewGuid().ToString("D"),
                RequestCorrelationId = "d8b3-0123456789abcdef0123456789abcdef",
                IndexerId = 1,
                OperationType = IndexerSearchTerminalContract.OperationType,
                StartedAt = start,
                EndedAt = start.AddMilliseconds(500),
                Outcome = "SUCCEEDED",
                ResultCount = 0,
                Attempts = new[] { new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example", 200, null, start, start.AddMilliseconds(400)) }
            };
        }

        private sealed class WorkerMainDatabase : IMainDatabase
        {
            private readonly string _databasePath;

            public WorkerMainDatabase(string databasePath) => _databasePath = databasePath;
            public IDbConnection OpenConnection()
            {
                var builder = new SQLiteConnectionStringBuilder
                {
                    DataSource = _databasePath,
                    Version = 3,
                    Pooling = false,
                    JournalMode = SQLiteJournalModeEnum.Wal
                };
                var connection = new SQLiteConnection(builder.ConnectionString);
                connection.Open();
                connection.Execute("PRAGMA synchronous=FULL");
                return connection;
            }

            public Version Version => new Version(3, 0);
            public int Migration => 45;
            public DatabaseType DatabaseType => DatabaseType.SQLite;
            public void Vacuum()
            {
            }
        }

        [SQLiteFunction(Name = "b3b_block_before_commit", FuncType = FunctionType.Scalar, Arguments = 0)]
        public sealed class BlockBeforeCommitFunction : SQLiteFunction
        {
            public override object Invoke(object[] args)
            {
                File.WriteAllText(Environment.GetEnvironmentVariable("B3B_TERMINAL_SIGNAL_PATH"), "insert-started");
                Thread.Sleep(Timeout.Infinite);
                return 1;
            }
        }
    }
}
