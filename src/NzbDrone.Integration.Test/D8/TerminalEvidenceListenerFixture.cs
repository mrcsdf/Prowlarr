using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Moq;
using NUnit.Framework;
using NzbDrone.Common.Cache;
using NzbDrone.Common.Http;
using NzbDrone.Common.Http.Dispatchers;
using NzbDrone.Common.Http.Proxy;
using NzbDrone.Common.TPL;
using NzbDrone.Core.Datastore;
using NzbDrone.Core.IndexerProxies;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Indexers.Newznab;
using NzbDrone.Core.IndexerSearch;
using NzbDrone.Host;
using NzbDrone.Test.Common;

namespace NzbDrone.Integration.Test.D8
{
    [TestFixture]
    public class TerminalEvidenceListenerFixture : TestBase<ReleaseSearchService>
    {
        private const string PfxPassword = "disposable-test-only";
        private const string Issuer = "urn:test:d8-capability-issuer";
        private const string Audience = "prowlarr-terminal-evidence:test";
        private const string KeyId = "disposable-key-1";
        private const string InitiatorUri = "spiffe://cluster.local/ns/validation/sa/d8-initiator";
        private const string CorrelationId = "d8b3-" + "AAAAAAAAAAAAAAAAAAAA" + "AAAAAAAAAAAAAAAAAAAAAAA";
        private const string ResponderPodUid = "123e4567-e89b-12d3-a456-426614174010";
        private FakeResponseHandler _transport;
        private Newznab _indexer;

        [DllImport("libc", SetLastError = true)]
        private static extern int fcntl(int fileDescriptor, int command, int argument);

        [Test]
        public async Task listener_should_be_disabled_when_configuration_is_absent()
        {
            var configuration = new ConfigurationBuilder().Build();
            var listener = new TerminalEvidenceListener(configuration, null);

            await listener.StartAsync(CancellationToken.None);
            await listener.StopAsync(CancellationToken.None);
        }

        [Test]
        [Platform("Linux")]
        public async Task worker_startup_and_child_failures_release_admission_and_recover()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-worker-lifecycle-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "terminal-evidence.db"));
                InitializeDatabase(database);
                await AssertWorkerFailureRecovers(database, new TestWorkerProcessLauncher(failFirstLaunch: true), expectedReaped: false);
                await AssertWorkerFailureRecovers(database, new TestWorkerProcessLauncher(crashFirstLaunch: true), expectedReaped: true);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_host_adapter_round_trips_the_production_interface()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-adapter-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-adapter.db"));
                InitializeDatabase(database);
                var journal = new D8SearchOperationJournalWorkerClient(CreateWorkerClient(database));
                var startedAt = DateTimeOffset.UtcNow;
                var started = await journal.StartAsync(
                    "spiffe://cluster.local/ns/validation/sa/d8-initiator",
                    "adapter-round-trip",
                    42,
                    Guid.NewGuid().ToString("D"),
                    Guid.NewGuid().ToString("D"),
                    CorrelationId,
                    startedAt,
                    TimeSpan.FromSeconds(8),
                    CancellationToken.None);
                started.Created.Should().BeTrue();
                started.Entry.State.Should().Be("RUNNING");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_executes_typed_commands_and_recovers_interrupted_operations()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-worker-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal.db"));
                InitializeDatabase(database);
                var client = CreateWorkerClient(database);
                var caller = "spiffe://cluster.local/ns/validation/sa/d8-initiator";
                var operationId = Guid.NewGuid().ToString("D");
                var processInstanceId = Guid.NewGuid().ToString("D");
                var startedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                var start = await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-success",
                    IndexerId = 42,
                    OperationId = operationId,
                    ProcessInstanceId = processInstanceId,
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = startedAt.ToUnixTimeMilliseconds()
                });
                start.ResultCode.Should().Be("ok");
                using (var startPayload = JsonDocument.Parse(start.PayloadJson))
                {
                    startPayload.RootElement.GetProperty("Created").GetBoolean().Should().BeTrue();
                }

                var duplicate = await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-success",
                    IndexerId = 42,
                    OperationId = Guid.NewGuid().ToString("D"),
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = startedAt.AddSeconds(1).ToUnixTimeMilliseconds()
                });
                duplicate.ResultCode.Should().Be("ok");
                using (var duplicatePayload = JsonDocument.Parse(duplicate.PayloadJson))
                {
                    duplicatePayload.RootElement.GetProperty("Created").GetBoolean().Should().BeFalse();
                    duplicatePayload.RootElement.GetProperty("Entry").GetProperty("OperationId").GetString().Should().Be(operationId);
                }

                var concurrentClient = CreateWorkerClient(database);
                D8SearchJournalWorkerRequest ConcurrentStart() => new()
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-concurrent",
                    IndexerId = 42,
                    OperationId = Guid.NewGuid().ToString("D"),
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = startedAt.ToUnixTimeMilliseconds()
                };
                var concurrentResults = await Task.WhenAll(
                    RunJournalCommand(client, ConcurrentStart()),
                    RunJournalCommand(concurrentClient, ConcurrentStart()));
                concurrentResults.Select(result =>
                {
                    using var resultPayload = JsonDocument.Parse(result.PayloadJson);
                    return resultPayload.RootElement.GetProperty("Created").GetBoolean();
                }).Count(created => created).Should().Be(1, "concurrent requests with one caller-scoped key create one durable operation");

                var attemptId = Guid.NewGuid();
                var attemptStarted = startedAt.AddMilliseconds(10).AddTicks(1234);
                var intent = new ObservedAttempt(attemptId, "GET", "indexer.example.net", null, null, attemptStarted, null);
                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "attempt-intent",
                    OperationId = operationId,
                    Attempt = intent
                })).ResultCode.Should().Be("ok");

                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "pre-dispatch-boundary",
                    OperationId = operationId,
                    AttemptId = attemptId.ToString("D")
                })).ResultCode.Should().Be("ok");

                var completedAt = attemptStarted.AddMilliseconds(50);
                var terminalAttempt = intent with { StatusCode = 200, CompletedAt = completedAt };
                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "attempt-terminal",
                    OperationId = operationId,
                    Attempt = terminalAttempt
                })).ResultCode.Should().Be("ok");

                var terminal = CreateRecord(operationId);
                terminal.ProcessInstanceId = processInstanceId;
                terminal.RequestCorrelationId = CorrelationId;
                terminal.StartedAt = startedAt;
                terminal.EndedAt = completedAt;
                terminal.ResultCount = 0;
                terminal.Attempts = new[] { terminalAttempt };
                var finalized = await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "finalize",
                    OperationId = operationId,
                    TerminalRecord = terminal
                });
                finalized.ResultCode.Should().Be("ok");

                using (var connection = database.OpenConnection())
                {
                    var row = connection.QuerySingle<(string State, int ResultCount)>(
                        "SELECT \"State\", \"ResultCount\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @operationId",
                        new { operationId });
                    row.State.Should().Be("SUCCEEDED");
                    row.ResultCount.Should().Be(0, "a completed zero-result search is not NO_SEARCH");
                    connection.QuerySingle<string>(
                        "SELECT \"Stage\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @operationId AND \"AttemptId\" = @attemptId",
                        new { operationId, attemptId = attemptId.ToString("D") }).Should().Be("TERMINAL");
                }

                var interruptedId = Guid.NewGuid().ToString("D");
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-interrupted",
                    IndexerId = 42,
                    OperationId = interruptedId,
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                });
                var interruptedAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, DateTimeOffset.UtcNow, null);
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "attempt-intent",
                    OperationId = interruptedId,
                    Attempt = interruptedAttempt
                });
                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                    .ResultCode.Should().Be("ok");
                using (var connection = database.OpenConnection())
                {
                    connection.QuerySingle<string>("SELECT \"State\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @interruptedId",
                        new { interruptedId }).Should().Be("INDETERMINATE");
                    connection.QuerySingle<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @interruptedId",
                        new { interruptedId }).Should().Be("INDETERMINATE");
                }

                var noSearchId = Guid.NewGuid().ToString("D");
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-no-search",
                    IndexerId = 42,
                    OperationId = noSearchId,
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                });
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" });
                using (var connection = database.OpenConnection())
                {
                    connection.QuerySingle<string>("SELECT \"State\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @noSearchId",
                        new { noSearchId }).Should().Be("NO_SEARCH");
                    connection.QuerySingle<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @noSearchId",
                        new { noSearchId }).Should().Be("NO_SEARCH");
                }

                var failedId = Guid.NewGuid().ToString("D");
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "journal-worker-fail",
                    IndexerId = 42,
                    OperationId = failedId,
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                });
                var failedAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, DateTimeOffset.UtcNow, null);
                await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "attempt-intent",
                    OperationId = failedId,
                    Attempt = failedAttempt
                });
                var failed = await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "fail",
                    OperationId = failedId,
                    CallerIdentity = caller,
                    CorrelationId = CorrelationId,
                    EndedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    FailureReason = "pre_dispatch_boundary_failed"
                });
                failed.ResultCode.Should().Be("ok");
                using (var failedPayload = JsonDocument.Parse(failed.PayloadJson))
                {
                    failedPayload.RootElement.GetProperty("Outcome").GetString().Should().Be("INDETERMINATE");
                }
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_fails_closed_for_every_write_command_under_exclusive_lock()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-lock-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-lock.db"));
                InitializeDatabase(database);
                var terminalRecords = new IndexerSearchTerminalRecordRepository(database, null);
                var journal = new D8SearchOperationJournalRepository(database, terminalRecords);
                var caller = "spiffe://cluster.local/ns/validation/sa/d8-initiator";
                var now = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                D8SearchOperationStartResult Started(string key) => journal.Start(
                    caller,
                    key,
                    42,
                    Guid.NewGuid().ToString("D"),
                    Guid.NewGuid().ToString("D"),
                    CorrelationId,
                    now);

                var intentOperation = Started("lock-intent");
                var intentAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, now, null);
                journal.RecordAttemptIntent(intentOperation.Entry.OperationId, intentAttempt);

                var boundaryOperation = Started("lock-boundary");
                var boundaryAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, now, null);
                journal.RecordAttemptIntent(boundaryOperation.Entry.OperationId, boundaryAttempt);

                var terminalAttemptOperation = Started("lock-terminal-attempt");
                var terminalAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, now, null);
                journal.RecordAttemptIntent(terminalAttemptOperation.Entry.OperationId, terminalAttempt);
                journal.RecordPreDispatchBoundary(terminalAttemptOperation.Entry.OperationId, terminalAttempt.Id);

                var finalizeOperation = Started("lock-finalize");
                var finalizeAttempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, now, null);
                journal.RecordAttemptIntent(finalizeOperation.Entry.OperationId, finalizeAttempt);
                journal.RecordPreDispatchBoundary(finalizeOperation.Entry.OperationId, finalizeAttempt.Id);
                journal.TerminalizeAttempt(finalizeOperation.Entry.OperationId,
                    finalizeAttempt with { StatusCode = 200, CompletedAt = now.AddMilliseconds(10) });

                var failOperation = Started("lock-fail");

                var client = CreateWorkerClient(database);
                var busyResponses = 0;
                using (var lockConnection = database.OpenConnection())
                {
                    lockConnection.Execute("BEGIN EXCLUSIVE");
                    lockConnection.Execute("UPDATE \"D8SearchOperations\" SET \"ResultCount\" = \"ResultCount\" WHERE \"OperationId\" = @operationId",
                        new { operationId = intentOperation.Entry.OperationId });

                    var commands = new[]
                    {
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "start", CallerIdentity = caller, IdempotencyKey = "locked-start", IndexerId = 42, OperationId = Guid.NewGuid().ToString("D"), ProcessInstanceId = Guid.NewGuid().ToString("D"), CorrelationId = CorrelationId, StartedAtUnixMs = now.ToUnixTimeMilliseconds() },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "attempt-intent", OperationId = intentOperation.Entry.OperationId, Attempt = new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, now, null) },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "pre-dispatch-boundary", OperationId = boundaryOperation.Entry.OperationId, AttemptId = boundaryAttempt.Id.ToString("D") },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "attempt-terminal", OperationId = terminalAttemptOperation.Entry.OperationId, Attempt = terminalAttempt with { StatusCode = 200, CompletedAt = now.AddMilliseconds(10) } },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "finalize", OperationId = finalizeOperation.Entry.OperationId, TerminalRecord = new IndexerSearchTerminalRecord { OperationId = finalizeOperation.Entry.OperationId, SchemaVersion = IndexerSearchTerminalContract.SchemaVersion, ApplicationVersion = "3.0.0-test", ProcessInstanceId = finalizeOperation.Entry.ProcessInstanceId, RequestCorrelationId = CorrelationId, IndexerId = 42, OperationType = IndexerSearchTerminalContract.OperationType, StartedAt = now, EndedAt = now.AddMilliseconds(10), Outcome = "SUCCEEDED", ResultCount = 1, Attempts = new[] { finalizeAttempt with { StatusCode = 200, CompletedAt = now.AddMilliseconds(10) } } } },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "fail", OperationId = failOperation.Entry.OperationId, CallerIdentity = caller, CorrelationId = CorrelationId, EndedAtUnixMs = now.AddMilliseconds(10).ToUnixTimeMilliseconds(), FailureReason = "locked-fail" },
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }
                    };

                    foreach (var command in commands)
                    {
                        var commandStarted = Stopwatch.GetTimestamp();
                        try
                        {
                            var response = await client.RunJournalCommandAsync(command, TimeSpan.FromSeconds(4), CancellationToken.None);
                            response.ResultCode.Should().NotBe(
                                "ok",
                                "SQLite contention is reported as a closed category, never success, for {0}",
                                command.Operation);
                            response.ResultCode.Should().Be(D8SearchJournalFailure.BusyLocked);
                            busyResponses++;
                        }
                        catch (OperationCanceledException)
                        {
                            // The process supervisor's fixed parent deadline remains the
                            // backstop if provider contention exceeds its own 1000 ms limit.
                        }

                        Stopwatch.GetElapsedTime(commandStarted).Should().BeLessThan(
                            TimeSpan.FromSeconds(4), "the absolute parent deadline includes worker termination/reaping for {0}", command.Operation);
                    }

                    lockConnection.Execute("ROLLBACK");
                }

                busyResponses.Should().Be(7, "all seven typed write operations return the sanitized busy category before the parent deadline");

                using (var connection = database.OpenConnection())
                {
                    var persistedAttempts = Newtonsoft.Json.JsonConvert.DeserializeObject<ObservedAttempt[]>(connection.QuerySingle<string>(
                        "SELECT \"AttemptsJson\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = intentOperation.Entry.OperationId }));
                    persistedAttempts.Should().ContainSingle(attempt => attempt.Id == intentAttempt.Id,
                        "the killed before-commit intent update cannot partially persist");
                    connection.QuerySingle<string>("SELECT \"Stage\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId",
                        new { OperationId = boundaryOperation.Entry.OperationId, AttemptId = boundaryAttempt.Id.ToString("D") })
                        .Should().Be("INTENT", "the pre-dispatch transition was not committed under contention");
                    connection.QuerySingle<string>("SELECT \"Stage\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId AND \"AttemptId\" = @AttemptId",
                        new { OperationId = terminalAttemptOperation.Entry.OperationId, AttemptId = terminalAttempt.Id.ToString("D") })
                        .Should().Be("BOUNDARY_RECORDED", "terminalization remains atomic under contention");
                    connection.QuerySingle<string>("SELECT \"State\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = finalizeOperation.Entry.OperationId }).Should().Be("RUNNING");
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = finalizeOperation.Entry.OperationId }).Should().Be(0);
                    connection.QuerySingle<string>("SELECT \"State\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = failOperation.Entry.OperationId }).Should().Be("RUNNING");
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"CallerIdentity\" = @CallerIdentity AND \"IdempotencyKey\" = 'locked-start'",
                        new { CallerIdentity = caller }).Should().Be(0);
                }

                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                    .ResultCode.Should().Be("ok", "recovery can terminalize all operations once contention clears");
                using (var connection = database.OpenConnection())
                {
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"State\" IN ('RUNNING', 'FINALIZING')")
                        .Should().Be(0, "contention failures cannot leave recoverable operations indefinitely RUNNING after recovery");
                }

                (await RunJournalCommand(client, new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "after-exclusive-lock",
                    IndexerId = 42,
                    OperationId = Guid.NewGuid().ToString("D"),
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = now.ToUnixTimeMilliseconds()
                })).ResultCode.Should().Be("ok", "the worker is reaped and can service work after contention clears");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_admission_queues_fairly_and_recovery_isolated_from_evidence_reads()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-admission-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-admission.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var seed = new D8SearchOperationJournalRepository(database, repository);
                var startedAt = DateTimeOffset.UtcNow;
                var interrupted = seed.Start(
                    InitiatorUri,
                    "queued-recovery",
                    42,
                    Guid.NewGuid().ToString("D"),
                    Guid.NewGuid().ToString("D"),
                    CorrelationId,
                    startedAt);
                seed.AttemptStarted(interrupted.Entry.OperationId,
                    new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, startedAt, null));

                var launcher = new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 30);
                var client = CreateWorkerClient(database, launcher);
                try
                {
                    using var firstCancellation = new CancellationTokenSource();
                    var first = client.RunJournalCommandAsync(
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                        TimeSpan.FromSeconds(10),
                        firstCancellation.Token);
                    await launcher.FirstStartInfoCreated.Task.WaitAsync(TimeSpan.FromSeconds(3));

                    var queuedOperationId = Guid.NewGuid().ToString("D");
                    var queuedStart = RunJournalCommand(client, new D8SearchJournalWorkerRequest
                    {
                        Version = 1,
                        Operation = "start",
                        CallerIdentity = InitiatorUri,
                        IdempotencyKey = "queued-start",
                        IndexerId = 42,
                        OperationId = queuedOperationId,
                        ProcessInstanceId = Guid.NewGuid().ToString("D"),
                        CorrelationId = CorrelationId,
                        StartedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    });
                    launcher.LaunchCount.Should().Be(1, "the second journal command is queued behind the first, not launched as an excess child");

                    var read = await client.ReadAndRedeemAsync(
                        Guid.NewGuid().ToString("D"),
                        CorrelationId,
                        Guid.NewGuid().ToString("D"),
                        DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds(),
                        CancellationToken.None);
                    read.Redeemed.Should().BeTrue("read admission has a separate bounded slot from journal writes");
                    launcher.LaunchCount.Should().Be(2, "one journal child and one evidence child are the total process ceiling");

                    firstCancellation.Cancel();
                    await FluentActions.Awaiting(() => first).Should().ThrowAsync<OperationCanceledException>();
                    var startResponse = await queuedStart;
                    startResponse.ResultCode.Should().Be("ok", "the FIFO waiter receives the released slot and can durably start a second operation");
                    using (var startPayload = JsonDocument.Parse(startResponse.PayloadJson))
                    {
                        startPayload.RootElement.GetProperty("Entry").GetProperty("OperationId")
                            .GetString().Should().Be(queuedOperationId);
                    }

                    (await RunJournalCommand(client, new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                        .ResultCode.Should().Be("ok", "recovery can clear the older operation without starving behind the new start");

                    repository.Get(interrupted.Entry.OperationId).Outcome.Should().Be("INDETERMINATE");
                    using var connection = database.OpenConnection();
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"State\" IN ('RUNNING', 'FINALIZING')")
                        .Should().Be(0, "recovery clears the durable interrupted row after the saturated command is reaped");
                    launcher.LaunchCount.Should().Be(4);
                }
                finally
                {
                    await client.StopAsync(CancellationToken.None);
                }
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task concurrent_initiations_do_not_remain_running_when_journal_admission_is_occupied()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-concurrent-initiation-admission-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-concurrent-initiation-admission.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var launcher = new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 30);
                var workerClient = CreateWorkerClient(database, launcher);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                SetUpObservedSearch();
                var service = Subject;

                try
                {
                    using var blockerCancellation = new CancellationTokenSource();
                    var blocker = workerClient.RunJournalCommandAsync(
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                        TimeSpan.FromSeconds(20),
                        blockerCancellation.Token);
                    await launcher.FirstStartInfoCreated.Task.WaitAsync(TimeSpan.FromSeconds(3));

                    using var firstDeadline = new D8SearchDeadline(CancellationToken.None, TimeSpan.FromSeconds(30));
                    using var secondDeadline = new D8SearchDeadline(CancellationToken.None, TimeSpan.FromSeconds(30));
                    var firstInitiation = service.InitiateD8ValidationSearchAsync(42, InitiatorUri, "admission-initiation-one", firstDeadline);
                    var secondInitiation = service.InitiateD8ValidationSearchAsync(42, InitiatorUri, "admission-initiation-two", secondDeadline);

                    launcher.LaunchCount.Should().Be(1, "both initiations are waiting in the bounded FIFO queue instead of creating excess children");
                    blockerCancellation.Cancel();
                    await FluentActions.Awaiting(() => blocker).Should().ThrowAsync<OperationCanceledException>();

                    var results = await Task.WhenAll(firstInitiation, secondInitiation).WaitAsync(TimeSpan.FromSeconds(25));
                    results.Should().HaveCount(2);
                    results.Should().OnlyContain(result => result.Started && result.Operation.State == "SUCCEEDED");
                    using var connection = database.OpenConnection();
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"State\" IN ('RUNNING', 'FINALIZING')")
                        .Should().Be(0, "journal contention cannot leave a recoverable operation stranded in an active state");
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"State\" = 'SUCCEEDED'").Should().Be(2);
                    connection.QuerySingle<int>("SELECT COUNT(*) FROM \"IndexerSearchTerminalRecords\" WHERE \"Outcome\" = 'SUCCEEDED'").Should().Be(2);
                }
                finally
                {
                    await workerClient.StopAsync(CancellationToken.None);
                }
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [TestCase(SQLiteErrorCode.Busy, D8SearchJournalFailure.BusyLocked)]
        [TestCase(SQLiteErrorCode.Locked, D8SearchJournalFailure.BusyLocked)]
        [TestCase(SQLiteErrorCode.Constraint, D8SearchJournalFailure.Constraint)]
        [TestCase(SQLiteErrorCode.Schema, D8SearchJournalFailure.Schema)]
        [TestCase(SQLiteErrorCode.Corrupt, D8SearchJournalFailure.Corrupt)]
        [TestCase(SQLiteErrorCode.NotADb, D8SearchJournalFailure.Corrupt)]
        [TestCase((SQLiteErrorCode)517, D8SearchJournalFailure.BusyLocked)]
        [TestCase((SQLiteErrorCode)2067, D8SearchJournalFailure.Constraint)]
        [TestCase((SQLiteErrorCode)267, D8SearchJournalFailure.Corrupt)]
        [TestCase(SQLiteErrorCode.Error, D8SearchJournalFailure.Internal)]
        public void journal_error_categories_are_closed_and_sanitized(SQLiteErrorCode sqliteError, string expected)
        {
            D8SearchJournalFailure.FromSqliteError(sqliteError).Should().Be(expected);
            var failure = new D8SearchJournalException(expected);
            failure.Category.Should().Be(expected);
            failure.Message.Should().Be("D8 storage operation failed (" + expected + ")");
            failure.Message.Should().NotContain("/tmp/").And.NotContain("SELECT").And.NotContain("token");
            new D8SearchJournalException("/private/path?token=secret").Category.Should().Be(D8SearchJournalFailure.Internal);
        }

        [Test]
        [Platform("Linux")]
        public async Task worker_shutdown_preserves_caller_cancellation()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-shutdown-cancellation-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            var childProcessId = 0;
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-shutdown-cancellation.db"));
                InitializeDatabase(database);
                var launcher = new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 30);
                var waitStarted = new TaskCompletionSource<Process>(TaskCreationOptions.RunContinuationsAsynchronously);
                var releaseWait = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var processControl = new TestWorkerProcessControl(onWait: async (worker, token) =>
                {
                    waitStarted.TrySetResult(worker);
                    await releaseWait.Task.ConfigureAwait(false);
                    await worker.WaitForExitAsync(token).ConfigureAwait(false);
                });
                var client = CreateWorkerClient(database, launcher, processControl);
                var inFlight = client.RunJournalCommandAsync(
                    new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                    TimeSpan.FromSeconds(10),
                    CancellationToken.None);
                await launcher.FirstStartInfoCreated.Task.WaitAsync(TimeSpan.FromSeconds(3));

                using var cancellation = new CancellationTokenSource();
                var stopping = client.StopAsync(cancellation.Token);
                var child = await waitStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));
                childProcessId = child.Id;
                cancellation.Cancel();
                await FluentActions.Awaiting(() => stopping)
                    .Should().ThrowAsync<OperationCanceledException>();
                releaseWait.TrySetResult(true);
                await FluentActions.Awaiting(() => inFlight)
                    .Should().ThrowAsync<OperationCanceledException>();
                IsProcessRunning(childProcessId).Should().BeFalse("caller cancellation must still permit child cleanup and reap");
            }
            finally
            {
                EnsureProcessStopped(childProcessId);
                Directory.Delete(directory, true);
            }
        }

        [Test]
        public void production_worker_client_constructor_uses_real_process_control()
        {
            typeof(TerminalEvidenceWorkerClient).GetConstructors().Should().ContainSingle(
                "production dependency injection must see only the default process-control constructor");
            var directory = Path.Combine(Path.GetTempPath(), "d8-production-di-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-production-di.db"));
                InitializeDatabase(database);
                var client = CreateWorkerClient(database);
                client.ProcessControlType.Should().Be(typeof(TerminalEvidenceWorkerProcessControl));
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task worker_shutdown_timeout_is_reported_and_client_fails_closed()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-shutdown-timeout-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-shutdown-timeout.db"));
                InitializeDatabase(database);
                var launcherEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                using var releaseLauncher = new ManualResetEventSlim();
                var launcher = new TestWorkerProcessLauncher(onLaunch: _ =>
                {
                    launcherEntered.TrySetResult(true);
                    releaseLauncher.Wait();
                });
                var client = CreateWorkerClient(database, launcher);
                var operation = Task.Run(() => client.RunJournalCommandAsync(
                    new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                    TimeSpan.FromSeconds(10),
                    CancellationToken.None));
                await launcherEntered.Task.WaitAsync(TimeSpan.FromSeconds(3));

                try
                {
                    await FluentActions.Awaiting(() => client.StopAsync(CancellationToken.None))
                        .Should().ThrowAsync<TimeoutException>()
                        .WithMessage("D8 workers did not stop within the shutdown bound");
                }
                finally
                {
                    releaseLauncher.Set();
                }

                await FluentActions.Awaiting(() => operation)
                    .Should().ThrowAsync<OperationCanceledException>();
                await FluentActions.Awaiting(() => RunJournalCommand(client,
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                    .Should().ThrowAsync<TerminalEvidenceWorkerUnavailableException>();
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task worker_shutdown_fault_preserves_pending_child_and_fault_classification()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-shutdown-fault-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            var childProcessId = 0;
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-shutdown-fault.db"));
                InitializeDatabase(database);
                var launcher = new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 30);
                var killAttempted = new TaskCompletionSource<Process>(TaskCreationOptions.RunContinuationsAsynchronously);
                var injectedShutdownFault = new IOException("injected process wait failure");
                var killCount = 0;
                var failNextWait = false;
                var processControl = new TestWorkerProcessControl(
                    onKill: worker =>
                    {
                        if (Interlocked.Increment(ref killCount) == 1)
                        {
                            killAttempted.TrySetResult(worker);
                            throw new InvalidOperationException("injected initial termination failure");
                        }

                        if (!worker.HasExited)
                        {
                            worker.Kill(entireProcessTree: true);
                        }
                    },
                    onWait: (worker, token) => failNextWait
                        ? Task.FromException(injectedShutdownFault)
                        : worker.WaitForExitAsync(token));
                var client = CreateWorkerClient(database, launcher, processControl);
                client.IsUnhealthy.Should().BeFalse("the operation begins on a healthy client");
                using var cancellation = new CancellationTokenSource();
                var operation = client.RunJournalCommandAsync(
                    new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                    TimeSpan.FromSeconds(10),
                    cancellation.Token);
                await launcher.FirstStartInfoCreated.Task.WaitAsync(TimeSpan.FromSeconds(3));
                cancellation.Cancel();
                var child = await killAttempted.Task.WaitAsync(TimeSpan.FromSeconds(3));
                childProcessId = child.Id;
                await FluentActions.Awaiting(() => operation).Should().ThrowAsync<OperationCanceledException>();
                client.IsUnhealthy.Should().BeTrue("an unreaped child is the reachable transition to unhealthy");
                client.PendingWorkerCount.Should().Be(1, "the unreaped worker must remain tracked");
                await FluentActions.Awaiting(() => RunJournalCommand(client,
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                    .Should().ThrowAsync<TerminalEvidenceWorkerUnavailableException>();

                failNextWait = true;
                var shutdownError = await FluentActions.Awaiting(() => client.StopAsync(CancellationToken.None))
                    .Should().ThrowAsync<IOException>();
                shutdownError.Which.Should().BeSameAs(injectedShutdownFault,
                    "unexpected reaping faults must not be reclassified as timeout or cancellation");
                client.PendingWorkerCount.Should().Be(1, "unexpected shutdown faults must retain the pending process");

                failNextWait = false;
                await client.StopAsync(CancellationToken.None);
                client.PendingWorkerCount.Should().Be(0);
                IsProcessRunning(childProcessId).Should().BeFalse("a later shutdown pass must still find and reap the retained child");
            }
            finally
            {
                EnsureProcessStopped(childProcessId);
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task worker_shutdown_timeout_while_reaping_pending_child_retains_tracking()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-shutdown-pending-timeout-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            var childProcessId = 0;
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-shutdown-pending-timeout.db"));
                InitializeDatabase(database);
                var launcher = new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 30);
                var killAttempted = new TaskCompletionSource<Process>(TaskCreationOptions.RunContinuationsAsynchronously);
                var waitStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var releaseWait = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var killCount = 0;
                var blockWait = false;
                var processControl = new TestWorkerProcessControl(
                    onKill: worker =>
                    {
                        if (Interlocked.Increment(ref killCount) == 1)
                        {
                            killAttempted.TrySetResult(worker);
                            throw new InvalidOperationException("injected initial termination failure");
                        }

                        if (!worker.HasExited)
                        {
                            worker.Kill(entireProcessTree: true);
                        }
                    },
                    onWait: async (worker, token) =>
                    {
                        if (blockWait)
                        {
                            waitStarted.TrySetResult(true);
                            await releaseWait.Task.ConfigureAwait(false);
                        }

                        await worker.WaitForExitAsync(token).ConfigureAwait(false);
                    });
                var client = CreateWorkerClient(database, launcher, processControl);
                client.IsUnhealthy.Should().BeFalse("the operation begins on a healthy client");
                using var cancellation = new CancellationTokenSource();
                var operation = client.RunJournalCommandAsync(
                    new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" },
                    TimeSpan.FromSeconds(10),
                    cancellation.Token);
                await launcher.FirstStartInfoCreated.Task.WaitAsync(TimeSpan.FromSeconds(3));
                cancellation.Cancel();
                var child = await killAttempted.Task.WaitAsync(TimeSpan.FromSeconds(3));
                childProcessId = child.Id;
                await FluentActions.Awaiting(() => operation).Should().ThrowAsync<OperationCanceledException>();
                client.IsUnhealthy.Should().BeTrue("an unreaped child latches the client unhealthy before shutdown");
                client.PendingWorkerCount.Should().Be(1, "the unreaped child remains in the pending set");

                blockWait = true;
                var stopping = client.StopAsync(CancellationToken.None);
                await waitStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));
                await FluentActions.Awaiting(() => stopping)
                    .Should().ThrowAsync<TimeoutException>()
                    .WithMessage("D8 workers did not stop within the shutdown bound");
                client.PendingWorkerCount.Should().Be(1, "a timed-out reap must not discard the pending child");
                await FluentActions.Awaiting(() => RunJournalCommand(client,
                        new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" }))
                    .Should().ThrowAsync<TerminalEvidenceWorkerUnavailableException>();

                blockWait = false;
                releaseWait.TrySetResult(true);
                await client.StopAsync(CancellationToken.None);
                client.PendingWorkerCount.Should().Be(0);
                IsProcessRunning(childProcessId).Should().BeFalse("timed-out reaping must retain the process until a later pass confirms exit");
            }
            finally
            {
                EnsureProcessStopped(childProcessId);
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_rejects_malformed_frames_and_recovers_after_child_crash()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-protocol-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-protocol.db"));
                InitializeDatabase(database);
                var assembly = Path.Combine(AppContext.BaseDirectory, "Prowlarr.dll");
                var validShapeWrongVersion = Encoding.UTF8.GetBytes("{\"Version\":2,\"Operation\":\"recover\"}");
                (await RunRawJournalWorker(assembly, database.ConnectionString, validShapeWrongVersion.Length, validShapeWrongVersion)).Should().Be(31);
                var unexpectedArgument = Encoding.UTF8.GetBytes("{\"Version\":1,\"Operation\":\"recover\",\"IndexerId\":42}");
                (await RunRawJournalWorker(assembly, database.ConnectionString, unexpectedArgument.Length, unexpectedArgument)).Should().Be(31,
                    "typed operations reject arguments outside their contract");
                var operationId = Guid.NewGuid().ToString("D");
                var attemptId = Guid.NewGuid().ToString("D");
                var unexpectedNestedArgument = Encoding.UTF8.GetBytes(
                    $"{{\"Version\":1,\"Operation\":\"attempt-intent\",\"OperationId\":\"{operationId}\",\"Attempt\":{{\"Id\":\"{attemptId}\",\"Method\":\"GET\",\"Host\":\"indexer.example.net\",\"StatusCode\":null,\"ExceptionType\":null,\"StartedAt\":\"2026-10-20T12:00:00Z\",\"CompletedAt\":null,\"Unexpected\":true}}}}");
                (await RunRawJournalWorker(assembly, database.ConnectionString, unexpectedNestedArgument.Length, unexpectedNestedArgument)).Should().Be(40,
                    "nested typed arguments reject undeclared members");
                (await RunRawJournalWorker(assembly, database.ConnectionString, null, null)).Should().Be(40, "unexpected EOF is a protocol failure");
                (await RunRawJournalWorker(assembly, database.ConnectionString, 12, Encoding.UTF8.GetBytes("{}"))).Should().Be(40, "truncated frames must fail closed");
                (await RunRawJournalWorker(assembly, database.ConnectionString, TerminalEvidenceWorker.MaximumJournalRequestFrameBytes + 1, Array.Empty<byte>()))
                    .Should().Be(40, "oversized frames must be rejected before allocation");
                var invalidJson = Encoding.UTF8.GetBytes("{not-json");
                (await RunRawJournalWorker(assembly, database.ConnectionString, invalidJson.Length, invalidJson)).Should().Be(40);

                var client = CreateWorkerClient(database, new TestWorkerProcessLauncher(crashFirstLaunch: true));
                var request = new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" };
                var failed = () => client.RunJournalCommandAsync(request, TimeSpan.FromSeconds(4), CancellationToken.None);
                await FluentActions.Awaiting(failed).Should().ThrowAsync<IOException>();
                (await RunJournalCommand(client, request)).ResultCode.Should().Be("ok", "the failed child must be reaped and admission released");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_killed_after_commit_keeps_idempotency_consumed()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-after-commit-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            Process worker = null;
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-after-commit.db"));
                InitializeDatabase(database);
                var terminalRecords = new IndexerSearchTerminalRecordRepository(database, null);
                var journal = new D8SearchOperationJournalRepository(database, terminalRecords);
                var caller = "spiffe://cluster.local/ns/validation/sa/d8-initiator";
                var operationId = Guid.NewGuid().ToString("D");
                var processInstanceId = Guid.NewGuid().ToString("D");
                var now = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                var started = journal.Start(caller, "after-commit-no-redispatch", 42, operationId, processInstanceId, CorrelationId, now);
                started.Created.Should().BeTrue();

                var attempts = Enumerable.Range(0, 16)
                    .Select(_ => new ObservedAttempt(
                        Guid.NewGuid(),
                        "GET",
                        "indexer.example.net",
                        500,
                        new string('\u0001', 128),
                        now,
                        now.AddMilliseconds(100)))
                    .ToArray();
                using (var connection = database.OpenConnection())
                using (var transaction = connection.BeginTransaction())
                {
                    connection.Execute(
                        "UPDATE \"D8SearchOperations\" SET \"AttemptsJson\" = @AttemptsJson WHERE \"OperationId\" = @OperationId",
                        new { AttemptsJson = Newtonsoft.Json.JsonConvert.SerializeObject(attempts), OperationId = operationId },
                        transaction);
                    foreach (var attempt in attempts)
                    {
                        connection.Execute(
                            "INSERT INTO \"D8SearchAttemptProgress\" (\"OperationId\", \"AttemptId\", \"Stage\", \"AttemptJson\") VALUES (@OperationId, @AttemptId, 'TERMINAL', @AttemptJson)",
                            new { OperationId = operationId, AttemptId = attempt.Id.ToString("D"), AttemptJson = Newtonsoft.Json.JsonConvert.SerializeObject(attempt) },
                            transaction);
                    }

                    transaction.Commit();
                }

                var terminal = new IndexerSearchTerminalRecord
                {
                    OperationId = operationId,
                    SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                    ApplicationVersion = "3.0.0-test",
                    ProcessInstanceId = processInstanceId,
                    RequestCorrelationId = CorrelationId,
                    IndexerId = 42,
                    OperationType = IndexerSearchTerminalContract.OperationType,
                    StartedAt = now,
                    EndedAt = now.AddMilliseconds(200),
                    Outcome = "FAILED",
                    FailureReason = "disposable_failure",
                    ResultCount = 0,
                    Attempts = attempts
                };
                var responseBody = JsonSerializer.SerializeToUtf8Bytes(D8SearchJournalWorkerResponse.Success(
                    Newtonsoft.Json.JsonConvert.SerializeObject(terminal)));
                responseBody.Length.Should().BeGreaterThan(4096, "the response must exceed the deliberately constrained pipe capacity");
                responseBody.Length.Should().BeLessThanOrEqualTo(TerminalEvidenceWorker.MaximumResponseFrameBytes);

                var assembly = Path.Combine(AppContext.BaseDirectory, "Prowlarr.dll");
                var startInfo = new ProcessStartInfo("dotnet")
                {
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                startInfo.ArgumentList.Add(assembly);
                startInfo.ArgumentList.Add(TerminalEvidenceWorker.JournalEntryPointArgument);
                startInfo.Environment["PROWLARR_D8_WORKER_PARENT_PID"] = Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                startInfo.Environment["PROWLARR_D8_WORKER_SQLITE_CONNECTION"] = database.ConnectionString;
                worker = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start disposable journal worker");
                var errorDrain = worker.StandardError.ReadToEndAsync();
                var outputPipe = worker.StandardOutput.BaseStream as PipeStream;
                outputPipe.Should().NotBeNull("the worker response is carried over a bounded anonymous pipe");
                var pipeCapacity = fcntl((int)outputPipe.SafePipeHandle.DangerousGetHandle(), 1031, 4096);
                pipeCapacity.Should().BeGreaterThan(0, "the test must constrain the disposable response pipe");

                var requestBytes = JsonSerializer.SerializeToUtf8Bytes(new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "finalize",
                    OperationId = operationId,
                    TerminalRecord = terminal
                });
                var prefix = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(prefix, requestBytes.Length);
                await worker.StandardInput.BaseStream.WriteAsync(prefix);
                await worker.StandardInput.BaseStream.WriteAsync(requestBytes);
                worker.StandardInput.Close();

                var committed = false;
                var deadline = Stopwatch.StartNew();
                while (deadline.Elapsed < TimeSpan.FromSeconds(3))
                {
                    using var connection = database.OpenConnection();
                    var state = connection.QuerySingle<string>("SELECT \"State\" FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = operationId });
                    if (state == "FAILED")
                    {
                        committed = true;
                        break;
                    }

                    await Task.Delay(10);
                }

                committed.Should().BeTrue("the child must durably finalize before the parent kills it");
                worker.HasExited.Should().BeFalse("the child must still be blocked sending the response after its database commit");
                worker.Kill(entireProcessTree: true);
                await worker.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(2));
                await errorDrain.WaitAsync(TimeSpan.FromSeconds(1));
                worker.HasExited.Should().BeTrue("the parent confirms child reaping");

                using (var connection = database.OpenConnection())
                {
                    connection.QuerySingle<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @OperationId",
                        new { OperationId = operationId }).Should().Be("FAILED");
                }

                var retryResponse = await RunJournalCommand(CreateWorkerClient(database), new D8SearchJournalWorkerRequest
                {
                    Version = 1,
                    Operation = "start",
                    CallerIdentity = caller,
                    IdempotencyKey = "after-commit-no-redispatch",
                    IndexerId = 42,
                    OperationId = Guid.NewGuid().ToString("D"),
                    ProcessInstanceId = Guid.NewGuid().ToString("D"),
                    CorrelationId = CorrelationId,
                    StartedAtUnixMs = now.AddSeconds(1).ToUnixTimeMilliseconds()
                });
                retryResponse.ResultCode.Should().Be("ok");
                using (var retryPayload = JsonDocument.Parse(retryResponse.PayloadJson))
                {
                    retryPayload.RootElement.GetProperty("Created").GetBoolean().Should().BeFalse(
                        "ambiguous retry after commit returns the durable operation and cannot start another search");
                    retryPayload.RootElement.GetProperty("Entry").GetProperty("OperationId").GetString().Should().Be(operationId);
                }
            }
            finally
            {
                if (worker != null)
                {
                    if (!worker.HasExited)
                    {
                        worker.Kill(entireProcessTree: true);
                        await worker.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(2));
                    }

                    worker.Dispose();
                }

                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task journal_worker_caller_cancellation_kills_reaps_and_releases_admission()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-journal-cancel-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-journal-cancel.db"));
                InitializeDatabase(database);
                var client = CreateWorkerClient(database, new TestWorkerProcessLauncher(hangFirstLaunch: true));
                using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
                var request = new D8SearchJournalWorkerRequest { Version = 1, Operation = "recover" };
                var cancelled = () => client.RunJournalCommandAsync(request, TimeSpan.FromSeconds(4), cancellation.Token);
                await FluentActions.Awaiting(cancelled).Should().ThrowAsync<OperationCanceledException>();

                (await RunJournalCommand(client, request)).ResultCode.Should().Be("ok",
                    "the cancelled child must be killed and reaped before the next worker is admitted");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        private static async Task<int> RunRawJournalWorker(string assembly, string connectionString, int? frameLength, byte[] body)
        {
            var startInfo = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
            startInfo.ArgumentList.Add(assembly);
            startInfo.ArgumentList.Add(TerminalEvidenceWorker.JournalEntryPointArgument);
            startInfo.Environment["PROWLARR_D8_WORKER_PARENT_PID"] = Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
            startInfo.Environment["PROWLARR_D8_WORKER_SQLITE_CONNECTION"] = connectionString;
            using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start disposable journal worker");
            var stderr = process.StandardError.ReadToEndAsync();
            if (frameLength.HasValue)
            {
                var prefix = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(prefix, frameLength.Value);
                await process.StandardInput.BaseStream.WriteAsync(prefix);
                if (body?.Length > 0)
                {
                    await process.StandardInput.BaseStream.WriteAsync(body);
                }
            }

            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(4));
            await stderr.WaitAsync(TimeSpan.FromSeconds(1));
            return process.ExitCode;
        }

        [Test]
        [Platform("Linux")]
        public async Task initiation_requires_exact_mtls_identity_and_persists_one_correlated_search()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-search-initiation-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                using var root = CreateRootCertificate();
                using var server = CreateLeafCertificate(root, "d8-initiation-server", false, IPAddress.Loopback, ResponderPodUid);
                using var initiator = CreateLeafCertificate(root, "approved-initiator", true, uriSan: InitiatorUri);
                using var wrongUriInitiator = CreateLeafCertificate(root, "wrong-uri-initiator", true, uriSan: "spiffe://cluster.local/ns/validation/sa/other");
                using var retrievalOnly = CreateLeafCertificate(root, "retrieval-only", true);
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-initiation.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var workerClient = CreateWorkerClient(database);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                SetUpObservedSearch();
                var service = Subject;
                using (var serviceDeadline = new D8SearchDeadline(CancellationToken.None))
                {
                    var serviceResult = await service.InitiateD8ValidationSearchAsync(42, InitiatorUri, "direct-service-check", serviceDeadline);
                    serviceResult.Operation.State.Should().Be("SUCCEEDED");
                }

                var rootPath = Path.Combine(directory, "client-root.pem");
                var serverPath = Path.Combine(directory, "server.pfx");
                var publicKeyPath = Path.Combine(directory, "capability-public.pem");
                File.WriteAllText(rootPath, root.ExportCertificatePem());
                File.WriteAllBytes(serverPath, server.Export(X509ContentType.Pfx, PfxPassword));
                using (var signingKey = RSA.Create(2048))
                {
                    File.WriteAllText(publicKeyPath, signingKey.ExportSubjectPublicKeyInfoPem());
                }

                var initiatorFingerprint = Convert.ToHexString(SHA256.HashData(initiator.RawData)).ToLowerInvariant();
                var wrongUriFingerprint = Convert.ToHexString(SHA256.HashData(wrongUriInitiator.RawData)).ToLowerInvariant();
                var retrievalFingerprint = Convert.ToHexString(SHA256.HashData(retrievalOnly.RawData)).ToLowerInvariant();
                var port = FindAvailablePort();
                var values = new Dictionary<string, string>
                {
                    ["Prowlarr:D8TerminalEvidence:Enabled"] = "true",
                    ["Prowlarr:D8TerminalEvidence:BindAddress"] = IPAddress.Loopback.ToString(),
                    ["Prowlarr:D8TerminalEvidence:Port"] = port.ToString(),
                    ["Prowlarr:D8TerminalEvidence:ServerCertificatePath"] = serverPath,
                    ["Prowlarr:D8TerminalEvidence:ServerCertificatePassword"] = PfxPassword,
                    ["Prowlarr:D8TerminalEvidence:ClientCaCertificatePath"] = rootPath,
                    ["Prowlarr:D8TerminalEvidence:CapabilityPublicKeyPath"] = publicKeyPath,
                    ["Prowlarr:D8TerminalEvidence:CapabilityIssuer"] = Issuer,
                    ["Prowlarr:D8TerminalEvidence:CapabilityAudience"] = Audience,
                    ["Prowlarr:D8TerminalEvidence:CapabilityKeyId"] = KeyId,
                    ["Prowlarr:D8TerminalEvidence:AllowedClientCertificateSha256"] = retrievalFingerprint,
                    ["Prowlarr:D8TerminalEvidence:AllowedInitiatorCertificateSha256"] = initiatorFingerprint + "," + wrongUriFingerprint,
                    ["Prowlarr:D8TerminalEvidence:AllowedInitiatorUriSans"] = InitiatorUri,
                    ["Prowlarr:D8TerminalEvidence:AllowedInitiatorIndexerIds"] = "42"
                };
                var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
                var listener = new TerminalEvidenceListener(configuration, workerClient, service);
                await listener.StartAsync(CancellationToken.None);
                try
                {
                    using var authorizedHandler = CreateClientHandler(root, initiator);
                    using var authorizedClient = new System.Net.Http.HttpClient(authorizedHandler);
                    using var request = new HttpRequestMessage(HttpMethod.Post, $"https://127.0.0.1:{port}/v1/search-operations")
                    {
                        Content = new StringContent("{\"version\":1,\"indexerId\":42,\"idempotencyKey\":\"admission-review-1\"}", Encoding.UTF8, "application/json")
                    };
                    using var response = await authorizedClient.SendAsync(request);
                    response.StatusCode.Should().Be(HttpStatusCode.OK);
                    using var payload = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                    var rootPayload = payload.RootElement;
                    var operationId = rootPayload.GetProperty("operationId").GetString();
                    var correlationId = rootPayload.GetProperty("correlationId").GetString();
                    Guid.TryParseExact(operationId, "D", out _).Should().BeTrue();
                    correlationId.Should().StartWith("d8b3-").And.HaveLength(48);
                    var tokenBytes = Convert.FromBase64String(correlationId[5..].Replace('-', '+').Replace('_', '/') + "=");
                    tokenBytes.Should().HaveCount(32);
                    rootPayload.GetProperty("state").GetString().Should().Be("SUCCEEDED");
                    rootPayload.GetProperty("outcome").GetString().Should().Be("SUCCEEDED");

                    using (var connection = database.OpenConnection())
                    {
                        var row = connection.QuerySingle<InitiationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"OperationId\" = @OperationId", new { OperationId = operationId });
                        row.CallerIdentity.Should().Be(InitiatorUri);
                        row.CorrelationId.Should().Be(correlationId);
                        row.State.Should().Be("SUCCEEDED");
                        var attempts = JsonDocument.Parse(row.AttemptsJson).RootElement;
                        attempts.GetArrayLength().Should().Be(1, "only the actual correlated search dispatch is durable");
                    }

                    var requestCount = _transport.RequestCount;
                    using var retry = new HttpRequestMessage(HttpMethod.Post, $"https://127.0.0.1:{port}/v1/search-operations")
                    {
                        Content = new StringContent("{\"version\":1,\"indexerId\":42,\"idempotencyKey\":\"admission-review-1\"}", Encoding.UTF8, "application/json")
                    };
                    using var retryResponse = await authorizedClient.SendAsync(retry);
                    retryResponse.StatusCode.Should().Be(HttpStatusCode.OK);
                    using var retryPayload = JsonDocument.Parse(await retryResponse.Content.ReadAsStringAsync());
                    retryPayload.RootElement.GetProperty("operationId").GetString().Should().Be(operationId);
                    retryPayload.RootElement.GetProperty("correlationId").GetString().Should().Be(correlationId);
                    retryPayload.RootElement.GetProperty("started").GetBoolean().Should().BeFalse();
                    _transport.RequestCount.Should().Be(requestCount, "an idempotent replay must not dispatch another search");

                    using var badIndexRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":99,\"idempotencyKey\":\"bad-indexer\"}");
                    using var badIndexClient = new System.Net.Http.HttpClient(CreateClientHandler(root, initiator));
                    using var badIndexResponse = await badIndexClient.SendAsync(badIndexRequest);
                    badIndexResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);

                    using var malformedRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42,\"correlationId\":\"caller-controlled\"}");
                    using var malformedResponse = await authorizedClient.SendAsync(malformedRequest);
                    malformedResponse.StatusCode.Should().Be(HttpStatusCode.BadRequest);

                    using var badKeyRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42,\"idempotencyKey\":\"bad key\"}");
                    using var badKeyResponse = await authorizedClient.SendAsync(badKeyRequest);
                    badKeyResponse.StatusCode.Should().Be(HttpStatusCode.BadRequest);

                    using var missingKeyRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42}");
                    using var missingKeyResponse = await authorizedClient.SendAsync(missingKeyRequest);
                    missingKeyResponse.StatusCode.Should().Be(HttpStatusCode.BadRequest);

                    using var nullKeyRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42,\"idempotencyKey\":null}");
                    using var nullKeyResponse = await authorizedClient.SendAsync(nullKeyRequest);
                    nullKeyResponse.StatusCode.Should().Be(HttpStatusCode.BadRequest);

                    using var wrongUriClient = new System.Net.Http.HttpClient(CreateClientHandler(root, wrongUriInitiator));
                    using var wrongUriRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42,\"idempotencyKey\":\"wrong-uri\"}");
                    using var wrongUriResponse = await wrongUriClient.SendAsync(wrongUriRequest);
                    wrongUriResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);

                    using var retrievalClient = new System.Net.Http.HttpClient(CreateClientHandler(root, retrievalOnly));
                    using var retrievalRequest = CreateInitiationRequest(port, "{\"version\":1,\"indexerId\":42,\"idempotencyKey\":\"retrieval-only\"}");
                    using var retrievalResponse = await retrievalClient.SendAsync(retrievalRequest);
                    retrievalResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);
                }
                finally
                {
                    await listener.StopAsync(CancellationToken.None);
                }
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        [Platform("Linux")]
        public async Task initiation_deadline_or_caller_cancellation_is_persisted_without_timeout_warning(bool callerCancellation)
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-search-deadline-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-deadline.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var workerClient = CreateWorkerClient(database);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                SetUpObservedSearch();
                _transport.BlockUntilCancelled = true;

                using var callerToken = new CancellationTokenSource();
                if (callerCancellation)
                {
                    callerToken.CancelAfter(TimeSpan.FromSeconds(3));
                }

                using var deadline = new D8SearchDeadline(callerToken.Token,
                    callerCancellation ? TimeSpan.FromSeconds(120) : TimeSpan.FromSeconds(5));
                await FluentActions.Awaiting(() => Subject.InitiateD8ValidationSearchAsync(42, InitiatorUri, "deadline-test", deadline))
                    .Should().ThrowAsync<OperationCanceledException>();

                using var connection = database.OpenConnection();
                var row = connection.QuerySingle<InitiationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"IdempotencyKey\" = @Key",
                    new { Key = "deadline-test" });
                var expectedOutcome = callerCancellation ? "CANCELLED" : "TIMED_OUT";
                row.State.Should().Be(expectedOutcome);
                JsonDocument.Parse(row.AttemptsJson).RootElement.GetArrayLength().Should().Be(1);
                var terminal = repository.Get(row.OperationId);
                terminal.Should().NotBeNull();
                terminal.Outcome.Should().Be(expectedOutcome);
                terminal.Attempts.Should().ContainSingle().Which.ExceptionType.Should().Be("OperationCanceledException");
                ExceptionVerification.ExpectedWarns(0);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task failed_pre_transport_boundary_never_dispatches_and_recovers_as_indeterminate()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-boundary-failure-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-boundary-failure.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var workerClient = CreateWorkerClient(database, new TestWorkerProcessLauncher(crashLaunchNumber: 3));
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                SetUpObservedSearch();

                using var deadline = new D8SearchDeadline(CancellationToken.None);
                var failure = await FluentActions.Awaiting(() => Subject.InitiateD8ValidationSearchAsync(42, InitiatorUri, "boundary-crash", deadline))
                    .Should().ThrowAsync<D8SearchJournalException>();
                failure.Which.Category.Should().Be(D8SearchJournalFailure.Rejected);

                _transport.RequestCount.Should().Be(0, "SendAsync is forbidden until the boundary worker response is confirmed");
                using var connection = database.OpenConnection();
                var row = connection.QuerySingle<InitiationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"IdempotencyKey\" = @Key",
                    new { Key = "boundary-crash" });
                row.State.Should().Be("INDETERMINATE");
                connection.QuerySingle<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @OperationId",
                    new { row.OperationId }).Should().Be("INDETERMINATE");
                connection.QuerySingle<string>("SELECT \"Stage\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId",
                    new { row.OperationId }).Should().Be("INTENT");
                ExceptionVerification.ExpectedErrors(1);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task cancellation_before_intent_persistence_creates_no_operation_or_dispatch()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-before-intent-cancel-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-before-intent-cancel.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(CreateWorkerClient(database)));
                SetUpObservedSearch();

                using var cancellation = new CancellationTokenSource();
                cancellation.Cancel();
                using var deadline = new D8SearchDeadline(cancellation.Token);
                await FluentActions.Awaiting(() => Subject.InitiateD8ValidationSearchAsync(42, InitiatorUri, "cancel-before-intent", deadline))
                    .Should().ThrowAsync<OperationCanceledException>();

                _transport.RequestCount.Should().Be(0);
                using var connection = database.OpenConnection();
                connection.QuerySingle<int>("SELECT COUNT(*) FROM \"D8SearchOperations\" WHERE \"IdempotencyKey\" = @Key",
                    new { Key = "cancel-before-intent" }).Should().Be(0);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task cancellation_after_durable_intent_but_before_boundary_is_indeterminate_without_dispatch()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-after-intent-cancel-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-after-intent-cancel.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                using var cancellation = new CancellationTokenSource();
                var launcher = new TestWorkerProcessLauncher(onLaunch: number =>
                {
                    if (number == 3)
                    {
                        cancellation.Cancel();
                    }
                });
                var workerClient = CreateWorkerClient(database, launcher);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                SetUpObservedSearch();

                using var deadline = new D8SearchDeadline(cancellation.Token);
                await FluentActions.Awaiting(() => Subject.InitiateD8ValidationSearchAsync(42, InitiatorUri, "cancel-after-intent", deadline))
                    .Should().ThrowAsync<Exception>();

                _transport.RequestCount.Should().Be(0, "the transport call follows the persisted boundary and was never reached");
                using var connection = database.OpenConnection();
                var row = connection.QuerySingle<InitiationRow>("SELECT * FROM \"D8SearchOperations\" WHERE \"IdempotencyKey\" = @Key",
                    new { Key = "cancel-after-intent" });
                row.State.Should().Be("INDETERMINATE");
                connection.QuerySingle<string>("SELECT \"Outcome\" FROM \"IndexerSearchTerminalRecords\" WHERE \"OperationId\" = @OperationId",
                    new { row.OperationId }).Should().Be("INDETERMINATE");
                connection.QuerySingle<string>("SELECT \"Stage\" FROM \"D8SearchAttemptProgress\" WHERE \"OperationId\" = @OperationId",
                    new { row.OperationId }).Should().Be("INTENT");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task startup_recovers_interrupted_operations_even_when_listener_is_disabled()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-disabled-recovery-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-recovery.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var seedJournal = new D8SearchOperationJournalRepository(database, repository);
                var workerClient = CreateWorkerClient(database);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));
                var operationId = Guid.NewGuid().ToString("D");
                var correlationId = "d8b3-" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
                var startedAt = DateTimeOffset.UtcNow;
                seedJournal.Start(InitiatorUri, "disabled-recovery", 42, operationId, Guid.NewGuid().ToString("D"), correlationId, startedAt);
                seedJournal.AttemptStarted(operationId, new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", null, null, startedAt, null));

                var listener = new TerminalEvidenceListener(new ConfigurationBuilder().Build(), null, Subject);
                await listener.StartAsync(CancellationToken.None);
                var terminal = repository.Get(operationId);
                terminal.Should().NotBeNull();
                terminal.Outcome.Should().Be("INDETERMINATE");
                terminal.FailureReason.Should().Be("process_interrupted");
                await listener.StopAsync(CancellationToken.None);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task startup_recovery_failure_is_bounded_and_keeps_listener_unready()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-startup-recovery-bound-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                var database = new DisposableMainDatabase(Path.Combine(directory, "d8-startup-recovery.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                var workerClient = CreateWorkerClient(database, new TestWorkerProcessLauncher(hangFirstLaunch: true, hangSeconds: 60));
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                Mocker.SetConstant<ID8SearchOperationJournal>(new D8SearchOperationJournalWorkerClient(workerClient));

                var listener = new TerminalEvidenceListener(new ConfigurationBuilder().Build(), workerClient, Subject);
                var started = Stopwatch.GetTimestamp();
                await listener.StartAsync(CancellationToken.None);
                var elapsed = Stopwatch.GetElapsedTime(started);

                elapsed.Should().BeLessThan(TimeSpan.FromSeconds(17), "startup recovery has a fixed 15 second maximum plus bounded cleanup");
                listener.IsRecoveryReady.Should().BeFalse("failed mandatory recovery must block both initiation and evidence responses");
                await listener.StopAsync(CancellationToken.None);
                ExceptionVerification.ExpectedErrors(1);
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        private static HttpRequestMessage CreateInitiationRequest(int port, string body) => new(HttpMethod.Post, $"https://127.0.0.1:{port}/v1/search-operations")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };

        private static async Task AssertWorkerFailureRecovers(
            DisposableMainDatabase database,
            TestWorkerProcessLauncher launcher,
            bool expectedReaped)
        {
            var workerClient = CreateWorkerClient(database, launcher);
            try
            {
                var failed = false;
                try
                {
                    await workerClient.ReadAndRedeemAsync(
                        Guid.NewGuid().ToString("D"),
                        CorrelationId,
                        Guid.NewGuid().ToString("D"),
                        DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds(),
                        CancellationToken.None);
                }
                catch (TerminalEvidenceWorkerException exception)
                {
                    exception.WorkerReaped.Should().Be(expectedReaped);
                    failed = true;
                }

                failed.Should().BeTrue("the injected startup or child failure must fail closed");
                var recovered = await workerClient.ReadAndRedeemAsync(
                    Guid.NewGuid().ToString("D"),
                    CorrelationId,
                    Guid.NewGuid().ToString("D"),
                    DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds(),
                    CancellationToken.None);
                recovered.Redeemed.Should().BeTrue("a completed failure must release single-flight admission");
                recovered.Record.Should().BeNull();
            }
            finally
            {
                await workerClient.StopAsync(CancellationToken.None);
            }
        }

        [Test]
        [Platform("Linux")]
        public async Task producer_mtls_retrieval_succeeds_but_collector_rejects_missing_observer_receipt()
        {
            var directory = Path.Combine(Path.GetTempPath(), "d8-terminal-listener-" + Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(directory);
            try
            {
                using var root = CreateRootCertificate();
                using var server = CreateLeafCertificate(root, "terminal-evidence-server", false, IPAddress.Loopback, ResponderPodUid);
                using var collector = CreateLeafCertificate(root, "approved-collector", true);
                using var unapprovedCollector = CreateLeafCertificate(root, "unapproved-collector", true);
                var database = new DisposableMainDatabase(Path.Combine(directory, "terminal-evidence.db"));
                InitializeDatabase(database);
                var repository = new IndexerSearchTerminalRecordRepository(database, null);
                Mocker.SetConstant<IMainDatabase>(database);
                Mocker.SetConstant<IIndexerSearchTerminalRecordRepository>(repository);
                SetUpObservedSearch();
                var observedOperation = await Subject.SearchObserved(42, true).ConfigureAwait(false);
                var storedRecord = observedOperation.TerminalRecord;
                repository.Get(storedRecord.OperationId)
                    .Attempts.Should().ContainSingle();
                storedRecord.Outcome.Should().Be("SUCCEEDED");
                storedRecord.CommitAcknowledged.Should().BeTrue();
                storedRecord.Attempts[0].StatusCode.Should().Be((int)HttpStatusCode.OK);
                var rootPath = Path.Combine(directory, "client-root.pem");
                var serverPath = Path.Combine(directory, "server.pfx");
                var publicKeyPath = Path.Combine(directory, "capability-public.pem");
                File.WriteAllText(rootPath, root.ExportCertificatePem());
                File.WriteAllBytes(serverPath, server.Export(X509ContentType.Pfx, PfxPassword));
                var clientCertificatePath = Path.Combine(directory, "collector-cert.pem");
                var clientPrivateKeyPath = Path.Combine(directory, "collector-key.pem");
                File.WriteAllText(clientCertificatePath, collector.ExportCertificatePem());
                using (var clientKey = collector.GetRSAPrivateKey())
                {
                    File.WriteAllText(clientPrivateKeyPath, clientKey.ExportPkcs8PrivateKeyPem());
                }

                var issuerServerCertificatePath = Path.Combine(directory, "issuer-cert.pem");
                var issuerServerPrivateKeyPath = Path.Combine(directory, "issuer-key.pem");
                File.WriteAllText(issuerServerCertificatePath, server.ExportCertificatePem());
                using (var issuerServerKey = server.GetRSAPrivateKey())
                {
                    File.WriteAllText(issuerServerPrivateKeyPath, issuerServerKey.ExportPkcs8PrivateKeyPem());
                }

                using var signingKey = RSA.Create(2048);
                File.WriteAllText(publicKeyPath, signingKey.ExportSubjectPublicKeyInfoPem());
                var issuerSigningKeyPath = Path.Combine(directory, "issuer-signing-key.pem");
                File.WriteAllText(issuerSigningKeyPath, signingKey.ExportPkcs8PrivateKeyPem());
                RestrictPrivateFile(clientPrivateKeyPath);
                RestrictPrivateFile(issuerServerPrivateKeyPath);
                RestrictPrivateFile(issuerSigningKeyPath);

                var operationId = storedRecord.OperationId;
                var now = DateTimeOffset.UtcNow;
                var token = CreateCapability(
                    signingKey,
                    operationId,
                    collector,
                    now,
                    storedRecord.RequestCorrelationId);
                var expectedFingerprint = Convert.ToHexString(SHA256.HashData(collector.RawData)).ToLowerInvariant();
                var port = FindAvailablePort();
                var values = new System.Collections.Generic.Dictionary<string, string>
                {
                    ["Prowlarr:D8TerminalEvidence:Enabled"] = "true",
                    ["Prowlarr:D8TerminalEvidence:BindAddress"] = IPAddress.Loopback.ToString(),
                    ["Prowlarr:D8TerminalEvidence:Port"] = port.ToString(),
                    ["Prowlarr:D8TerminalEvidence:ServerCertificatePath"] = serverPath,
                    ["Prowlarr:D8TerminalEvidence:ServerCertificatePassword"] = PfxPassword,
                    ["Prowlarr:D8TerminalEvidence:ClientCaCertificatePath"] = rootPath,
                    ["Prowlarr:D8TerminalEvidence:CapabilityPublicKeyPath"] = publicKeyPath,
                    ["Prowlarr:D8TerminalEvidence:CapabilityIssuer"] = Issuer,
                    ["Prowlarr:D8TerminalEvidence:CapabilityAudience"] = Audience,
                    ["Prowlarr:D8TerminalEvidence:CapabilityKeyId"] = KeyId,
                    ["Prowlarr:D8TerminalEvidence:AllowedClientCertificateSha256"] = expectedFingerprint
                };
                var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
                var launcher = new TestWorkerProcessLauncher();
                var workerClient = CreateWorkerClient(database, launcher);
                TerminalEvidenceListener listener = new(configuration, workerClient);
                await listener.StartAsync(CancellationToken.None);
                try
                {
                    // Reuse the record produced by ReleaseSearchService.SearchObserved above.
                    // Do not substitute a hand-constructed successful evidence row in the
                    // producer -> collector retrieval path.
                    var interopRecord = storedRecord;
                    var receiptDirectory = Path.Combine(directory, "collector-receipts");
                    await AssertCollectorRejectsMissingObserverAsync(new
                    {
                        ca_file = rootPath,
                        client_certificate = clientCertificatePath,
                        client_private_key = clientPrivateKeyPath,
                        issuer_signing_key = issuerSigningKeyPath,
                        issuer_server_certificate = issuerServerCertificatePath,
                        issuer_server_private_key = issuerServerPrivateKeyPath,
                        issuer_port = FindAvailablePort(),
                        producer_port = port,
                        issuer = Issuer,
                        audience = Audience,
                        key_id = KeyId,
                        client_certificate_sha256 = expectedFingerprint,
                        operation_id = interopRecord.OperationId,
                        correlation_id = interopRecord.RequestCorrelationId,
                        responder_pod_uid = ResponderPodUid,
                        receipt_directory = receiptDirectory
                    }).ConfigureAwait(false);
                    File.Exists(Path.Combine(receiptDirectory, interopRecord.OperationId + ".json"))
                        .Should().BeFalse("the fixture has no independently trusted observer key or receipt");

                    using var handler = CreateClientHandler(root, collector);
                    using var client = new System.Net.Http.HttpClient(handler);
                    var wrongOperationToken = CreateCapability(
                        signingKey,
                        Guid.NewGuid().ToString("D"),
                        collector,
                        now,
                        storedRecord.RequestCorrelationId);
                    using var wrongOperationResponse = await SendCapabilityAsync(client, port, operationId, wrongOperationToken);
                    wrongOperationResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

                    var wrongCorrelation = "d8b3-" + new string('A', 42) + "Q";
                    var wrongCorrelationToken = CreateCapability(signingKey, operationId, collector, now, wrongCorrelation);
                    using var wrongCorrelationResponse = await SendCapabilityAsync(client, port, operationId, wrongCorrelationToken);
                    wrongCorrelationResponse.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

                    using var first = await SendCapabilityAsync(client, port, operationId, token);
                    first.StatusCode.Should().Be(HttpStatusCode.OK);
                    launcher.LastStartInfo.Environment.Should().NotContainKey("D8_TEST_INHERITED_SECRET");
                    launcher.LastStartInfo.Environment.Should().ContainKey("PROWLARR_D8_WORKER_SQLITE_CONNECTION");
                    var body = await first.Content.ReadAsStringAsync();
                    JsonDocument.Parse(body).RootElement.GetProperty("OperationId").GetString().Should().Be(operationId);
                    JsonDocument.Parse(body).RootElement.GetProperty("RequestCorrelationId").GetString().Should().Be(storedRecord.RequestCorrelationId);
                    JsonDocument.Parse(body).RootElement.GetProperty("Attempts").GetArrayLength().Should().Be(1);

                    using var replayResponse = await SendCapabilityAsync(client, port, operationId, token);
                    replayResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

                    await listener.StopAsync(CancellationToken.None);
                    workerClient = CreateWorkerClient(database);
                    listener = new TerminalEvidenceListener(configuration, workerClient);
                    await listener.StartAsync(CancellationToken.None);
                    using var restartedHandler = CreateClientHandler(root, collector);
                    using var restartedClient = new System.Net.Http.HttpClient(restartedHandler);
                    using var restartReplayResponse = await SendCapabilityAsync(restartedClient, port, operationId, token);
                    restartReplayResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

                    var concurrentRecord = repository.Save(CreateRecord(Guid.NewGuid().ToString("D")));
                    var concurrentToken = CreateCapability(signingKey, concurrentRecord.OperationId, collector, now);
                    var concurrentResponses = await Task.WhenAll(
                        SendCapabilityAsync(client, port, concurrentRecord.OperationId, concurrentToken),
                        SendCapabilityAsync(client, port, concurrentRecord.OperationId, concurrentToken));
                    using (concurrentResponses[0])
                    using (concurrentResponses[1])
                    {
                        new[] { concurrentResponses[0].StatusCode, concurrentResponses[1].StatusCode }
                            .Count(code => code == HttpStatusCode.OK).Should().Be(1);
                        new[] { concurrentResponses[0].StatusCode, concurrentResponses[1].StatusCode }
                            .Count(code => code is HttpStatusCode.Unauthorized or HttpStatusCode.ServiceUnavailable)
                            .Should().Be(1, "the loser is either rejected as replay or refused by bounded single-flight admission");
                    }

                    var expiredRecord = repository.Save(CreateRecord(Guid.NewGuid().ToString("D")));
                    var expiredToken = CreateCapability(signingKey, expiredRecord.OperationId, collector, now.AddSeconds(-90));
                    using var expiredResponse = await SendCapabilityAsync(client, port, expiredRecord.OperationId, expiredToken);
                    expiredResponse.StatusCode.Should().Be(HttpStatusCode.Unauthorized);

                    var corruptRecord = repository.Save(CreateRecord(Guid.NewGuid().ToString("D")));
                    using (var connection = database.OpenConnection())
                    {
                        connection.Execute("UPDATE \"IndexerSearchTerminalRecords\" SET \"AttemptsJson\" = 'not-json' WHERE \"OperationId\" = @OperationId",
                            new { corruptRecord.OperationId });
                    }

                    var corruptToken = CreateCapability(signingKey, corruptRecord.OperationId, collector, now);
                    using var corruptResponse = await SendCapabilityAsync(client, port, corruptRecord.OperationId, corruptToken);
                    corruptResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);

                    var lockedRecord = repository.Save(CreateRecord(Guid.NewGuid().ToString("D")));
                    var lockedToken = CreateCapability(signingKey, lockedRecord.OperationId, collector, now);
                    using (var lockConnection = database.OpenConnection())
                    {
                        lockConnection.Execute("BEGIN EXCLUSIVE");
                        using var lockedResponse = await SendCapabilityAsync(client, port, lockedRecord.OperationId, lockedToken);
                        lockedResponse.StatusCode.Should().BeOneOf(
                            HttpStatusCode.GatewayTimeout,
                            HttpStatusCode.ServiceUnavailable);
                        (await lockedResponse.Content.ReadAsByteArrayAsync()).Should().BeEmpty();
                        lockConnection.Execute("ROLLBACK");
                    }

                    using var afterLockResponse = await SendCapabilityAsync(client, port, lockedRecord.OperationId, lockedToken);
                    afterLockResponse.StatusCode.Should().Be(HttpStatusCode.OK);
                    (await afterLockResponse.Content.ReadAsStringAsync()).Should().Contain(lockedRecord.OperationId);

                    var databaseFailureRecord = repository.Save(CreateRecord(Guid.NewGuid().ToString("D")));
                    using (var connection = database.OpenConnection())
                    {
                        connection.Execute("CREATE TRIGGER fail_d8_redemption BEFORE INSERT ON \"D8TerminalCapabilityRedemptions\" BEGIN SELECT RAISE(ABORT, 'injected failure'); END");
                    }

                    var databaseFailureToken = CreateCapability(signingKey, databaseFailureRecord.OperationId, collector, now);
                    using var databaseFailureResponse = await SendCapabilityAsync(client, port, databaseFailureRecord.OperationId, databaseFailureToken);
                    databaseFailureResponse.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

                    using var administrativeRoute = await client.GetAsync($"https://127.0.0.1:{port}/api/v1/system/status");
                    administrativeRoute.StatusCode.Should().Be(HttpStatusCode.NotFound);

                    using var unapprovedHandler = CreateClientHandler(root, unapprovedCollector);
                    using var unapprovedClient = new System.Net.Http.HttpClient(unapprovedHandler);
                    using var rejectedRequest = new HttpRequestMessage(HttpMethod.Get,
                        $"https://127.0.0.1:{port}/v1/terminal-records/{operationId}");
                    rejectedRequest.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Capability", token);
                    var send = () => unapprovedClient.SendAsync(rejectedRequest);
                    await FluentActions.Awaiting(send).Should().ThrowAsync<HttpRequestException>();
                    ExceptionVerification.ExpectedWarns(3);
                }
                finally
                {
                    await listener.StopAsync(CancellationToken.None);
                }
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        private static TerminalEvidenceWorkerClient CreateWorkerClient(
            DisposableMainDatabase database,
            TestWorkerProcessLauncher launcher = null,
            ITerminalEvidenceWorkerProcessControl processControl = null)
        {
            var connectionFactory = new FixtureConnectionStringFactory(database.ConnectionString);
            var processLauncher = launcher ?? new TestWorkerProcessLauncher();
            return processControl == null
                ? new TerminalEvidenceWorkerClient(connectionFactory, processLauncher)
                : new TerminalEvidenceWorkerClient(connectionFactory, processLauncher, processControl);
        }

        private static Task<D8SearchJournalWorkerResponse> RunJournalCommand(
            TerminalEvidenceWorkerClient client,
            D8SearchJournalWorkerRequest request)
        {
            return client.RunJournalCommandAsync(request, TimeSpan.FromSeconds(8), CancellationToken.None);
        }

        private static bool IsProcessRunning(int processId)
        {
            try
            {
                using var process = Process.GetProcessById(processId);
                return !process.HasExited;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        private static void EnsureProcessStopped(int processId)
        {
            if (processId <= 0)
            {
                return;
            }

            try
            {
                using var process = Process.GetProcessById(processId);
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(3000).Should().BeTrue("disposable test workers must not leak when a regression assertion fails");
                }
            }
            catch (ArgumentException)
            {
                // The test already reaped the child.
            }
        }

        private sealed class FixtureConnectionStringFactory : IConnectionStringFactory
        {
            public FixtureConnectionStringFactory(string connectionString)
            {
                MainDbConnection = new DatabaseConnectionInfo(DatabaseType.SQLite, connectionString);
                LogDbConnection = MainDbConnection;
            }

            public DatabaseConnectionInfo MainDbConnection { get; }
            public DatabaseConnectionInfo LogDbConnection { get; }

            public string GetDatabasePath(string connectionString) => new SQLiteConnectionStringBuilder(connectionString).DataSource;
        }

        private sealed class TestWorkerProcessLauncher : ITerminalEvidenceWorkerProcessLauncher
        {
            private readonly int _crashLaunchNumber;
            private readonly Action<int> _onLaunch;
            private readonly int _hangSeconds;
            private bool _failFirstLaunch;
            private bool _crashFirstLaunch;
            private bool _hangFirstLaunch;
            private int _launchNumber;

            public TestWorkerProcessLauncher(
                bool failFirstLaunch = false,
                bool crashFirstLaunch = false,
                bool hangFirstLaunch = false,
                int crashLaunchNumber = 0,
                Action<int> onLaunch = null,
                int hangSeconds = 10)
            {
                _failFirstLaunch = failFirstLaunch;
                _crashFirstLaunch = crashFirstLaunch;
                _hangFirstLaunch = hangFirstLaunch;
                _crashLaunchNumber = crashLaunchNumber;
                _onLaunch = onLaunch;
                _hangSeconds = hangSeconds;
            }

            public ProcessStartInfo LastStartInfo { get; private set; }
            public TaskCompletionSource<bool> FirstStartInfoCreated { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public int LaunchCount => Volatile.Read(ref _launchNumber);

            public ProcessStartInfo CreateStartInfo(string entryPointArgument = null)
            {
                var launchNumber = Interlocked.Increment(ref _launchNumber);
                if (launchNumber == 1)
                {
                    FirstStartInfoCreated.TrySetResult(true);
                }

                _onLaunch?.Invoke(launchNumber);
                if (launchNumber == _crashLaunchNumber)
                {
                    return new ProcessStartInfo("/usr/bin/true")
                    {
                        UseShellExecute = false,
                        RedirectStandardInput = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    };
                }

                if (_failFirstLaunch)
                {
                    _failFirstLaunch = false;
                    throw new InvalidOperationException("Disposable worker startup failure");
                }

                if (_crashFirstLaunch)
                {
                    _crashFirstLaunch = false;
                    return new ProcessStartInfo("/usr/bin/true")
                    {
                        UseShellExecute = false,
                        RedirectStandardInput = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    };
                }

                if (_hangFirstLaunch)
                {
                    _hangFirstLaunch = false;
                    var waitInfo = new ProcessStartInfo("/usr/bin/sleep")
                    {
                        UseShellExecute = false,
                        RedirectStandardInput = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    };
                    waitInfo.ArgumentList.Add(_hangSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    return waitInfo;
                }

                var assemblyPath = Path.Combine(AppContext.BaseDirectory, "Prowlarr.dll");
                if (!File.Exists(assemblyPath))
                {
                    throw new FileNotFoundException("Referenced Prowlarr console assembly is missing", assemblyPath);
                }

                var startInfo = new ProcessStartInfo("dotnet")
                {
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                startInfo.Environment["D8_TEST_INHERITED_SECRET"] = "disposable-sentinel";
                startInfo.ArgumentList.Add(assemblyPath);
                startInfo.ArgumentList.Add(entryPointArgument ?? TerminalEvidenceWorker.EntryPointArgument);
                LastStartInfo = startInfo;
                return startInfo;
            }
        }

        private sealed class TestWorkerProcessControl : ITerminalEvidenceWorkerProcessControl
        {
            private readonly Action<Process> _onKill;
            private readonly Func<Process, CancellationToken, Task> _onWait;

            public TestWorkerProcessControl(
                Action<Process> onKill = null,
                Func<Process, CancellationToken, Task> onWait = null)
            {
                _onKill = onKill;
                _onWait = onWait;
            }

            public void Kill(Process worker)
            {
                if (_onKill != null)
                {
                    _onKill(worker);
                }
                else if (!worker.HasExited)
                {
                    worker.Kill(entireProcessTree: true);
                }
            }

            public Task WaitForExitAsync(Process worker, CancellationToken cancellationToken)
            {
                return _onWait?.Invoke(worker, cancellationToken) ?? worker.WaitForExitAsync(cancellationToken);
            }
        }

        private static HttpClientHandler CreateClientHandler(X509Certificate2 root, X509Certificate2 client)
        {
            var handler = new HttpClientHandler();
            handler.ClientCertificates.Add(client);
            handler.ServerCertificateCustomValidationCallback = (_, certificate, _, _) =>
            {
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(root);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return certificate != null && chain.Build(new X509Certificate2(certificate));
            };
            return handler;
        }

        private static async Task<HttpResponseMessage> SendCapabilityAsync(System.Net.Http.HttpClient client, int port, string operationId, string token)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get,
                $"https://127.0.0.1:{port}/v1/terminal-records/{operationId}");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Capability", token);
            return await client.SendAsync(request);
        }

        private static X509Certificate2 CreateRootCertificate()
        {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=D8 Disposable Test Root", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
            return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(2));
        }

        private static X509Certificate2 CreateLeafCertificate(
            X509Certificate2 root,
            string name,
            bool clientAuthentication,
            IPAddress address = null,
            string responderPodUid = null,
            string uriSan = null)
        {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=" + name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            var usages = new OidCollection
            {
                new Oid(clientAuthentication ? "1.3.6.1.5.5.7.3.2" : "1.3.6.1.5.5.7.3.1")
            };
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(usages, true));
            if (address != null || responderPodUid != null || !string.IsNullOrWhiteSpace(uriSan))
            {
                var names = new SubjectAlternativeNameBuilder();
                if (address != null)
                {
                    names.AddIpAddress(address);
                    names.AddDnsName("localhost");
                }

                if (responderPodUid != null)
                {
                    names.AddUri(new Uri($"spiffe://cluster.local/ns/media/pod/{responderPodUid}"));
                }

                if (!string.IsNullOrWhiteSpace(uriSan))
                {
                    names.AddUri(new Uri(uriSan));
                }

                request.CertificateExtensions.Add(names.Build());
            }

            using var issued = request.Create(root, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1), RandomNumberGenerator.GetBytes(16));
            var withKey = issued.CopyWithPrivateKey(key);
            return new X509Certificate2(withKey.Export(X509ContentType.Pfx, PfxPassword), PfxPassword, X509KeyStorageFlags.EphemeralKeySet);
        }

        private static async Task AssertCollectorRejectsMissingObserverAsync(object configuration)
        {
            var collectorRoot = Environment.GetEnvironmentVariable("D8F_COLLECTOR_ROOT");
            if (string.IsNullOrWhiteSpace(collectorRoot))
            {
                Assert.Ignore("Cross-repository producer/collector test requires D8F_COLLECTOR_ROOT");
            }

            var helper = Path.Combine(collectorRoot, "tools", "python", "tests", "d8f_producer_collector_interop.py");
            if (!File.Exists(helper))
            {
                Assert.Ignore("D8F_COLLECTOR_ROOT does not contain the collector interop helper");
            }

            var configPath = Path.Combine(Path.GetTempPath(), "d8-interop-" + Guid.NewGuid().ToString("N") + ".json");
            await File.WriteAllTextAsync(configPath, JsonSerializer.Serialize(configuration)).ConfigureAwait(false);
            try
            {
                var startInfo = new ProcessStartInfo(Environment.GetEnvironmentVariable("PYTHON") ?? "python3")
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    WorkingDirectory = collectorRoot
                };
                startInfo.ArgumentList.Add(helper);
                startInfo.ArgumentList.Add("--config");
                startInfo.ArgumentList.Add(configPath);
                startInfo.Environment["PYTHONPATH"] = Path.Combine(collectorRoot, "tools", "python", "src");
                using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Collector interop process did not start");
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var stdout = process.StandardOutput.ReadToEndAsync();
                var stderr = process.StandardError.ReadToEndAsync();
                try
                {
                    await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    process.Kill(true);
                    await process.WaitForExitAsync().ConfigureAwait(false);
                    throw new TimeoutException("Collector interop test exceeded its deadline");
                }

                var output = await stdout.ConfigureAwait(false);
                var error = await stderr.ConfigureAwait(false);
                process.ExitCode.Should().Be(2, "missing independent observer proof must fail closed; helper reported {0}", error.Trim());
                error.Should().Contain("ProvenanceError: independent process-observer receipt and protected verifier policy are required");
                output.Should().NotContain("PASS receipt=");
            }
            finally
            {
                File.Delete(configPath);
            }
        }

        private void SetUpObservedSearch()
        {
            _transport = new FakeResponseHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    ReadAllText("Files/Indexers/Newznab/newznab_nzb_su.xml"),
                    Encoding.UTF8,
                    "application/rss+xml")
            });
            var dispatcher = new FakeManagedHttpDispatcher(
                Mocker.GetMock<IHttpProxySettingsProvider>().Object,
                Mocker.GetMock<ICreateManagedWebProxy>().Object,
                Mocker.GetMock<ICertificateValidationService>().Object,
                Mocker.Resolve<UserAgentBuilder>(),
                Mocker.Resolve<CacheManager>(),
                TestLogger,
                _transport);
            Mocker.SetConstant<IHttpDispatcher>(dispatcher);
            Mocker.SetConstant<IIndexerHttpClient>(new IndexerHttpClient(
                Mocker.GetMock<IIndexerProxyFactory>().Object,
                Array.Empty<IHttpRequestInterceptor>(),
                Mocker.Resolve<CacheManager>(),
                Mocker.Resolve<IRateLimitService>(),
                dispatcher,
                TestLogger));

            _indexer = Mocker.Resolve<Newznab>();
            _indexer.Definition = new IndexerDefinition
            {
                Id = 42,
                Name = "D8 integration Newznab",
                Enable = true,
                Implementation = nameof(Newznab),
                Settings = new NewznabSettings { BaseUrl = "https://indexer.example", ApiPath = "/api" }
            };
            var capabilities = new IndexerCapabilities { LimitsDefault = 100 };
            capabilities.Categories.AddCategoryMapping(5000, NewznabStandardCategory.TV, "TV");
            capabilities.Categories.AddCategoryMapping(5040, NewznabStandardCategory.TVHD, "TV/HD");
            Mocker.GetMock<INewznabCapabilitiesProvider>()
                .Setup(provider => provider.GetCapabilities(
                    It.IsAny<NewznabSettings>(),
                    It.IsAny<NzbDrone.Core.ThingiProvider.ProviderDefinition>()))
                .Returns(capabilities);
            Mocker.GetMock<IIndexerFactory>()
                .Setup(factory => factory.Enabled(It.IsAny<bool>()))
                .Returns(new List<IIndexer> { _indexer });
            Mocker.GetMock<IIndexerLimitService>()
                .Setup(service => service.AtQueryLimit(It.IsAny<IndexerDefinition>()))
                .Returns(false);
        }

        private static void RestrictPrivateFile(string path)
        {
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
        }

        private static void CreatePrivateDirectory(string path)
        {
            if (!OperatingSystem.IsWindows())
            {
                Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            else
            {
                Directory.CreateDirectory(path);
            }
        }

        private static IndexerSearchTerminalRecord CreateRecord(string operationId)
        {
            var startedAt = DateTimeOffset.UtcNow.AddSeconds(-2);
            var completedAt = DateTimeOffset.UtcNow.AddSeconds(-1);
            return new IndexerSearchTerminalRecord
            {
                OperationId = operationId,
                SchemaVersion = IndexerSearchTerminalContract.SchemaVersion,
                ApplicationVersion = "3.0.0-test",
                ProcessInstanceId = Guid.NewGuid().ToString("D"),
                RequestCorrelationId = CorrelationId,
                IndexerId = 42,
                OperationType = IndexerSearchTerminalContract.OperationType,
                StartedAt = startedAt,
                EndedAt = DateTimeOffset.UtcNow,
                Outcome = "SUCCEEDED",
                ResultCount = 1,
                Attempts = new[]
                {
                    new ObservedAttempt(Guid.NewGuid(), "GET", "indexer.example.net", 200, null, startedAt, completedAt)
                },
                CommitAcknowledged = true
            };
        }

        private static void InitializeDatabase(DisposableMainDatabase database)
        {
            using var connection = database.OpenConnection();
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
                    "FailureReason" varchar(128),
                    "ResultCount" integer NOT NULL,
                    "AttemptsJson" varchar(65536) NOT NULL
                );
                CREATE INDEX "IX_IndexerSearchTerminalRecords_EndedAtUnixMs" ON "IndexerSearchTerminalRecords" ("EndedAtUnixMs");
                CREATE TABLE "D8TerminalCapabilityRedemptions" (
                    "TokenId" varchar(36) PRIMARY KEY,
                    "ExpiresAtUnixMs" bigint NOT NULL
                );
                CREATE INDEX "IX_D8TerminalCapabilityRedemptions_ExpiresAtUnixMs" ON "D8TerminalCapabilityRedemptions" ("ExpiresAtUnixMs");
                CREATE TABLE "D8SearchOperations" (
                    "OperationId" varchar(36) PRIMARY KEY,
                    "CallerIdentity" varchar(256) NOT NULL,
                    "IdempotencyKey" varchar(128) NOT NULL,
                    "CorrelationId" varchar(64) NOT NULL,
                    "IndexerId" integer NOT NULL,
                    "ProcessInstanceId" varchar(36) NOT NULL,
                    "StartedAtUnixMs" bigint NOT NULL,
                    "EndedAtUnixMs" bigint NULL,
                    "State" varchar(16) NOT NULL,
                    "FailureReason" varchar(128) NULL,
                    "ResultCount" integer NOT NULL,
                    "AttemptsJson" varchar(65536) NOT NULL
                );
                CREATE UNIQUE INDEX "IX_D8SearchOperations_Caller_Idempotency" ON "D8SearchOperations" ("CallerIdentity", "IdempotencyKey");
                CREATE INDEX "IX_D8SearchOperations_State_StartedAt" ON "D8SearchOperations" ("State", "StartedAtUnixMs");
                CREATE TABLE "D8SearchAttemptProgress" (
                    "OperationId" varchar(36) NOT NULL,
                    "AttemptId" varchar(36) NOT NULL,
                    "Stage" varchar(24) NOT NULL,
                    "AttemptJson" varchar(4096) NOT NULL,
                    PRIMARY KEY ("OperationId", "AttemptId")
                );
                """);
        }

        private sealed class DisposableMainDatabase : IMainDatabase
        {
            private readonly string _connectionString;

            public DisposableMainDatabase(string path)
            {
                _connectionString = new SQLiteConnectionStringBuilder
                {
                    DataSource = path,
                    Version = 3,
                    JournalMode = SQLiteJournalModeEnum.Wal,
                    Pooling = false,
                    BusyTimeout = 100
                }.ConnectionString;
            }

            public string ConnectionString => _connectionString;

            public IDbConnection OpenConnection()
            {
                var connection = new SQLiteConnection(_connectionString);
                connection.Open();
                connection.Execute("PRAGMA synchronous=FULL");
                return connection;
            }

            public Version Version => new(3, 50, 4);
            public int Migration => 46;
            public DatabaseType DatabaseType => DatabaseType.SQLite;
            public void Vacuum()
            {
            }
        }

        private sealed class InitiationRow
        {
            public string OperationId { get; set; }
            public string CallerIdentity { get; set; }
            public string CorrelationId { get; set; }
            public string State { get; set; }
            public string AttemptsJson { get; set; }
        }

        private sealed class FakeResponseHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, HttpResponseMessage> _responseFactory;
            private int _requestCount;

            public FakeResponseHandler(Func<HttpRequestMessage, HttpResponseMessage> responseFactory)
            {
                _responseFactory = responseFactory;
            }

            public int RequestCount => Volatile.Read(ref _requestCount);
            public bool BlockUntilCancelled { get; set; }

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _requestCount);
                if (BlockUntilCancelled)
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await Task.Yield();
                }

                var response = _responseFactory(request);
                response.RequestMessage = request;
                return response;
            }
        }

        private sealed class FakeManagedHttpDispatcher : ManagedHttpDispatcher
        {
            private readonly System.Net.Http.HttpClient _client;

            public FakeManagedHttpDispatcher(
                IHttpProxySettingsProvider proxySettingsProvider,
                ICreateManagedWebProxy createManagedWebProxy,
                ICertificateValidationService certificateValidationService,
                IUserAgentBuilder userAgentBuilder,
                ICacheManager cacheManager,
                NLog.Logger logger,
                HttpMessageHandler handler)
                : base(
                    proxySettingsProvider,
                    createManagedWebProxy,
                    certificateValidationService,
                    userAgentBuilder,
                    cacheManager,
                    logger)
            {
                _client = new System.Net.Http.HttpClient(handler, false)
                {
                    Timeout = Timeout.InfiniteTimeSpan
                };
            }

            protected override System.Net.Http.HttpClient GetClient(
                HttpUri uri,
                HttpProxySettings requestProxy,
                bool observed = false)
            {
                requestProxy.Should().BeNull("the integration exercise uses the direct bounded path");
                observed.Should().BeTrue("the producer record must come from SearchObserved");
                return _client;
            }
        }

        private static string CreateCapability(
            RSA key,
            string operationId,
            X509Certificate2 certificate,
            DateTimeOffset now,
            string correlationId = CorrelationId)
        {
            var header = JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT", kid = KeyId });
            var payload = JsonSerializer.SerializeToUtf8Bytes(new
            {
                iss = Issuer,
                aud = Audience,
                scope = "terminal-record:read",
                operation_id = operationId,
                correlation_id = correlationId,
                certificate_sha256 = Convert.ToHexString(SHA256.HashData(certificate.RawData)).ToLowerInvariant(),
                jti = Guid.NewGuid().ToString("D"),
                iat = now.ToUnixTimeSeconds(),
                exp = now.AddSeconds(30).ToUnixTimeSeconds()
            });
            var input = Encode(header) + "." + Encode(payload);
            var signature = key.SignData(Encoding.ASCII.GetBytes(input), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            return input + "." + Encode(signature);
        }

        private static string Encode(byte[] value)
        {
            return Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        }

        private static int FindAvailablePort()
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }

        private sealed class FixtureRecordRepository : IIndexerSearchTerminalRecordRepository
        {
            private readonly IndexerSearchTerminalRecord _record;

            public FixtureRecordRepository(IndexerSearchTerminalRecord record)
            {
                _record = record;
            }

            public IndexerSearchTerminalRecord Save(IndexerSearchTerminalRecord record) => throw new NotSupportedException();

            public IndexerSearchTerminalRecord Get(string operationId) => operationId == _record.OperationId ? _record : null;

            public IndexerSearchTerminalRecord Get(string operationId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Get(operationId);
            }

            public Task<IndexerSearchTerminalRecord> GetAsync(string operationId, CancellationToken cancellationToken)
            {
                return Task.FromResult(Get(operationId, cancellationToken));
            }
        }

        private sealed class FixtureReplayStore : ITerminalEvidenceCapabilityReplayStore
        {
            private readonly ConcurrentDictionary<string, byte> _redeemed = new();

            public bool TryRedeem(string tokenId, long expiresAtUnixMs)
            {
                return expiresAtUnixMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() && _redeemed.TryAdd(tokenId, 0);
            }

            public bool TryRedeem(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return TryRedeem(tokenId, expiresAtUnixMs);
            }

            public Task<bool> TryRedeemAsync(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken)
            {
                return Task.FromResult(TryRedeem(tokenId, expiresAtUnixMs, cancellationToken));
            }
        }
    }
}
