using System;
using System.Data;
using System.Data.SQLite;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using NzbDrone.Core.Datastore;

namespace NzbDrone.Core.IndexerSearch
{
    public sealed class TerminalEvidenceCapabilityReplayStore : ITerminalEvidenceCapabilityReplayStore
    {
        private readonly IMainDatabase _database;
        private readonly ITerminalEvidenceSqliteConnectionFactory _sqliteConnections;

        public TerminalEvidenceCapabilityReplayStore(
            IMainDatabase database,
            ITerminalEvidenceSqliteConnectionFactory sqliteConnections)
        {
            _database = database;
            _sqliteConnections = sqliteConnections;
        }

        public bool TryRedeem(string tokenId, long expiresAtUnixMs)
        {
            return TryRedeem(tokenId, expiresAtUnixMs, CancellationToken.None);
        }

        public bool TryRedeem(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Guid.TryParseExact(tokenId, "D", out _) || expiresAtUnixMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
            {
                return false;
            }

            if (_database.DatabaseType != DatabaseType.SQLite)
            {
                return false;
            }

            IDbConnection openedConnection;
            try
            {
                openedConnection = OpenConnection(cancellationToken);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (SQLiteException exception)
            {
                throw new D8StorageException(D8SearchJournalFailure.FromSqliteError(exception.ResultCode));
            }

            cancellationToken.ThrowIfCancellationRequested();
            using var connection = openedConnection;
            using var interruption = connection is SQLiteConnection sqliteConnection
                ? cancellationToken.Register(() => sqliteConnection.Cancel())
                : default;
            try
            {
            if (!string.Equals(connection.ExecuteScalar<string>("PRAGMA journal_mode"), "wal", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            connection.Execute("PRAGMA synchronous=FULL");
            if (connection.ExecuteScalar<int>("PRAGMA synchronous") != 2)
            {
                return false;
            }

            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            connection.Execute(
                "DELETE FROM \"D8TerminalCapabilityRedemptions\" WHERE \"ExpiresAtUnixMs\" <= @Now",
                new { Now = now },
                transaction);
            if (connection.ExecuteScalar<int>(
                    "SELECT COUNT(*) FROM \"D8TerminalCapabilityRedemptions\" WHERE \"TokenId\" = @TokenId",
                    new { TokenId = tokenId },
                    transaction) != 0)
            {
                transaction.Commit();
                return false;
            }

            connection.Execute(
                """
                INSERT INTO "D8TerminalCapabilityRedemptions" ("TokenId", "ExpiresAtUnixMs")
                VALUES (@TokenId, @ExpiresAtUnixMs)
                ON CONFLICT ("TokenId") DO NOTHING
                """,
                new { TokenId = tokenId, ExpiresAtUnixMs = expiresAtUnixMs },
                transaction);
            var inserted = connection.ExecuteScalar<int>(
                "SELECT COUNT(*) FROM \"D8TerminalCapabilityRedemptions\" WHERE \"TokenId\" = @TokenId AND \"ExpiresAtUnixMs\" = @ExpiresAtUnixMs",
                new { TokenId = tokenId, ExpiresAtUnixMs = expiresAtUnixMs },
                transaction);
            transaction.Commit();
            return inserted == 1;
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (SQLiteException exception)
            {
                throw new D8StorageException(D8SearchJournalFailure.FromSqliteError(exception.ResultCode));
            }
        }

        public async Task<bool> TryRedeemAsync(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!Guid.TryParseExact(tokenId, "D", out _) || expiresAtUnixMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() ||
                _database.DatabaseType != DatabaseType.SQLite)
            {
                return false;
            }

            IDbConnection openedConnection;
            try
            {
                openedConnection = OpenConnection(cancellationToken);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (SQLiteException exception)
            {
                throw new D8StorageException(D8SearchJournalFailure.FromSqliteError(exception.ResultCode));
            }

            cancellationToken.ThrowIfCancellationRequested();
            using var connection = openedConnection;
            using var interruption = connection is SQLiteConnection sqliteConnection
                ? cancellationToken.Register(() => sqliteConnection.Cancel())
                : default;
            try
            {
                if (!string.Equals(
                    await connection.ExecuteScalarAsync<string>(
                        new CommandDefinition("PRAGMA journal_mode", cancellationToken: cancellationToken)).ConfigureAwait(false),
                    "wal",
                    StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                await connection.ExecuteAsync(new CommandDefinition(
                    "PRAGMA synchronous=FULL", cancellationToken: cancellationToken)).ConfigureAwait(false);
                if (await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                        "PRAGMA synchronous", cancellationToken: cancellationToken)).ConfigureAwait(false) != 2)
                {
                    return false;
                }

                var transaction = await ((SQLiteConnection)connection).BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await using (transaction.ConfigureAwait(false))
                {
                    var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    await connection.ExecuteAsync(new CommandDefinition(
                        "DELETE FROM \"D8TerminalCapabilityRedemptions\" WHERE \"ExpiresAtUnixMs\" <= @Now",
                        new { Now = now },
                        transaction,
                        cancellationToken: cancellationToken)).ConfigureAwait(false);
                    var alreadyRedeemed = await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                        "SELECT COUNT(*) FROM \"D8TerminalCapabilityRedemptions\" WHERE \"TokenId\" = @TokenId",
                        new { TokenId = tokenId },
                        transaction,
                        cancellationToken: cancellationToken)).ConfigureAwait(false);
                    if (alreadyRedeemed != 0)
                    {
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        return false;
                    }

                    await connection.ExecuteAsync(new CommandDefinition("""
                        INSERT INTO "D8TerminalCapabilityRedemptions" ("TokenId", "ExpiresAtUnixMs")
                        VALUES (@TokenId, @ExpiresAtUnixMs)
                        ON CONFLICT ("TokenId") DO NOTHING
                        """,
                        new { TokenId = tokenId, ExpiresAtUnixMs = expiresAtUnixMs },
                        transaction,
                        cancellationToken: cancellationToken)).ConfigureAwait(false);
                    var inserted = await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                        "SELECT COUNT(*) FROM \"D8TerminalCapabilityRedemptions\" WHERE \"TokenId\" = @TokenId AND \"ExpiresAtUnixMs\" = @ExpiresAtUnixMs",
                        new { TokenId = tokenId, ExpiresAtUnixMs = expiresAtUnixMs },
                        transaction,
                        cancellationToken: cancellationToken)).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return inserted == 1;
                }
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (SQLiteException exception)
            {
                throw new D8StorageException(D8SearchJournalFailure.FromSqliteError(exception.ResultCode));
            }
        }

        private IDbConnection OpenConnection(CancellationToken cancellationToken)
        {
            if (_sqliteConnections != null && _database.DatabaseType == DatabaseType.SQLite)
            {
                return _sqliteConnections.OpenConnection(cancellationToken);
            }

            return _database.OpenConnection();
        }
    }
}
