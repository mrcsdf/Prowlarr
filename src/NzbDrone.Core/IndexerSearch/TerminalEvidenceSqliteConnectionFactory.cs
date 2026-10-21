using System;
using System.Data.SQLite;
using System.Threading;
using NzbDrone.Core.Datastore;

namespace NzbDrone.Core.IndexerSearch
{
    public interface ITerminalEvidenceSqliteConnectionFactory
    {
        SQLiteConnection OpenConnection(CancellationToken cancellationToken);
    }

    public sealed class TerminalEvidenceSqliteConnectionFactory : ITerminalEvidenceSqliteConnectionFactory
    {
        private const int OpenBusyTimeoutMilliseconds = 100;
        private const int PostOpenBusyTimeoutMilliseconds = 1000;
        private readonly IConnectionStringFactory _connectionStringFactory;

        public TerminalEvidenceSqliteConnectionFactory(IConnectionStringFactory connectionStringFactory)
        {
            _connectionStringFactory = connectionStringFactory;
        }

        public SQLiteConnection OpenConnection(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var connectionInfo = _connectionStringFactory.MainDbConnection;
            if (connectionInfo.DatabaseType != DatabaseType.SQLite)
            {
                throw new NotSupportedException("Terminal evidence SQLite access requires the SQLite backend");
            }

            var builder = new SQLiteConnectionStringBuilder(connectionInfo.ConnectionString)
            {
                Pooling = false,
                BusyTimeout = OpenBusyTimeoutMilliseconds,
                DefaultTimeout = 1
            };
            var connection = new SQLiteConnection(builder.ConnectionString);
            try
            {
                // System.Data.SQLite's synchronous Open is not cancellation-aware. Keep its
                // native lock wait at 100 ms before Open begins; post-open commands use a
                // 1000 ms SQLite busy handler so cancellation can interrupt contention. The
                // one-second provider DefaultTimeout is a separate command timeout, not an
                // extension of Open's bound. Process isolation supplies the outer hard deadline
                // for listener use.
                connection.Open();
                cancellationToken.ThrowIfCancellationRequested();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "PRAGMA busy_timeout=" + PostOpenBusyTimeoutMilliseconds;
                    command.ExecuteNonQuery();
                }

                cancellationToken.ThrowIfCancellationRequested();
                return connection;
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                connection.Dispose();
                throw new OperationCanceledException(cancellationToken);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }
    }
}
