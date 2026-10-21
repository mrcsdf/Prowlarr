using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Dapper;
using FluentMigrator;
using Newtonsoft.Json.Linq;
using Npgsql;
using NzbDrone.Common.Serializer;
using NzbDrone.Core.Datastore.Migration.Framework;

namespace NzbDrone.Core.Datastore.Migration
{
    // 045 is the first unused migration on the frozen integration baseline
    // (Prowlarr develop 26a605e41f6156b959bc8f3ae5db4bec252106c5).
    [Migration(045)]
    public class indexer_search_terminal_records : NzbDroneMigrationBase
    {
        private const string TableName = "IndexerSearchTerminalRecords";
        private const string ExpirationIndex = "IX_IndexerSearchTerminalRecords_EndedAtUnixMs";

        private static readonly PrototypeColumn[] PrototypeColumns =
        {
            new("OperationId", "string", 36, false, true),
            new("SchemaVersion", "string", 64, false),
            new("ApplicationVersion", "string", 64, false),
            new("ProcessInstanceId", "string", 36, false),
            new("RequestCorrelationId", "string", 64, false),
            new("IndexerId", "integer", null, false),
            new("OperationType", "string", 64, false),
            new("StartedAtUnixMs", "bigint", null, false),
            new("EndedAtUnixMs", "bigint", null, false),
            new("Outcome", "string", 16, false),
            new("FailureReason", "string", 128, true),
            new("ResultCount", "integer", null, false),
            new("AttemptsJson", "string", 65536, false)
        };

        protected override void MainDbUpgrade()
        {
            var tableExists = Schema.Table(TableName).Exists();
            if (tableExists)
            {
                Execute.WithConnection((connection, transaction) =>
                {
                    InspectPrototypeSchema(connection, transaction);
                    var compatibleIndexExists = InspectExpirationIndex(connection, transaction);
                    if (!compatibleIndexExists)
                    {
                        connection.Execute("CREATE INDEX \"IX_IndexerSearchTerminalRecords_EndedAtUnixMs\" ON \"IndexerSearchTerminalRecords\" (\"EndedAtUnixMs\" ASC)", transaction: transaction);
                    }

                    MigrateIndexersToWedgeCheckbox(connection, transaction);
                });

                return;
            }

            // A prototype may have recorded migration 044 but lost/never created
            // its table. Reapply upstream 044 on every 045 path: the conversion
            // only changes integer values and is idempotent for converted rows.
            Execute.WithConnection(MigrateIndexersToWedgeCheckbox);

            Create.Table(TableName)
                .WithColumn("OperationId").AsString(36).PrimaryKey()
                .WithColumn("SchemaVersion").AsString(64).NotNullable()
                .WithColumn("ApplicationVersion").AsString(64).NotNullable()
                .WithColumn("ProcessInstanceId").AsString(36).NotNullable()
                .WithColumn("RequestCorrelationId").AsString(64).NotNullable()
                .WithColumn("IndexerId").AsInt32().NotNullable()
                .WithColumn("OperationType").AsString(64).NotNullable()
                .WithColumn("StartedAtUnixMs").AsInt64().NotNullable()
                .WithColumn("EndedAtUnixMs").AsInt64().NotNullable()
                .WithColumn("Outcome").AsString(16).NotNullable()
                .WithColumn("FailureReason").AsString(128).Nullable()
                .WithColumn("ResultCount").AsInt32().NotNullable()
                .WithColumn("AttemptsJson").AsString(65536).NotNullable();
            Create.Index(ExpirationIndex)
                .OnTable(TableName).OnColumn("EndedAtUnixMs").Ascending();
        }

        private static void InspectPrototypeSchema(IDbConnection connection, IDbTransaction transaction)
        {
            if (connection is SQLiteConnection)
            {
                var columns = connection.Query("PRAGMA table_info('IndexerSearchTerminalRecords')", transaction: transaction)
                    .Cast<IDictionary<string, object>>()
                    .ToDictionary(column => column["name"].ToString(), System.StringComparer.Ordinal);
                if (columns.Count != PrototypeColumns.Length)
                {
                    throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                }

                foreach (var expected in PrototypeColumns)
                {
                    if (!columns.TryGetValue(expected.Name, out var actual))
                    {
                        throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                    }

                    var type = actual["type"]?.ToString() ?? string.Empty;
                    var typeMatches = expected.Type == "integer"
                        ? type.Contains("int", System.StringComparison.OrdinalIgnoreCase)
                        : expected.Type == "bigint"
                            ? type.Contains("int", System.StringComparison.OrdinalIgnoreCase)
                            : type.Contains("char", System.StringComparison.OrdinalIgnoreCase) || type.Contains("text", System.StringComparison.OrdinalIgnoreCase) || type.Contains("clob", System.StringComparison.OrdinalIgnoreCase);
                    var notNull = Convert.ToInt32(actual["notnull"], CultureInfo.InvariantCulture) != 0;
                    var primaryKey = Convert.ToInt32(actual["pk"], CultureInfo.InvariantCulture) != 0;
                    if (!typeMatches || notNull != !expected.Nullable || primaryKey != expected.PrimaryKey)
                    {
                        throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                    }
                }

                return;
            }

            if (connection is NpgsqlConnection)
            {
                var columns = connection.Query("""
                    SELECT column_name AS "Name", data_type AS "DataType", is_nullable AS "IsNullable", character_maximum_length AS "CharacterMaximumLength"
                    FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = @TableName
                    """,
                    new { TableName },
                    transaction)
                    .Cast<IDictionary<string, object>>()
                    .ToDictionary(column => column["Name"].ToString(), System.StringComparer.Ordinal);
                var primaryKeyColumns = connection.Query<string>("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = current_schema() AND tc.table_name = @TableName AND tc.constraint_type = 'PRIMARY KEY'
                    """,
                    new { TableName },
                    transaction).ToArray();
                if (columns.Count != PrototypeColumns.Length || primaryKeyColumns.Length != 1 || primaryKeyColumns[0] != "OperationId")
                {
                    throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                }

                foreach (var expected in PrototypeColumns)
                {
                    if (!columns.TryGetValue(expected.Name, out var actual))
                    {
                        throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                    }

                    var dataType = actual["DataType"]?.ToString();
                    var isNullable = actual["IsNullable"]?.ToString() == "YES";
                    var typeMatches = expected.Type switch
                    {
                        "string" => dataType == "character varying" && Convert.ToInt32(actual["CharacterMaximumLength"], CultureInfo.InvariantCulture) == expected.Length,
                        "integer" => dataType == "integer",
                        "bigint" => dataType == "bigint",
                        _ => false
                    };
                    if (!typeMatches || isNullable != expected.Nullable)
                    {
                        throw new System.InvalidOperationException("Migration 045 found an incompatible pre-existing terminal evidence table");
                    }
                }

                return;
            }

            throw new System.NotSupportedException("Migration 045 cannot verify terminal evidence table schema for this database provider");
        }

        private bool InspectExpirationIndex(IDbConnection connection, IDbTransaction transaction)
        {
            if (connection is SQLiteConnection)
            {
                var index = connection.Query("PRAGMA index_list('IndexerSearchTerminalRecords')", transaction: transaction)
                    .Cast<IDictionary<string, object>>()
                    .SingleOrDefault(row => string.Equals(row["name"]?.ToString(), ExpirationIndex, System.StringComparison.Ordinal));
                if (index == null)
                {
                    return false;
                }

                var columns = connection.Query($"PRAGMA index_xinfo('{ExpirationIndex}')", transaction: transaction)
                    .Cast<IDictionary<string, object>>()
                    .Where(column => Convert.ToInt32(column["key"], CultureInfo.InvariantCulture) == 1)
                    .ToArray();
                if (Convert.ToInt32(index["unique"], CultureInfo.InvariantCulture) != 0 ||
                    Convert.ToInt32(index["partial"], CultureInfo.InvariantCulture) != 0 || columns.Length != 1 ||
                    columns[0]["name"]?.ToString() != "EndedAtUnixMs" ||
                    Convert.ToInt32(columns[0]["desc"], CultureInfo.InvariantCulture) != 0)
                {
                    throw new System.InvalidOperationException("Migration 045 found an incompatible terminal evidence expiration index");
                }

                return true;
            }

            if (connection is NpgsqlConnection)
            {
                var definition = connection.QueryFirstOrDefault<string>("""
                    SELECT indexdef FROM pg_indexes
                    WHERE schemaname = current_schema() AND tablename = @TableName AND indexname = @IndexName
                    """,
                    new { TableName, IndexName = ExpirationIndex },
                    transaction);
                if (definition == null)
                {
                    return false;
                }

                var normalized = Regex.Replace(definition.Replace("\"", string.Empty), @"\s+", " ").Trim().ToLowerInvariant();
                if (!Regex.IsMatch(normalized,
                    @"^create index ix_indexersearchterminalrecords_endedatunixms on (?:[a-z0-9_]+\.)?indexersearchterminalrecords using btree \(endedatunixms(?: asc)?\)$",
                    RegexOptions.CultureInvariant))
                {
                    throw new System.InvalidOperationException("Migration 045 found an incompatible terminal evidence expiration index");
                }

                return true;
            }

            throw new System.NotSupportedException("Migration 045 cannot verify the terminal evidence expiration index for this database provider");
        }

        private void MigrateIndexersToWedgeCheckbox(IDbConnection conn, IDbTransaction tran)
        {
            var updated = new List<object>();

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = "SELECT \"Id\", \"Settings\" FROM \"Indexers\" WHERE \"Implementation\" = 'MyAnonamouse'";

                using var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var id = reader.GetInt32(0);
                    var settings = Json.Deserialize<JObject>(reader.GetString(1));
                    if (settings.ContainsKey("useFreeleechWedge") && settings.Value<JToken>("useFreeleechWedge").Type == JTokenType.Integer)
                    {
                        var optionValue = settings.Value<int>("useFreeleechWedge") switch
                        {
                            1 or 2 => true,
                            _ => false,
                        };

                        settings.Remove("useFreeleechWedge");
                        settings.Add("useFreeleechWedge", optionValue);
                    }

                    updated.Add(new { Id = id, Settings = settings.ToJson() });
                }
            }

            conn.Execute("UPDATE \"Indexers\" SET \"Settings\" = @Settings WHERE \"Id\" = @Id", updated, transaction: tran);
        }

        private sealed record PrototypeColumn(string Name, string Type, int? Length, bool Nullable, bool PrimaryKey = false);
    }
}
