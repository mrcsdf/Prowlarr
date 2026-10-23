using FluentMigrator;
using NzbDrone.Core.Datastore.Migration.Framework;

namespace NzbDrone.Core.Datastore.Migration
{
    [Migration(047)]
    public class d8_search_operations : NzbDroneMigrationBase
    {
        protected override void MainDbUpgrade()
        {
            Create.Table("D8SearchOperations")
                .WithColumn("OperationId").AsString(36).PrimaryKey()
                .WithColumn("CallerIdentity").AsString(256).NotNullable()
                .WithColumn("IdempotencyKey").AsString(128).NotNullable()
                .WithColumn("CorrelationId").AsString(64).NotNullable()
                .WithColumn("IndexerId").AsInt32().NotNullable()
                .WithColumn("ProcessInstanceId").AsString(36).NotNullable()
                .WithColumn("StartedAtUnixMs").AsInt64().NotNullable()
                .WithColumn("EndedAtUnixMs").AsInt64().Nullable()
                .WithColumn("State").AsString(16).NotNullable()
                .WithColumn("FailureReason").AsString(128).Nullable()
                .WithColumn("ResultCount").AsInt32().NotNullable()
                .WithColumn("AttemptsJson").AsString(65536).NotNullable();

            Create.Index("IX_D8SearchOperations_Caller_Idempotency")
                .OnTable("D8SearchOperations")
                .OnColumn("CallerIdentity").Ascending()
                .OnColumn("IdempotencyKey").Ascending()
                .WithOptions().Unique();

            Create.Index("IX_D8SearchOperations_State_StartedAt")
                .OnTable("D8SearchOperations")
                .OnColumn("State").Ascending()
                .OnColumn("StartedAtUnixMs").Ascending();

            Create.Table("D8SearchAttemptProgress")
                .WithColumn("OperationId").AsString(36).NotNullable()
                .WithColumn("AttemptId").AsString(36).NotNullable()
                .WithColumn("Stage").AsString(24).NotNullable()
                .WithColumn("AttemptJson").AsString(4096).NotNullable();

            Create.Index("IX_D8SearchAttemptProgress_Operation_Attempt")
                .OnTable("D8SearchAttemptProgress")
                .OnColumn("OperationId").Ascending()
                .OnColumn("AttemptId").Ascending()
                .WithOptions().Unique();
        }
    }
}
