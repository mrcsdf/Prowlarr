using FluentMigrator;
using NzbDrone.Core.Datastore.Migration.Framework;

namespace NzbDrone.Core.Datastore.Migration
{
    [Migration(046)]
    public class d8_terminal_capability_redemptions : NzbDroneMigrationBase
    {
        protected override void MainDbUpgrade()
        {
            Create.Table("D8TerminalCapabilityRedemptions")
                .WithColumn("TokenId").AsString(36).PrimaryKey()
                .WithColumn("ExpiresAtUnixMs").AsInt64().NotNullable();
            Create.Index("IX_D8TerminalCapabilityRedemptions_ExpiresAtUnixMs")
                .OnTable("D8TerminalCapabilityRedemptions").OnColumn("ExpiresAtUnixMs").Ascending();
        }
    }
}
