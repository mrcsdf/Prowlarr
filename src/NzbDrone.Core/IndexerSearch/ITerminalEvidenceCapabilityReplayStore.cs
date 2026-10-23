using System.Threading;
using System.Threading.Tasks;

namespace NzbDrone.Core.IndexerSearch
{
    public interface ITerminalEvidenceCapabilityReplayStore
    {
        bool TryRedeem(string tokenId, long expiresAtUnixMs);
        bool TryRedeem(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken);
        Task<bool> TryRedeemAsync(string tokenId, long expiresAtUnixMs, CancellationToken cancellationToken);
    }
}
