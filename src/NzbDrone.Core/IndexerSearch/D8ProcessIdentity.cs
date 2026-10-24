using System;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace NzbDrone.Core.IndexerSearch
{
    internal static class D8ProcessIdentity
    {
        private const string BootIdPath = "/proc/sys/kernel/random/boot_id";
        private const string StatPath = "/proc/self/stat";
        private const string StatusPath = "/proc/self/status";
        private const string Domain = "fss-d8f-process-instance-v1";
        private const int MaxPid = 4194304;

        internal static string ReadCurrent()
        {
            if (!OperatingSystem.IsLinux())
            {
                throw new InvalidOperationException("D8 process identity requires Linux procfs");
            }

            try
            {
                return Derive(File.ReadAllText(BootIdPath), File.ReadAllText(StatPath), File.ReadAllText(StatusPath));
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                throw new InvalidOperationException("D8 process identity runtime metadata is unavailable", exception);
            }
        }

        internal static string Derive(string bootId, string stat, string status)
        {
            if (bootId == null || stat == null || status == null)
            {
                throw new InvalidOperationException("D8 process identity runtime metadata is missing");
            }

            var canonicalBootId = bootId.Trim();
            if (!Guid.TryParseExact(canonicalBootId, "D", out var parsedBootId) || parsedBootId.ToString("D") != canonicalBootId)
            {
                throw new InvalidOperationException("D8 process identity boot ID is malformed");
            }

            var startTicks = ParseStartTicks(stat);
            var namespacePid = ParseNamespacePid(status);
            var material = string.Concat(Domain, "\0", canonicalBootId, "\0", startTicks.ToString(CultureInfo.InvariantCulture), "\0", namespacePid.ToString(CultureInfo.InvariantCulture));
            var digest = SHA256.HashData(Encoding.UTF8.GetBytes(material));
            digest[6] = (byte)((digest[6] & 0x0F) | 0x80);
            digest[8] = (byte)((digest[8] & 0x3F) | 0x80);
            var hex = Convert.ToHexString(digest.AsSpan(0, 16)).ToLowerInvariant();
            return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..32]}";
        }

        private static long ParseStartTicks(string stat)
        {
            var open = stat.IndexOf(" (", StringComparison.Ordinal);
            var close = stat.LastIndexOf(')');
            if (open < 1 || close < open || !IsCanonicalDecimal(stat.AsSpan(0, open), 7))
            {
                throw new InvalidOperationException("D8 process identity stat metadata is malformed");
            }

            // Field 22 (starttime) is the 20th field after the parenthesized command name (field 2).
            var fields = stat.Substring(close + 1).Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 20 || !IsCanonicalDecimal(fields[19], 19) ||
                !long.TryParse(fields[19], NumberStyles.None, CultureInfo.InvariantCulture, out var startTicks))
            {
                throw new InvalidOperationException("D8 process identity start ticks are malformed");
            }

            return startTicks;
        }

        private static int ParseNamespacePid(string status)
        {
            string found = null;
            foreach (var line in status.Split('\n'))
            {
                if (line.StartsWith("NSpid:", StringComparison.Ordinal))
                {
                    if (found != null)
                    {
                        throw new InvalidOperationException("D8 process identity namespace PID is ambiguous");
                    }

                    found = line.Substring("NSpid:".Length);
                }
            }

            // One entry is valid (native PID namespace); otherwise the last entry is the PID in the
            // process's own namespace. Every entry must be well formed, matching the FSS observer.
            var values = found?.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
            if (values == null || values.Length < 1)
            {
                throw new InvalidOperationException("D8 process identity namespace PID is malformed");
            }

            var namespacePid = 0;
            foreach (var value in values)
            {
                if (!IsCanonicalDecimal(value, 7) ||
                    !int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out namespacePid) ||
                    namespacePid > MaxPid)
                {
                    throw new InvalidOperationException("D8 process identity namespace PID is malformed");
                }
            }

            return namespacePid;
        }

        private static bool IsCanonicalDecimal(ReadOnlySpan<char> value, int maxDigits)
        {
            if (value.Length < 1 || value.Length > maxDigits || value[0] < '1' || value[0] > '9')
            {
                return false;
            }

            foreach (var digit in value)
            {
                if (digit < '0' || digit > '9')
                {
                    return false;
                }
            }

            return true;
        }
    }
}
