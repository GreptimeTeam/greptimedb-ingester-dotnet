using Grpc.Core;

namespace GreptimeDB.Ingester.Internal;

/// <summary>
/// Encodes write hints as the <c>x-greptime-hints</c> gRPC metadata entry.
/// </summary>
internal static class RequestHints
{
    private const string MetadataKey = "x-greptime-hints";

    /// <summary>
    /// Returns the metadata carrying the hints, or null when there are none.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when a hint cannot be encoded.</exception>
    public static Metadata? ToMetadata(IReadOnlyDictionary<string, string>? hints, string paramName)
    {
        if (hints is null || hints.Count == 0)
        {
            return null;
        }

        Validate(hints, paramName);
        return new Metadata
        {
            { MetadataKey, string.Join(",", hints.Select(hint => $"{hint.Key}={hint.Value}")) }
        };
    }

    /// <summary>
    /// Validates that every hint can be encoded as <c>key=value</c> in a comma-separated ASCII header.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when a hint cannot be encoded.</exception>
    public static void Validate(IReadOnlyDictionary<string, string> hints, string paramName)
    {
        foreach (var (key, value) in hints)
        {
            if (string.IsNullOrWhiteSpace(key) || !IsEncodable(key) || key.Contains('='))
            {
                throw new ArgumentException(
                    $"Invalid hint key '{key}': must be non-empty printable ASCII without ',' or '='.",
                    paramName);
            }

            if (value is null || !IsEncodable(value))
            {
                throw new ArgumentException(
                    $"Invalid value for hint '{key}': must be printable ASCII without ','.",
                    paramName);
            }
        }
    }

    // The server splits the header on ',' and each entry on the first '='.
    private static bool IsEncodable(string s) => s.All(c => c is >= ' ' and <= '~' and not ',');
}
