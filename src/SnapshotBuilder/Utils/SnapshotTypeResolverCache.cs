using Xtraq.Metadata;

namespace Xtraq.SnapshotBuilder.Utils;

/// <summary>
/// Provides cached <see cref="TypeMetadataResolver"/> instances keyed by snapshot root paths.
/// The cache invalidates automatically when the underlying scalar type directory changes.
/// </summary>
internal static class SnapshotTypeResolverCache
{
    private static readonly object Sync = new();
    private static readonly Dictionary<string, CachedResolver> Cache = new(StringComparer.OrdinalIgnoreCase);

    internal static TypeMetadataResolver Get(string root)
    {
        var timestamp = GetTypesDirectoryTimestamp(root);

        lock (Sync)
        {
            if (Cache.TryGetValue(root, out var cached))
            {
                if (timestamp <= cached.Timestamp)
                {
                    return cached.Resolver;
                }
            }

            var resolver = new TypeMetadataResolver(root);
            Cache[root] = new CachedResolver(resolver, timestamp);
            return resolver;
        }
    }

    private static DateTime GetTypesDirectoryTimestamp(string root)
    {
        try
        {
            var dir = Path.Combine(root, ".xtraq", "snapshots", "types");
            if (!Directory.Exists(dir))
            {
                return DateTime.MinValue;
            }

            return Directory.GetLastWriteTimeUtc(dir);
        }
        catch
        {
            return DateTime.MinValue;
        }
    }

    private sealed record CachedResolver(TypeMetadataResolver Resolver, DateTime Timestamp);
}
