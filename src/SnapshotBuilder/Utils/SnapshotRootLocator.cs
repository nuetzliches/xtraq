using Xtraq.Configuration;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Utils;

/// <summary>
/// Discovers project roots that contain Xtraq snapshot metadata (.xtraq directories).
/// The logic mirrors the analyzer/runtime enumeration so writers and analyzers share the same probing behaviour.
/// </summary>
internal static class SnapshotRootLocator
{
    internal static IEnumerable<string> EnumerateSnapshotRoots()
    {
        var roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in EnumerateCandidates())
        {
            if (string.IsNullOrWhiteSpace(candidate))
            {
                continue;
            }

            var normalized = NormalizeCandidate(candidate);
            if (normalized is null)
            {
                continue;
            }

            if (TryAddSnapshotRoot(normalized, roots))
            {
                continue;
            }

            if (Directory.Exists(normalized))
            {
                var resolvedProjectRoot = SafeResolveProjectRoot(normalized);
                if (!string.IsNullOrWhiteSpace(resolvedProjectRoot))
                {
                    TryAddSnapshotRoot(resolvedProjectRoot!, roots);
                }
            }
        }

        if (roots.Count == 0)
        {
            var fallback = NormalizeCandidate(ProjectRootResolver.ResolveCurrent());
            if (fallback is not null)
            {
                TryAddSnapshotRoot(fallback, roots);
            }
        }

        return roots;

        static IEnumerable<string?> EnumerateCandidates()
        {
            yield return Safe(() => Directory.GetCurrentDirectory());
            yield return Safe(static () => Path.Combine(Directory.GetCurrentDirectory(), "debug"));
            yield return Safe(() => DirectoryUtils.GetWorkingDirectory());
            yield return Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
            yield return Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT");
        }

        static string? Safe(Func<string> factory)
        {
            try
            {
                return factory();
            }
            catch
            {
                return null;
            }
        }

        static string? NormalizeCandidate(string? candidate)
        {
            if (string.IsNullOrWhiteSpace(candidate))
            {
                return null;
            }

            try
            {
                var fullPath = Path.GetFullPath(candidate);
                if (Path.GetFileName(fullPath).Equals(".xtraq", StringComparison.OrdinalIgnoreCase))
                {
                    var parent = Path.GetDirectoryName(fullPath);
                    return string.IsNullOrWhiteSpace(parent) ? null : parent;
                }

                return fullPath;
            }
            catch
            {
                return null;
            }
        }

        static string? SafeResolveProjectRoot(string baseDirectory)
        {
            try
            {
                return TrackableConfigManager.ResolveProjectRoot(baseDirectory);
            }
            catch
            {
                return null;
            }
        }

        static bool TryAddSnapshotRoot(string root, ISet<string> collector)
        {
            if (string.IsNullOrWhiteSpace(root))
            {
                return false;
            }

            try
            {
                var normalized = Path.GetFullPath(root);
                if (collector.Contains(normalized))
                {
                    return true;
                }

                if (Directory.Exists(Path.Combine(normalized, ".xtraq")))
                {
                    collector.Add(normalized);
                    return true;
                }
            }
            catch
            {
                return false;
            }

            return false;
        }
    }
}
