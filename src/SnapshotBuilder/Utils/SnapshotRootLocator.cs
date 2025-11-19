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
        var current = Directory.GetCurrentDirectory();
        var candidates = new List<string?>
        {
            current,
            Path.Combine(current, "debug"),
            Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT")
        };

        try
        {
            var resolvedWorkingDirectory = DirectoryUtils.GetWorkingDirectory();
            if (!string.IsNullOrWhiteSpace(resolvedWorkingDirectory))
            {
                candidates.Add(resolvedWorkingDirectory);
            }
        }
        catch
        {
            // Ignore failures resolving the working directory; remaining candidates still apply.
        }

        var projectRootEnv = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
        if (!string.IsNullOrWhiteSpace(projectRootEnv))
        {
            candidates.Add(projectRootEnv);
        }

        var roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in candidates)
        {
            if (string.IsNullOrWhiteSpace(candidate))
            {
                continue;
            }

            string fullPath;
            try
            {
                fullPath = Path.GetFullPath(candidate);
            }
            catch
            {
                continue;
            }

            if (!Directory.Exists(fullPath))
            {
                continue;
            }

            if (TryAddSnapshotRoot(fullPath, roots))
            {
                continue;
            }

            try
            {
                foreach (var snapshotDir in Directory.EnumerateDirectories(fullPath, ".xtraq", SearchOption.AllDirectories))
                {
                    var parent = Path.GetDirectoryName(snapshotDir);
                    if (!string.IsNullOrWhiteSpace(parent))
                    {
                        TryAddSnapshotRoot(parent, roots);
                    }
                }
            }
            catch
            {
                // Ignore traversal failures for individual candidates.
            }
        }

        return roots;

        static bool TryAddSnapshotRoot(string root, ISet<string> collector)
        {
            if (!Directory.Exists(Path.Combine(root, ".xtraq")))
            {
                return false;
            }

            collector.Add(root);
            return true;
        }
    }
}
