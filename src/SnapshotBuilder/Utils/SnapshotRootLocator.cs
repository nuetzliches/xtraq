using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Utils;

/// <summary>
/// Discovers project roots that contain Xtraq snapshot metadata (.xtraq directories).
/// </summary>
internal static class SnapshotRootLocator
{
    internal static IEnumerable<string> EnumerateSnapshotRoots()
    {
        var explicitRoot = ResolveExplicitSnapshotRoot();
        if (!string.IsNullOrWhiteSpace(explicitRoot))
        {
            yield return explicitRoot!;
            yield break;
        }

        var projectRoot = ResolveProjectRoot();
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            yield break;
        }

        if (Directory.Exists(Path.Combine(projectRoot!, ".xtraq")))
        {
            yield return projectRoot!;
        }
    }

    private static string? ResolveExplicitSnapshotRoot()
    {
        var hint = Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT");
        if (string.IsNullOrWhiteSpace(hint))
        {
            return null;
        }

        var normalized = NormalizeDirectory(hint);
        if (normalized is null)
        {
            return null;
        }

        if (Path.GetFileName(normalized).Equals(".xtraq", StringComparison.OrdinalIgnoreCase))
        {
            if (!Directory.Exists(normalized))
            {
                return null;
            }

            var parent = Path.GetDirectoryName(normalized);
            return NormalizeDirectory(parent);
        }

        return Directory.Exists(Path.Combine(normalized, ".xtraq"))
            ? normalized
            : null;
    }

    private static string? ResolveProjectRoot()
    {
        var hint = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH")
                   ?? Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");

        if (!string.IsNullOrWhiteSpace(hint))
        {
            var normalized = NormalizeDirectory(hint);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return normalized;
            }
        }

        var resolved = ProjectRootResolver.ResolveCurrent();
        return NormalizeDirectory(resolved);
    }

    private static string? NormalizeDirectory(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        try
        {
            var path = Path.GetFullPath(value);
            return Directory.Exists(path) ? path : null;
        }
        catch
        {
            return null;
        }
    }
}
