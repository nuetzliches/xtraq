
using Xtraq.Configuration;

namespace Xtraq.Utils;

/// <summary>
/// Centralized resolution of the active project root for generation.
/// Order of precedence:
/// 1. Environment variable XTRAQ_PROJECT_ROOT when it points to an existing directory
/// 2. .xtraqconfig discovery from the current working directory upwards
/// 3. Current working directory
/// Falls back gracefully and never throws.
/// Provides also a heuristic for the solution root (first parent containing src/Xtraq.csproj or .git folder).
/// </summary>
internal static class ProjectRootResolver
{
    public static string ResolveCurrent()
    {
        try
        {
            var projRoot = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
            if (!string.IsNullOrWhiteSpace(projRoot))
            {
                try
                {
                    var normalized = Path.GetFullPath(projRoot);
                    if (Directory.Exists(normalized))
                    {
                        return normalized;
                    }
                }
                catch
                {
                    // ignore invalid env hints
                }
            }

            return TrackableConfigManager.ResolveProjectRoot(Directory.GetCurrentDirectory());
        }
        catch { }
        // Fallback: current working directory (already adjusted by -p via DirectoryUtils.SetBasePath in CommandBase)
        try { return Directory.GetCurrentDirectory(); } catch { return AppContext.BaseDirectory; }
    }

    public static string GetSolutionRootOrCwd()
    {
        var start = ResolveCurrent();
        try
        {
            var dir = new DirectoryInfo(start);
            while (dir != null)
            {
                if (File.Exists(Path.Combine(dir.FullName, "Xtraq.sln")) || Directory.Exists(Path.Combine(dir.FullName, ".git")))
                {
                    return dir.FullName;
                }
                dir = dir.Parent;
            }
        }
        catch { }
        return start;
    }
}
