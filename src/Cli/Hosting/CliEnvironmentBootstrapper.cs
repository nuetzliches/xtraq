using Xtraq.Utils;

namespace Xtraq.Cli.Hosting;

/// <summary>
/// Bootstraps CLI-specific environment variables and .env files before the host is constructed.
/// </summary>
internal static class CliEnvironmentBootstrapper
{
    public static void Initialize(string[] normalizedArgs)
    {
        TryPublishExistingProjectRoot();
        LoadEnvironmentFiles(normalizedArgs);
    }

    private static void TryPublishExistingProjectRoot()
    {
        try
        {
            var existingProjectPath = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH")
                ?? Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");

            if (!string.IsNullOrWhiteSpace(existingProjectPath))
            {
                CliHostUtilities.UpdateProjectEnvironment(existingProjectPath);
            }
            else
            {
                var defaultRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(Directory.GetCurrentDirectory());
                if (!string.IsNullOrWhiteSpace(defaultRoot))
                {
                    CliHostUtilities.UpdateProjectEnvironment(defaultRoot);
                }
            }
        }
        catch
        {
        }
    }

    private static void LoadEnvironmentFiles(string[] normalizedArgs)
    {
        try
        {
            var cwd = Directory.GetCurrentDirectory();
            LoadSkipVarsFromEnv(Path.Combine(cwd, ".env"));
            LoadEnvVariables(Path.Combine(cwd, ".env"));

            try
            {
                var resolvedRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(cwd);
                if (!string.Equals(resolvedRoot, cwd, StringComparison.OrdinalIgnoreCase))
                {
                    LoadSkipVarsFromEnv(Path.Combine(resolvedRoot, ".env"));
                    LoadEnvVariables(Path.Combine(resolvedRoot, ".env"));
                }
            }
            catch
            {
            }

            var projectHint = CliHostUtilities.TryExtractProjectHint(normalizedArgs);
            if (!string.IsNullOrWhiteSpace(projectHint))
            {
                var (configPath, projectRoot) = CliHostUtilities.NormalizeCliProjectHint(projectHint);
                if (!string.IsNullOrWhiteSpace(configPath))
                {
                    LoadSkipVarsFromEnv(configPath);
                    LoadEnvVariables(configPath);
                }

                if (!string.IsNullOrWhiteSpace(projectRoot) && !string.Equals(projectRoot, cwd, StringComparison.OrdinalIgnoreCase))
                {
                    LoadSkipVarsFromEnv(Path.Combine(projectRoot!, ".env"));
                    LoadEnvVariables(Path.Combine(projectRoot!, ".env"));
                }
            }
        }
        catch
        {
        }
    }

    private static void LoadSkipVarsFromEnv(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        foreach (var rawLine in File.ReadAllLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var equalsIndex = line.IndexOf('=');
            if (equalsIndex <= 0)
            {
                continue;
            }

            var key = line.Substring(0, equalsIndex).Trim();
            if (!key.Equals("XTRAQ_NO_UPDATE", StringComparison.OrdinalIgnoreCase) &&
                !key.Equals("XTRAQ_SKIP_UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = line[(equalsIndex + 1)..].Trim();
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(key)))
            {
                Environment.SetEnvironmentVariable(key, value);
            }
        }
    }

    private static void LoadEnvVariables(string path)
    {
        try
        {
            EnvFileLoader.Apply(path);
        }
        catch
        {
        }
    }
}
