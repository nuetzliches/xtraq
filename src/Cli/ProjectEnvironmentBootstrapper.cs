namespace Xtraq.Cli;

/// <summary>
/// Orchestrates project environment setup by ensuring a .env file exists and syncing tracked configuration.
/// </summary>
internal static class ProjectEnvironmentBootstrapper
{
    private static readonly bool Verbose = Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE");
    private const string EnvFileName = ".env";
    private const string EnvExampleFileName = ".env.example";
    private const string EnvExampleTemplateRelativePath = "debug\\.env.example";

    /// <summary>
    /// Ensure a .env exists at <paramref name="projectRoot"/>. Can run interactively (prompt) or non-interactively (autoApprove).
    /// When force==true an existing file will be overwritten.
    /// </summary>
    internal static async Task<string> EnsureEnvAsync(string projectRoot, bool autoApprove = false, bool force = false, string? explicitTemplate = null)
    {
        Directory.CreateDirectory(projectRoot);
        var envPath = Path.Combine(projectRoot, EnvFileName);
        if (File.Exists(envPath) && !force)
        {
            TryWriteTrackableConfig(projectRoot, envPath);
            EnsureEnvExample(projectRoot, force: false, explicitTemplate);
            return envPath;
        }

        if (!autoApprove)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[xtraq] Generator requires a {EnvFileName} with at least one XTRAQ_ marker.");
            Console.ResetColor();
            Console.Write(File.Exists(envPath) ? $"Overwrite existing {EnvFileName}? [y/N]: " : "Create new .env now? [Y/n]: ");
            var answer = ReadAnswer();
            if (!IsYes(answer))
            {
                throw new InvalidOperationException(".env creation aborted by user - Xtraq requires an .env file.");
            }
        }

        try
        {
            File.WriteAllText(envPath, BuildMinimalEnvContent());
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{(force ? "(re)created" : "Created")} {EnvFileName} at '{envPath}'.");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Failed to create .env: {ex.Message}.");
            Console.ResetColor();
            throw;
        }

        TryWriteTrackableConfig(projectRoot, envPath);
        EnsureEnvExample(projectRoot, force, explicitTemplate);
        await Task.CompletedTask;
        return envPath;
    }

    /// <summary>
    /// Ensure a project-scoped .env.example exists using the bundled template as source.
    /// </summary>
    internal static string EnsureEnvExample(string projectRoot, bool force = false, string? explicitTemplate = null)
    {
        Directory.CreateDirectory(projectRoot);
        var examplePath = Path.Combine(projectRoot, EnvExampleFileName);
        if (!force && File.Exists(examplePath))
        {
            return examplePath;
        }

        try
        {
            var content = ResolveExampleTemplateContent(projectRoot, explicitTemplate);
            File.WriteAllText(examplePath, content);
        }
        catch (Exception ex)
        {
            if (Verbose)
            {
                Console.Out.WriteLine($"[xtraq] Failed to create {EnvExampleFileName}: {ex.Message}");
            }
        }

        return examplePath;
    }

    /// <summary>
    /// Ensures the project .gitignore ignores Xtraq cache and telemetry folders.
    /// </summary>
    /// <param name="projectRoot">Project root directory.</param>
    internal static void EnsureProjectGitignore(string projectRoot)
    {
        Directory.CreateDirectory(projectRoot);
        var gitignorePath = Path.Combine(projectRoot, ".gitignore");
        var lines = File.Exists(gitignorePath) ? File.ReadAllLines(gitignorePath).ToList() : new List<string>();
        var seen = new HashSet<string>(lines, StringComparer.Ordinal);
        var required = new[] { ".xtraq/cache/", ".xtraq/telemetry/" };
        var updated = false;

        foreach (var entry in required)
        {
            if (seen.Add(entry))
            {
                lines.Add(entry);
                updated = true;
            }
        }

        if (updated || !File.Exists(gitignorePath))
        {
            File.WriteAllLines(gitignorePath, lines);
        }
    }

    private static void TryWriteTrackableConfig(string projectRoot, string envPath)
    {
        try
        {
            Xtraq.Configuration.TrackableConfigManager.WriteFromEnvFile(projectRoot, envPath);
        }
        catch (Exception ex)
        {
            if (Verbose)
            {
                Console.Out.WriteLine($"[xtraq] Trackable config update failed: {ex.Message}");
            }
        }
    }

    private static string BuildMinimalEnvContent()
    {
        return "# Populate the generator connection string before running snapshot/build" + Environment.NewLine
             + "XTRAQ_GENERATOR_DB=" + Environment.NewLine;
    }

    private static string ResolveExampleTemplateContent(string projectRoot, string? explicitTemplate)
    {
        if (!string.IsNullOrEmpty(explicitTemplate))
        {
            return explicitTemplate;
        }

        var candidatePaths = new List<string>
        {
            Path.Combine(projectRoot, EnvExampleTemplateRelativePath)
        };

        var repoRoot = FindRepoRoot(projectRoot);
        if (!string.IsNullOrWhiteSpace(repoRoot))
        {
            candidatePaths.Add(Path.Combine(repoRoot, EnvExampleTemplateRelativePath));
        }

        foreach (var candidate in candidatePaths)
        {
            try
            {
                if (File.Exists(candidate))
                {
                    return File.ReadAllText(candidate);
                }
            }
            catch
            {
                // ignore missing or inaccessible templates
            }
        }

        return "# Example configuration for Xtraq" + Environment.NewLine
             + "#XTRAQ_GENERATOR_DB=Server=.;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;" + Environment.NewLine
             + "# XTRAQ_LOG_LEVEL=Debug" + Environment.NewLine
             + "# XTRAQ_ALIAS_DEBUG=1" + Environment.NewLine;
    }

    private static string? FindRepoRoot(string start)
    {
        try
        {
            var dir = new DirectoryInfo(start);
            while (dir is not null)
            {
                if (File.Exists(Path.Combine(dir.FullName, "README.md")) && Directory.Exists(Path.Combine(dir.FullName, "src")))
                {
                    return dir.FullName;
                }

                dir = dir.Parent;
            }
        }
        catch
        {
            // best-effort lookup
        }

        return null;
    }

    private static string ReadAnswer()
    {
        var line = Console.ReadLine();
        return line?.Trim() ?? string.Empty;
    }

    private static bool IsYes(string input)
    {
        return input.Length == 0
            || input.Equals("y", StringComparison.OrdinalIgnoreCase)
            || input.Equals("yes", StringComparison.OrdinalIgnoreCase);
    }
}
