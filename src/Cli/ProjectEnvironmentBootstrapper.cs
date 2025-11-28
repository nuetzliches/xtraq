namespace Xtraq.Cli;

/// <summary>
/// Orchestrates project environment setup by ensuring a .env file exists and syncing tracked configuration.
/// </summary>
internal static class ProjectEnvironmentBootstrapper
{
    private static readonly bool Verbose = Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE");
    private const string EnvFileName = ".env";
    private const string EnvExampleFileName = ".env.example";
    private static readonly string[] EnvExampleTemplateRelativePaths = new[]
    {
        Path.Combine("Templates", ".env.example"),          // packaged / build output
        Path.Combine("src", "Templates", ".env.example"),   // repo root (source)
    };

    /// <summary>
    /// Ensure a .env exists at <paramref name="projectRoot"/>. Can run interactively (prompt) or non-interactively (autoApprove).
    /// When force==true an existing file will be overwritten.
    /// </summary>
    internal static async Task<string> EnsureEnvAsync(string projectRoot, bool autoApprove = false, bool force = false, string? explicitTemplate = null, string? connectionString = null)
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
            var content = ResolveExampleTemplateContent(projectRoot, explicitTemplate);
            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                content = content.Replace("<your_database_connection_string>", connectionString.Trim());
            }
            Console.ForegroundColor = ConsoleColor.Green;
            File.WriteAllText(envPath, content);
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
        var header = "# Xtraq";
        var entries = new[] { ".xtraq/cache/", ".xtraq/telemetry/", ".xtraqconfig.local" };
        var updated = false;

        var headerIndex = lines.FindIndex(line => string.Equals(line.Trim(), header, StringComparison.Ordinal));

        if (headerIndex < 0)
        {
            if (lines.Count > 0 && lines[^1].Length != 0)
            {
                lines.Add(string.Empty);
            }

            lines.Add(header);
            lines.AddRange(entries);
            updated = true;
        }
        else
        {
            if (headerIndex > 0 && lines[headerIndex - 1].Length != 0)
            {
                lines.Insert(headerIndex, string.Empty);
                headerIndex++;
                updated = true;
            }

            var blockStart = headerIndex + 1;
            var blockEnd = blockStart;
            while (blockEnd < lines.Count && lines[blockEnd].Length != 0 && !lines[blockEnd].StartsWith("#", StringComparison.Ordinal))
            {
                blockEnd++;
            }

            var currentBlock = lines.GetRange(blockStart, blockEnd - blockStart);
            if (currentBlock.Count != entries.Length || !currentBlock.SequenceEqual(entries))
            {
                lines.RemoveRange(blockStart, blockEnd - blockStart);
                lines.InsertRange(blockStart, entries);
                updated = true;
                blockEnd = blockStart + entries.Length;
            }

            var entrySet = new HashSet<string>(entries, StringComparer.Ordinal);
            for (var index = lines.Count - 1; index >= 0; index--)
            {
                var withinBlock = index >= blockStart && index < blockStart + entries.Length;
                if (withinBlock)
                {
                    continue;
                }

                if (entrySet.Contains(lines[index]))
                {
                    lines.RemoveAt(index);
                    if (index < blockStart)
                    {
                        blockStart--;
                    }
                    updated = true;
                }
            }
        }

        if (updated || !File.Exists(gitignorePath))
        {
            File.WriteAllLines(gitignorePath, lines);
        }

        TryEnsureCsprojIncludesXtraq(projectRoot);
    }

    private static void TryEnsureCsprojIncludesXtraq(string projectRoot)
    {
        try
        {
            var csprojPath = FindNearestCsproj(projectRoot);
            if (string.IsNullOrWhiteSpace(csprojPath))
            {
                return;
            }

            var content = File.ReadAllText(csprojPath);
            if (content.IndexOf(".xtraq", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return;
            }

            Console.Write($"[xtraq init] Add .xtraq content include to '{Path.GetFileName(csprojPath)}'? [Y/n]: ");
            var answer = ReadAnswer();
            if (!IsYes(answer))
            {
                return;
            }

            var newline = content.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
            var itemGroup = $"{newline}  <ItemGroup>{newline}    <Content Include=\".xtraq\\\\**\\\\*\" />{newline}  </ItemGroup>{newline}";
            var insertIndex = content.LastIndexOf("</Project>", StringComparison.OrdinalIgnoreCase);
            content = insertIndex >= 0
                ? content.Insert(insertIndex, itemGroup)
                : content + itemGroup;

            File.WriteAllText(csprojPath, content);
            Console.WriteLine($"[xtraq init] .xtraq include added to '{csprojPath}'.");
        }
        catch (Exception ex)
        {
            if (Verbose)
            {
                Console.Out.WriteLine($"[xtraq] Failed to update .csproj for .xtraq include: {ex.Message}");
            }
        }
    }

    private static string? FindNearestCsproj(string projectRoot)
    {
        try
        {
            var current = new DirectoryInfo(projectRoot);
            while (current is not null)
            {
                var proj = current.GetFiles("*.csproj", SearchOption.TopDirectoryOnly).FirstOrDefault();
                if (proj is not null)
                {
                    return proj.FullName;
                }

                current = current.Parent;
            }
        }
        catch
        {
            // ignore discovery errors
        }

        return null;
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

    internal static string ResolveExampleTemplateContent(string projectRoot, string? explicitTemplate)
    {
        if (!string.IsNullOrEmpty(explicitTemplate))
        {
            return explicitTemplate;
        }

        var searchRoots = new List<string>();
        if (!string.IsNullOrWhiteSpace(projectRoot))
        {
            searchRoots.Add(projectRoot);
        }

        var repoRoot = FindRepoRoot(projectRoot);
        if (!string.IsNullOrWhiteSpace(repoRoot))
        {
            searchRoots.Add(repoRoot);
        }

        try
        {
            var appBase = AppContext.BaseDirectory;
            if (!string.IsNullOrWhiteSpace(appBase) && !searchRoots.Contains(appBase, StringComparer.OrdinalIgnoreCase))
            {
                searchRoots.Add(appBase);
            }
        }
        catch
        {
            // ignore issues resolving app base
        }

        foreach (var root in searchRoots)
        {
            foreach (var relative in EnvExampleTemplateRelativePaths)
            {
                try
                {
                    var candidate = Path.Combine(root, relative);
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
        }

        return "# Example configuration for Xtraq" + Environment.NewLine
             + "# XTRAQ_GENERATOR_DB=Server=.;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;" + Environment.NewLine
             + "# XTRAQ_LOG_LEVEL=Debug" + Environment.NewLine;
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
