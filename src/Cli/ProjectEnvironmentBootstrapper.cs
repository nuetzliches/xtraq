namespace Xtraq.Cli;

/// <summary>
/// Orchestrates project environment setup by ensuring a .env file exists and syncing tracked configuration.
/// </summary>
internal static class ProjectEnvironmentBootstrapper
{
    private static readonly bool Verbose = Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE");
    private const string ExampleRelativePath = "samples\\restapi\\.env.example";
    private const string EnvFileName = ".env";

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
            // Enhancement: attempt in-place XTRAQ_BUILD_SCHEMAS prefill if missing.
            try
            {
                var existing = File.ReadAllText(envPath);
                if (!existing.Split('\n').Any(l => l.TrimStart().StartsWith("XTRAQ_BUILD_SCHEMAS", StringComparison.OrdinalIgnoreCase)))
                {
                    var inferred = InferSchemasForPrefill(projectRoot);
                    if (inferred != null && inferred.Count > 0)
                    {
                        existing += (existing.EndsWith("\n") ? string.Empty : "\n") +
                                    "XTRAQ_BUILD_SCHEMAS=" + string.Join(",", inferred) + "\n";
                        File.WriteAllText(envPath, existing);
                        try { if (Verbose) Console.Out.WriteLine($"[xtraq] Augmented existing .env with XTRAQ_BUILD_SCHEMAS={string.Join(",", inferred)}"); } catch { }
                    }
                }
            }
            catch { /* non-fatal */ }
            TryWriteTrackableConfig(projectRoot, envPath);
            return envPath; // already present and not forcing (after augmentation attempt)
        }

        // Interactive approval unless autoApprove
        if (!autoApprove)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[xtraq] Generator requires a {EnvFileName} with at least one XTRAQ_ marker.");
            Console.ResetColor();
            Console.Write(File.Exists(envPath) ? $"Overwrite existing {EnvFileName}? [y/N]: " : "Create new .env from example now? [Y/n]: ");
            var answer = ReadAnswer();
            var proceed = IsYes(answer);
            if (!proceed)
            {
                throw new InvalidOperationException(".env creation aborted by user - Xtraq requires an .env file.");
            }
        }

        try
        {
            string baseContent = ResolveExampleContent(projectRoot, explicitTemplate);
            var mergedContent = MergeWithConfig(projectRoot, baseContent);
            File.WriteAllText(envPath, mergedContent);
            if (!mergedContent.Contains("XTRAQ_"))
            {
                File.AppendAllText(envPath, "# XTRAQ_NAMESPACE=AddYourNamespaceHere\n");
            }
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{(force ? "(re)created" : "Created")} {EnvFileName} at '{envPath}'.");
            Console.ResetColor();
            if (Verbose) Console.WriteLine("[xtraq] Next steps: (1) Review XTRAQ_NAMESPACE (2) Re-run generation.");
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
        await Task.CompletedTask;
        return envPath;
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

    private static string? FindRepoRoot(string start)
    {
        try
        {
            var dir = new DirectoryInfo(start);
            while (dir != null)
            {
                if (File.Exists(Path.Combine(dir.FullName, "README.md")) && Directory.Exists(Path.Combine(dir.FullName, "src")))
                    return dir.FullName;
                dir = dir.Parent;
            }
        }
        catch { }
        return null;
    }

    private static string ReadAnswer()
    {
        var line = Console.ReadLine();
        return line?.Trim() ?? string.Empty;
    }
    private static bool IsYes(string input) => input.Length == 0 || input.Equals("y", StringComparison.OrdinalIgnoreCase) || input.Equals("yes", StringComparison.OrdinalIgnoreCase);

    private static string ResolveExampleContent(string projectRoot, string? explicitTemplate)
    {
        if (!string.IsNullOrEmpty(explicitTemplate)) return explicitTemplate;
        var examplePath = Path.Combine(projectRoot, ExampleRelativePath);
        if (!File.Exists(examplePath))
        {
            var repoRoot = FindRepoRoot(projectRoot);
            if (repoRoot != null)
            {
                var alt = Path.Combine(repoRoot, ExampleRelativePath);
                if (File.Exists(alt)) examplePath = alt;
            }
        }
        if (File.Exists(examplePath)) return File.ReadAllText(examplePath);
        return "# Xtraq configuration\n"
            + "# XTRAQ_NAMESPACE=Your.Project.Namespace\n"
            + "# XTRAQ_GENERATOR_DB=Server=localhost;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;\n"
            + "# XTRAQ_OUTPUT_DIR=Xtraq\n"
            + "# XTRAQ_BUILD_SCHEMAS=SchemaA,SchemaB\n";
    }

    private static string MergeWithConfig(string projectRoot, string exampleContent)
    {
        var buildSchemas = InferSchemasForPrefill(projectRoot);
        // Simple line map override: keep comments, replace key lines if present, append if missing.
        var lines = exampleContent.Replace("\r\n", "\n").Split('\n');
        var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line) || line.TrimStart().StartsWith("#")) continue;
            var eq = line.IndexOf('=');
            if (eq > 0)
            {
                var key = line.Substring(0, eq).Trim();
                if (key.StartsWith("XTRAQ_", StringComparison.OrdinalIgnoreCase) && !dict.ContainsKey(key)) dict[key] = i;
            }
        }
        void Upsert(string key, string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return;
            var line = key + "=" + value;
            if (dict.TryGetValue(key, out var idx)) lines[idx] = line;
            else
            {
                // append before final separator block if exists
                var list = lines.ToList();
                list.Add(line);
                lines = list.ToArray();
            }
        }
        // Without legacy configuration we only prefill build schema hints; namespace/TFM/connection remain placeholders.
        // Insert XTRAQ_BUILD_SCHEMAS (comma separated) if we have any inferred/explicit build schemas; else add placeholder comment
        if (buildSchemas != null && buildSchemas.Count > 0)
        {
            Upsert("XTRAQ_BUILD_SCHEMAS", string.Join(",", buildSchemas.Distinct(StringComparer.OrdinalIgnoreCase)));
        }
        else
        {
            if (!dict.ContainsKey("XTRAQ_BUILD_SCHEMAS"))
            {
                var listLines = lines.ToList();
                listLines.Add("# XTRAQ_BUILD_SCHEMAS=SchemaA,SchemaB (optional positive allow-list; empty -> all except ignored)");
                lines = listLines.ToArray();
            }
        }
        return string.Join(Environment.NewLine, lines) + Environment.NewLine;
    }

    // Helper: reuse schema inference for augmentation
    private static List<string>? InferSchemasForPrefill(string projectRoot)
    {
        List<string>? buildSchemas = null;
        // Snapshot fallback (index.json)
        try
        {
            var snapshotDir = Path.Combine(projectRoot, ".xtraq", "snapshots");
            var indexPath = Path.Combine(snapshotDir, "index.json");
            if (buildSchemas == null && File.Exists(indexPath))
            {
                using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(indexPath));
                var root = doc.RootElement;
                if (root.TryGetProperty("Procedures", out var procsEl) && procsEl.ValueKind == System.Text.Json.JsonValueKind.Array)
                {
                    foreach (var procEl in procsEl.EnumerateArray())
                    {
                        try
                        {
                            string? opName = procEl.TryGetProperty("OperationName", out var opEl) ? opEl.GetString() : null;
                            if (string.IsNullOrWhiteSpace(opName)) continue;
                            var dotIdx = opName.IndexOf('.');
                            var schemaName = dotIdx > 0 ? opName.Substring(0, dotIdx) : "dbo";
                            if (!string.IsNullOrWhiteSpace(schemaName))
                            {
                                buildSchemas ??= new List<string>();
                                buildSchemas.Add(schemaName);
                            }
                        }
                        catch { }
                    }
                }
            }
        }
        catch { }
        // Expanded layout (procedures/*.json)
        try
        {
            if (buildSchemas == null)
            {
                var procDir = Path.Combine(projectRoot, ".xtraq", "snapshots", "procedures");
                if (Directory.Exists(procDir))
                {
                    foreach (var file in Directory.EnumerateFiles(procDir, "*.json", SearchOption.TopDirectoryOnly))
                    {
                        try
                        {
                            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(file));
                            var root = doc.RootElement;
                            string? opName = root.TryGetProperty("OperationName", out var opEl) ? opEl.GetString() : null;
                            if (string.IsNullOrWhiteSpace(opName)) continue;
                            var dotIdx = opName.IndexOf('.');
                            var schemaName = dotIdx > 0 ? opName.Substring(0, dotIdx) : "dbo";
                            if (!string.IsNullOrWhiteSpace(schemaName))
                            {
                                buildSchemas ??= new List<string>();
                                buildSchemas.Add(schemaName);
                            }
                        }
                        catch { }
                    }
                }
            }
        }
        catch { }
        if (buildSchemas != null && buildSchemas.Count > 0)
            return buildSchemas.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
        return null;
    }
}
