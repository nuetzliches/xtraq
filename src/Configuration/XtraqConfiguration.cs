namespace Xtraq.Configuration;

/// <summary>
/// Strongly typed configuration for generator with precedence:
/// CLI overrides > Environment Variables > .env file (xtraq.json fallback removed)
/// </summary>
public sealed class XtraqConfiguration
{
    /// <summary>
    /// Gets the connection string used for metadata discovery.
    /// </summary>
    public string? GeneratorConnectionString { get; init; }
    /// <summary>
    /// Gets the default connection string used by the generator runtime.
    /// </summary>
    public string? DefaultConnection { get; init; }
    /// <summary>
    /// Gets the root namespace used for generated artifacts.
    /// </summary>
    public string? NamespaceRoot { get; init; }
    /// <summary>
    /// Gets the output directory for generated artifacts relative to the project root.
    /// </summary>
    public string? OutputDir { get; init; }
    /// <summary>
    /// Gets the resolved configuration file path when supplied.
    /// </summary>
    public string? ConfigPath { get; init; }
    /// <summary>
    /// Gets the project root directory that anchors resource discovery.
    /// </summary>
    public string ProjectRoot { get; init; } = string.Empty;
    /// <summary>
    /// Positive allow-list for schemas to generate (XTRAQ_BUILD_SCHEMAS). Empty => fallback to ignored-schemas exclusion.
    /// </summary>
    public IReadOnlyList<string> BuildSchemas { get; init; } = Array.Empty<string>();
    /// <summary>
    /// Gets a value indicating whether the generator should emit <c>[JsonIncludeNullValues]</c> attributes.
    /// </summary>
    public bool EmitJsonIncludeNullValuesAttribute { get; init; }

    /// <summary>
    /// Loads the environment configuration by merging CLI overrides, environment variables, and .env settings.
    /// </summary>
    /// <param name="projectRoot">Optional project root used to resolve relative paths.</param>
    /// <param name="cliOverrides">Optional CLI-supplied key/value overrides.</param>
    /// <param name="explicitConfigPath">Optional explicit configuration file path.</param>
    /// <returns>A populated <see cref="XtraqConfiguration"/> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when required configuration cannot be resolved.</exception>
    public static XtraqConfiguration Load(string? projectRoot = null, IDictionary<string, string?>? cliOverrides = null, string? explicitConfigPath = null)
    {
        var verbose = Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE");
        projectRoot ??= Directory.GetCurrentDirectory();

        static string ResolveAgainst(string baseDir, string hint)
        {
            if (string.IsNullOrWhiteSpace(hint))
            {
                return hint;
            }

            try
            {
                if (Path.IsPathRooted(hint))
                {
                    return Path.GetFullPath(hint);
                }

                var effectiveBase = string.IsNullOrWhiteSpace(baseDir) ? Directory.GetCurrentDirectory() : baseDir;
                return Path.GetFullPath(Path.Combine(effectiveBase, hint));
            }
            catch
            {
                return hint;
            }
        }

        var envFilePath = ResolveEnvFile(projectRoot);
        var filePairs = LoadDotEnv(envFilePath);
        MergeTrackableConfig(projectRoot, filePairs);
        PublishEnvironmentVariables(filePairs);

        string? configHint = string.IsNullOrWhiteSpace(explicitConfigPath) ? null : explicitConfigPath;
        if (string.IsNullOrWhiteSpace(configHint))
        {
            var fromProcessHint = Environment.GetEnvironmentVariable("XTRAQ_CONFIG_PATH");
            if (!string.IsNullOrWhiteSpace(fromProcessHint))
            {
                configHint = fromProcessHint;
            }
            else if (filePairs.TryGetValue("XTRAQ_CONFIG_PATH", out var fromFileHint) && !string.IsNullOrWhiteSpace(fromFileHint))
            {
                configHint = fromFileHint;
            }
        }

        if (!string.IsNullOrWhiteSpace(configHint))
        {
            try
            {
                var baseDirForHint = envFilePath is not null && envFilePath.Length > 0
                    ? Path.GetDirectoryName(envFilePath) ?? projectRoot
                    : projectRoot;

                var resolvedHint = ResolveAgainst(baseDirForHint, configHint);
                explicitConfigPath = resolvedHint;

                string? newRoot = null;
                if (!string.IsNullOrWhiteSpace(resolvedHint))
                {
                    if (Directory.Exists(resolvedHint))
                    {
                        newRoot = resolvedHint;
                    }
                    else if (File.Exists(resolvedHint))
                    {
                        newRoot = Path.GetDirectoryName(resolvedHint);
                    }
                    else
                    {
                        var hintParent = Path.GetDirectoryName(resolvedHint);
                        if (!string.IsNullOrWhiteSpace(hintParent) && Directory.Exists(hintParent))
                        {
                            newRoot = hintParent;
                        }
                    }
                }

                if (!string.IsNullOrWhiteSpace(newRoot))
                {
                    projectRoot = newRoot!;
                    envFilePath = ResolveEnvFile(projectRoot);
                    filePairs = LoadDotEnv(envFilePath);
                    MergeTrackableConfig(projectRoot, filePairs);
                }
            }
            catch
            {
                if (verbose) Console.Error.WriteLine("[xtraq] Warning: config path resolution failed; using default project root.");
            }
        }

        MergeTrackableConfig(projectRoot, filePairs);
        PublishEnvironmentVariables(filePairs, overwrite: true);

        if (!TrackableConfigExists(projectRoot))
        {
            throw new InvalidOperationException("Xtraq project is not initialised. Run 'xtraq init'.");
        }

        try
        {
            if (!string.IsNullOrWhiteSpace(projectRoot))
            {
                Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", Path.GetFullPath(projectRoot));
            }

            if (!string.IsNullOrWhiteSpace(explicitConfigPath))
            {
                Environment.SetEnvironmentVariable("XTRAQ_CONFIG_PATH", Path.GetFullPath(explicitConfigPath));
            }
        }
        catch
        {
            if (verbose) Console.Error.WriteLine("[xtraq] Warning: Failed to publish project root environment variables.");
        }

        string Get(string key)
        {
            if (cliOverrides != null && cliOverrides.TryGetValue(key, out var fromCli) && !string.IsNullOrWhiteSpace(fromCli)) return fromCli!;
            var fromProcess = Environment.GetEnvironmentVariable(key);
            if (!string.IsNullOrWhiteSpace(fromProcess)) return fromProcess!;
            if (filePairs.TryGetValue(key, out var fromFile) && !string.IsNullOrWhiteSpace(fromFile)) return fromFile!;
            return string.Empty;
        }

        var fullConn = NullIfEmpty(Get("XTRAQ_GENERATOR_DB"));
        var buildSchemasList = ParseList(NullIfEmpty(Get("XTRAQ_BUILD_SCHEMAS")));
        if (string.IsNullOrWhiteSpace(fullConn) && verbose)
        {
            Console.Error.WriteLine("[xtraq] Warning: XTRAQ_GENERATOR_DB is not set. Run 'xtraq init' or provide the connection string via environment variables.");
        }

        var outputDirResolved = NullIfEmpty(Get("XTRAQ_OUTPUT_DIR")) ?? "Xtraq";
        var emitJsonIncludeNullValues = Xtraq.Utils.EnvironmentHelper.EqualsTrue(Get("XTRAQ_JSON_INCLUDE_NULL_VALUES"));

        var cfg = new XtraqConfiguration
        {
            GeneratorConnectionString = fullConn,
            DefaultConnection = fullConn,
            NamespaceRoot = NullIfEmpty(Get("XTRAQ_NAMESPACE")),
            OutputDir = outputDirResolved,
            ConfigPath = explicitConfigPath,
            BuildSchemas = buildSchemasList,
            ProjectRoot = projectRoot,
            EmitJsonIncludeNullValuesAttribute = emitJsonIncludeNullValues
        };

        if (string.IsNullOrEmpty(envFilePath) || !File.Exists(envFilePath))
        {
            if (verbose) Console.WriteLine("[xtraq] Migration: no .env file found.");
            Console.Write("[xtraq] Create new .env now? (Y/n): ");
            string? answer = null; try { answer = Console.ReadLine(); } catch { }
            var create = true;
            if (!string.IsNullOrWhiteSpace(answer))
            {
                var a = answer.Trim();
                if (a.Equals("n", StringComparison.OrdinalIgnoreCase) || a.Equals("no", StringComparison.OrdinalIgnoreCase)) create = false;
            }
            if (!create)
            {
                throw new InvalidOperationException(".env file required when running Xtraq.");
            }

            var disableBootstrap = Environment.GetEnvironmentVariable("XTRAQ_DISABLE_ENV_BOOTSTRAP");
            if (!string.IsNullOrWhiteSpace(disableBootstrap) && disableBootstrap != "0")
                throw new InvalidOperationException(".env bootstrap disabled via XTRAQ_DISABLE_ENV_BOOTSTRAP; required for Xtraq.");

            var bootstrapPath = Xtraq.Cli.ProjectEnvironmentBootstrapper.EnsureEnvAsync(projectRoot).GetAwaiter().GetResult();
            if (!File.Exists(bootstrapPath))
            {
                throw new InvalidOperationException(".env bootstrap failed - required for Xtraq.");
            }

            envFilePath = bootstrapPath;
            filePairs = LoadDotEnv(envFilePath);
            MergeTrackableConfig(projectRoot, filePairs);
            PublishEnvironmentVariables(filePairs, overwrite: true);
        }
        Validate(cfg, envFilePath);
        return cfg;
    }

    /// <summary>
    /// Validates the supplied configuration to ensure required settings are present and well-formed.
    /// </summary>
    /// <param name="cfg">The configuration instance to validate.</param>
    /// <param name="envFilePath">The resolved path to the .env file.</param>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
    private static void Validate(XtraqConfiguration cfg, string? envFilePath)
    {
        if (!string.IsNullOrWhiteSpace(cfg.NamespaceRoot))
        {
            var ns = cfg.NamespaceRoot.Trim();
            if (!System.Text.RegularExpressions.Regex.IsMatch(ns, @"^[A-Za-z_][A-Za-z0-9_\.]*$"))
                throw new InvalidOperationException($"XTRAQ_NAMESPACE '{ns}' invalid.");
            if (ns.Contains(".."))
                throw new InvalidOperationException("XTRAQ_NAMESPACE contains '..'.");
        }
        if (!string.IsNullOrWhiteSpace(cfg.OutputDir) && cfg.OutputDir.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new InvalidOperationException($"XTRAQ_OUTPUT_DIR '{cfg.OutputDir}' contains invalid chars.");
        if (!TrackableConfigExists(cfg.ProjectRoot))
            throw new InvalidOperationException("Xtraq project is not initialised. Run 'xtraq init'.");

        if (!string.IsNullOrWhiteSpace(envFilePath) && File.Exists(envFilePath))
        {
            var hasMarker = File.ReadLines(envFilePath).Any(l => l.Contains("XTRAQ_", StringComparison.OrdinalIgnoreCase));
            if (!hasMarker)
                throw new InvalidOperationException(".env file has no XTRAQ_ marker lines.");
        }
        if (string.IsNullOrWhiteSpace(cfg.GeneratorConnectionString))
            throw new InvalidOperationException("XTRAQ_GENERATOR_DB must be configured via environment variables or .env. Run 'xtraq init' to scaffold the file.");
        foreach (var schema in cfg.BuildSchemas)
        {
            var s = schema.Trim(); if (s.Length == 0) continue;
            // Allow hyphen-separated schema names (e.g. workflow-state) – sanitized to PascalCase via NamePolicy.Sanitize.
            if (!System.Text.RegularExpressions.Regex.IsMatch(s, "^[A-Za-z_][A-Za-z0-9_-]*$"))
                throw new InvalidOperationException($"XTRAQ_BUILD_SCHEMAS entry '{s}' invalid (pattern ^[A-Za-z_][A-Za-z0-9_-]*$).");
        }
    }

    /// <summary>
    /// Resolves the appropriate .env file path using the provided project root.
    /// </summary>
    /// <param name="projectRoot">The project root directory.</param>
    /// <returns>The preferred .env file path, or the default path when none exist.</returns>
    private static string? ResolveEnvFile(string projectRoot)
    {
        var primary = Path.Combine(projectRoot, ".env");
        if (File.Exists(primary)) return primary;
        var local = Path.Combine(projectRoot, ".env.local");
        if (File.Exists(local)) return local;
        return primary;
    }

    /// <summary>
    /// Loads key/value pairs from a .env file.
    /// </summary>
    /// <param name="path">The path to the .env file.</param>
    /// <returns>A dictionary of environment variable names and values.</returns>
    private static Dictionary<string, string?> LoadDotEnv(string? path)
    {
        var dict = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (path == null || !File.Exists(path)) return dict;
        foreach (var rawLine in File.ReadAllLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var idx = line.IndexOf('=');
            if (idx <= 0) continue;
            var key = line.Substring(0, idx).Trim();
            var value = line.Substring(idx + 1).Trim();
            if ((value.StartsWith('"') && value.EndsWith('"')) || (value.StartsWith('\'') && value.EndsWith('\'')))
                value = value.Substring(1, value.Length - 2);
            dict[key] = value;
        }
        return dict;
    }

    /// <summary>
    /// Merges values sourced from .xtraqconfig into the provided environment collection.
    /// </summary>
    /// <param name="projectRoot">Project root that hosts the tracked configuration file.</param>
    /// <param name="pairs">Environment key/value pairs populated from .env.</param>
    private static void MergeTrackableConfig(string? projectRoot, IDictionary<string, string?> pairs)
    {
        if (string.IsNullOrWhiteSpace(projectRoot) || pairs == null)
        {
            return;
        }

        try
        {
            var trackable = LoadTrackableConfigPairs(projectRoot);
            if (trackable.Count == 0)
            {
                return;
            }

            foreach (var entry in trackable)
            {
                var value = entry.Value;
                if (string.IsNullOrWhiteSpace(value))
                {
                    continue;
                }

                if (!pairs.TryGetValue(entry.Key, out var existing) || string.IsNullOrWhiteSpace(existing))
                {
                    pairs[entry.Key] = value;
                }
            }
        }
        catch (Exception ex)
        {
            if (Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE"))
            {
                Console.Error.WriteLine($"[xtraq] Warning: failed to merge .xtraqconfig: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Reads the tracked configuration JSON file and translates supported keys to environment equivalents.
    /// </summary>
    /// <param name="projectRoot">Project root directory.</param>
    /// <returns>Dictionary containing trackable configuration keys mapped to environment variables.</returns>
    private static Dictionary<string, string?> LoadTrackableConfigPairs(string projectRoot)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return map;
        }

        var configPath = Path.Combine(projectRoot, ".xtraqconfig");
        if (!File.Exists(configPath))
        {
            return map;
        }

        using var stream = File.OpenRead(configPath);
        using var document = JsonDocument.Parse(stream);
        var root = document.RootElement;

        if (root.TryGetProperty("Namespace", out var namespaceProperty) && namespaceProperty.ValueKind == JsonValueKind.String)
        {
            var value = namespaceProperty.GetString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                map["XTRAQ_NAMESPACE"] = value.Trim();
            }
        }

        if (root.TryGetProperty("OutputDir", out var outputDirProperty) && outputDirProperty.ValueKind == JsonValueKind.String)
        {
            var value = outputDirProperty.GetString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                map["XTRAQ_OUTPUT_DIR"] = value.Trim();
            }
        }

        if (root.TryGetProperty("TargetFramework", out var tfmProperty) && tfmProperty.ValueKind == JsonValueKind.String)
        {
            var value = tfmProperty.GetString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                map["XTRAQ_TFM"] = value.Trim();
            }
        }

        if (root.TryGetProperty("BuildSchemas", out var schemasProperty))
        {
            switch (schemasProperty.ValueKind)
            {
                case JsonValueKind.Array:
                    {
                        var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        var ordered = new List<string>();
                        foreach (var item in schemasProperty.EnumerateArray())
                        {
                            if (item.ValueKind != JsonValueKind.String)
                            {
                                continue;
                            }

                            var schema = item.GetString();
                            if (string.IsNullOrWhiteSpace(schema))
                            {
                                continue;
                            }

                            var trimmed = schema.Trim();
                            if (unique.Add(trimmed))
                            {
                                ordered.Add(trimmed);
                            }
                        }

                        if (ordered.Count > 0)
                        {
                            map["XTRAQ_BUILD_SCHEMAS"] = string.Join(',', ordered);
                        }

                        break;
                    }
                case JsonValueKind.String:
                    {
                        var value = schemasProperty.GetString();
                        if (!string.IsNullOrWhiteSpace(value))
                        {
                            map["XTRAQ_BUILD_SCHEMAS"] = value.Trim();
                        }

                        break;
                    }
            }
        }

        return map;
    }

    /// <summary>
    /// Publishes the supplied key/value pairs to the current process environment.
    /// </summary>
    /// <param name="pairs">The environment variable pairs to publish.</param>
    /// <param name="overwrite">If set to <c>true</c>, existing values are overwritten.</param>
    private static void PublishEnvironmentVariables(IDictionary<string, string?> pairs, bool overwrite = false)
    {
        if (pairs == null || pairs.Count == 0)
        {
            return;
        }

        foreach (var pair in pairs)
        {
            var key = pair.Key;
            if (string.IsNullOrWhiteSpace(key) || !key.StartsWith("XTRAQ_", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!overwrite && !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(key)))
            {
                continue;
            }

            var value = pair.Value;
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            Environment.SetEnvironmentVariable(key, value);
        }
    }

    /// <summary>
    /// Returns whether the tracked configuration file exists for the provided project root.
    /// </summary>
    /// <param name="projectRoot">Project root directory.</param>
    /// <returns><c>true</c> when .xtraqconfig exists; otherwise <c>false</c>.</returns>
    private static bool TrackableConfigExists(string? projectRoot)
    {
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return false;
        }

        var configPath = Path.Combine(projectRoot, ".xtraqconfig");
        return File.Exists(configPath);
    }

    /// <summary>
    /// Returns <c>null</c> when the provided string is null, empty, or whitespace.
    /// </summary>
    /// <param name="value">The value to inspect.</param>
    /// <returns>The original value when non-empty; otherwise <c>null</c>.</returns>
    private static string? NullIfEmpty(string value) => string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>
    /// Parses a delimited list of schema names into a normalized sequence.
    /// </summary>
    /// <param name="raw">The raw delimited string.</param>
    /// <returns>A read-only list of schema names.</returns>
    private static IReadOnlyList<string> ParseList(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return Array.Empty<string>();
        return raw.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                  .Select(p => p.Trim())
                  .Where(p => p.Length > 0)
                  .Distinct(StringComparer.OrdinalIgnoreCase)
                  .ToList();
    }

}
