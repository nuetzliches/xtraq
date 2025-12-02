namespace Xtraq.Configuration;

/// <summary>
/// Strongly typed configuration for generator with precedence:
/// CLI overrides > Environment Variables > .env file > .xtraqconfig
/// </summary>
public sealed class XtraqConfiguration
{
    /// <summary>
    /// Gets the connection string used for metadata discovery.
    /// </summary>
    public string? GeneratorConnectionString { get; init; }
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
    /// Positive allow-list for schemas to generate (XTRAQ_BUILD_SCHEMAS). Empty => include every schema discovered by the snapshot.
    /// </summary>
    public IReadOnlyList<string> BuildSchemas { get; init; } = Array.Empty<string>();
    /// <summary>
    /// Gets a value indicating whether API integrations (request DTOs, table-type request rows, DI endpoints) are enabled.
    /// </summary>
    public bool ApiEnabled { get; init; }
    /// <summary>
    /// Optional global parameter auto-binding list applied to API endpoints (schema-qualified filters are configured separately).
    /// </summary>
    public IReadOnlyList<string> ApiAutoBindParameters { get; init; } = Array.Empty<string>();
    /// <summary>
    /// Optional allow-list of schema-qualified procedures that should participate in automatic parameter auto-binding.
    /// </summary>
    public IReadOnlyList<string> ApiAutoBindProcedures { get; init; } = Array.Empty<string>();
    /// <summary>
    /// Gets a value indicating whether Entity Framework Core integration helpers should be enabled for generated projects.
    /// </summary>
    public bool EntityFrameworkEnabled { get; init; }
    /// <summary>
    /// Gets a value indicating whether result-set JSON should emit <c>[JsonIncludeNullValues]</c> attributes.
    /// </summary>
    public bool ResultSetJsonIncludeNullValues { get; init; }
    /// <summary>
    /// Loads the environment configuration by merging CLI overrides, environment variables, and .env settings.
    /// </summary>
    /// <param name="projectRoot">Optional project root used to resolve relative paths.</param>
    /// <param name="cliOverrides">Optional CLI-supplied key/value overrides.</param>
    /// <param name="explicitConfigPath">Optional explicit configuration file path.</param>
    /// <param name="requireGeneratorConnection">When <c>true</c>, XTRAQ_GENERATOR_DB must be present (snapshot/refresh). Set to <c>false</c> for offline build-only flows.</param>
    /// <returns>A populated <see cref="XtraqConfiguration"/> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when required configuration cannot be resolved.</exception>
    public static XtraqConfiguration Load(string? projectRoot = null, IDictionary<string, string?>? cliOverrides = null, string? explicitConfigPath = null, bool requireGeneratorConnection = false)
    {
        var verbose = Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE");

        static string DetermineSearchBase(string? rootHint, string? configHint)
        {
            string? candidate = null;

            if (!string.IsNullOrWhiteSpace(configHint))
            {
                candidate = configHint.Trim();
            }
            else if (!string.IsNullOrWhiteSpace(rootHint))
            {
                candidate = rootHint.Trim();
            }

            if (string.IsNullOrWhiteSpace(candidate))
            {
                return Directory.GetCurrentDirectory();
            }

            try
            {
                candidate = Path.GetFullPath(candidate);
            }
            catch
            {
                // fall back to raw candidate when normalisation fails
            }

            if (File.Exists(candidate))
            {
                return Path.GetDirectoryName(candidate) ?? Directory.GetCurrentDirectory();
            }

            return candidate;
        }

        var searchBase = DetermineSearchBase(projectRoot, explicitConfigPath);
        var configDirectory = TrackableConfigManager.LocateConfigDirectory(searchBase);
        var configFilePath = Path.Combine(configDirectory, ".xtraqconfig");
        var localConfigFilePath = Path.Combine(configDirectory, ".xtraqconfig.local");
        var effectiveConfigPath = File.Exists(localConfigFilePath) ? localConfigFilePath : configFilePath;

        if (!File.Exists(configFilePath))
        {
            throw new InvalidOperationException("Xtraq project is not initialised. Run 'xtraq init'.");
        }

        ValidateConfigSchema(configFilePath);
        if (File.Exists(localConfigFilePath))
        {
            ValidateConfigSchema(localConfigFilePath);
        }

        projectRoot = TrackableConfigManager.ResolveRedirectTargets(configDirectory) ?? configDirectory;
        var trackedSnapshot = TrackableConfigManager.ReadMergedConfiguration(projectRoot);
        var trackedDefaults = BuildSnapshotMap(trackedSnapshot);

        var envFilePath = ResolveEnvFile(projectRoot);
        var filePairs = LoadDotEnv(envFilePath);
        PublishEnvironmentVariables(filePairs, overwrite: true);

        try
        {
            if (!string.IsNullOrWhiteSpace(projectRoot))
            {
                var normalized = Path.GetFullPath(projectRoot);
                Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", normalized);

                if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT")))
                {
                    Environment.SetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT", Path.Combine(normalized, ".xtraq"));
                }
            }
        }
        catch
        {
            if (verbose) Console.Error.WriteLine("[xtraq] Warning: Failed to publish project root environment variables.");
        }

        string Get(string key, bool allowProcessEnvironment = true, bool allowEnvFile = true)
        {
            if (cliOverrides != null && cliOverrides.TryGetValue(key, out var fromCli) && !string.IsNullOrWhiteSpace(fromCli)) return fromCli!;
            if (allowProcessEnvironment)
            {
                var fromProcess = Environment.GetEnvironmentVariable(key);
                if (!string.IsNullOrWhiteSpace(fromProcess)) return fromProcess!;
            }
            if (allowEnvFile && filePairs.TryGetValue(key, out var fromFile) && !string.IsNullOrWhiteSpace(fromFile)) return fromFile!;
            if (trackedDefaults.TryGetValue(key, out var fromTracked) && !string.IsNullOrWhiteSpace(fromTracked)) return fromTracked!;
            return string.Empty;
        }

        var fullConn = NullIfEmpty(Get("XTRAQ_GENERATOR_DB"));
        var buildSchemasList = ParseList(NullIfEmpty(Get("XTRAQ_BUILD_SCHEMAS", allowProcessEnvironment: false, allowEnvFile: false)));
        if (string.IsNullOrWhiteSpace(fullConn) && verbose)
        {
            Console.Error.WriteLine("[xtraq] Warning: XTRAQ_GENERATOR_DB is not set. Run 'xtraq init' or provide the connection string via environment variables.");
        }

        // Namespace and output directory are tracked exclusively in .xtraqconfig/.xtraqconfig.local
        // to avoid per-machine overrides leaking in via environment variables.
        var namespaceValue = NullIfEmpty(Get("XTRAQ_NAMESPACE", allowProcessEnvironment: false, allowEnvFile: false))?.Trim();
        var outputDirResolved = NullIfEmpty(Get("XTRAQ_OUTPUT_DIR", allowProcessEnvironment: false, allowEnvFile: false)) ?? "Xtraq";
        var apiEnabled = trackedSnapshot?.ApiEnabled ?? false;
        var autoBindParameters = ParseList(NullIfEmpty(Get("XTRAQ_API_AUTOBIND", allowProcessEnvironment: false, allowEnvFile: false)));
        var autoBindProcedures = ParseList(NullIfEmpty(Get("XTRAQ_API_AUTOBIND_PROCEDURES", allowProcessEnvironment: false, allowEnvFile: false)));
        var enableEntityFramework = trackedSnapshot?.EntityFrameworkEnabled ?? false;
        var emitJsonIncludeNullValues = Xtraq.Utils.EnvironmentHelper.EqualsTrue(Get("XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES", allowProcessEnvironment: false, allowEnvFile: false));

        var cfg = new XtraqConfiguration
        {
            GeneratorConnectionString = fullConn,
            NamespaceRoot = namespaceValue,
            OutputDir = outputDirResolved,
            ConfigPath = File.Exists(effectiveConfigPath) ? effectiveConfigPath : null,
            BuildSchemas = buildSchemasList,
            ProjectRoot = projectRoot,
            ApiEnabled = apiEnabled,
            ApiAutoBindParameters = autoBindParameters,
            ApiAutoBindProcedures = autoBindProcedures,
            EntityFrameworkEnabled = enableEntityFramework,
            ResultSetJsonIncludeNullValues = emitJsonIncludeNullValues
        };

        if (string.IsNullOrEmpty(envFilePath) || !File.Exists(envFilePath))
        {
            if (verbose) Console.WriteLine("[xtraq] No .env file found; continuing without env bootstrap.");
        }
        Validate(cfg, envFilePath, requireGeneratorConnection);
        return cfg;
    }

    /// <summary>
    /// Validates the supplied configuration to ensure required settings are present and well-formed.
    /// </summary>
    /// <param name="cfg">The configuration instance to validate.</param>
    /// <param name="envFilePath">The resolved path to the .env file.</param>
    /// <param name="requireGeneratorConnection">When true, XTRAQ_GENERATOR_DB is required.</param>
    /// <exception cref="InvalidOperationException">Thrown when validation fails.</exception>
    private static void Validate(XtraqConfiguration cfg, string? envFilePath, bool requireGeneratorConnection)
    {
        if (string.IsNullOrWhiteSpace(cfg.NamespaceRoot))
            throw new InvalidOperationException("XTRAQ_NAMESPACE is required. Run 'xtraq init' to capture the namespace.");

        var ns = cfg.NamespaceRoot.Trim();
        if (!System.Text.RegularExpressions.Regex.IsMatch(ns, @"^[A-Za-z_][A-Za-z0-9_\.]*$"))
            throw new InvalidOperationException($"XTRAQ_NAMESPACE '{ns}' invalid.");
        if (ns.Contains(".."))
            throw new InvalidOperationException("XTRAQ_NAMESPACE contains '..'.");
        if (!string.IsNullOrWhiteSpace(cfg.OutputDir) && cfg.OutputDir.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new InvalidOperationException($"XTRAQ_OUTPUT_DIR '{cfg.OutputDir}' contains invalid chars.");
        if (string.IsNullOrWhiteSpace(cfg.ConfigPath) || !File.Exists(cfg.ConfigPath))
            throw new InvalidOperationException("Xtraq project is not initialised. Run 'xtraq init'.");

        if (!string.IsNullOrWhiteSpace(envFilePath) && File.Exists(envFilePath))
        {
            // .env is optional; marker check removed to allow offline/DB-less init.
        }
        if (requireGeneratorConnection && string.IsNullOrWhiteSpace(cfg.GeneratorConnectionString))
            throw new InvalidOperationException("XTRAQ_GENERATOR_DB is required for snapshot/refresh operations.");
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

    private static void ValidateConfigSchema(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        try
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return;
            }
        }
        catch (JsonException)
        {
            // ignore malformed JSON here; the existing validation will handle errors later
        }
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
            if (!EnvironmentVariablePolicy.IsEnvFileAllowedKey(key))
            {
                continue;
            }

            var value = line.Substring(idx + 1).Trim();
            if ((value.StartsWith('"') && value.EndsWith('"')) || (value.StartsWith('\'') && value.EndsWith('\'')))
                value = value.Substring(1, value.Length - 2);
            dict[key] = value;
        }
        return dict;
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

            if (!EnvironmentVariablePolicy.IsEnvFileAllowedKey(key))
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

    private static IReadOnlyDictionary<string, string?> BuildSnapshotMap(TrackableConfigSnapshot? snapshot)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (snapshot is null)
        {
            return map;
        }

        static void Set(Dictionary<string, string?> target, string key, string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            target[key] = value.Trim();
        }

        Set(map, "XTRAQ_NAMESPACE", snapshot.Namespace);
        Set(map, "XTRAQ_OUTPUT_DIR", snapshot.OutputDir);
        Set(map, "XTRAQ_TARGET_FRAMEWORK", snapshot.TargetFramework);

        if (snapshot.BuildSchemas.Count > 0)
        {
            map["XTRAQ_BUILD_SCHEMAS"] = string.Join(',', snapshot.BuildSchemas);
        }

        if (snapshot.ApiAutoBind.Count > 0)
        {
            map["XTRAQ_API_AUTOBIND"] = string.Join(',', snapshot.ApiAutoBind);
        }

        if (snapshot.ApiAutoBindProcedures.Count > 0)
        {
            map["XTRAQ_API_AUTOBIND_PROCEDURES"] = string.Join(',', snapshot.ApiAutoBindProcedures);
        }

        if (snapshot.ResultSetJsonIncludeNullValues is not null)
        {
            map["XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES"] = snapshot.ResultSetJsonIncludeNullValues.Value ? "1" : "0";
        }

        return map;
    }

}
