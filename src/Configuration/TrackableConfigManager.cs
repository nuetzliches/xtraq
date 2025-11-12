using System.Text.Json.Serialization;

namespace Xtraq.Configuration;

/// <summary>
/// Manages the tracked configuration file (.xtraqconfig) that points to the active project root.
/// </summary>
internal static class TrackableConfigManager
{
    private const string ConfigFileName = ".xtraqconfig";
    private const int MaxRedirectDepth = 10;
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private static readonly string[] SchemaDelimiters = new[] { ",", ";" };

    /// <summary>
    /// Ensures a tracked configuration file exists and points to <paramref name="projectPath"/>.
    /// </summary>
    /// <param name="configDirectory">Directory that will host the .xtraqconfig file.</param>
    /// <param name="projectPath">Target project root that should be referenced.</param>
    public static void WriteProjectPath(string configDirectory, string projectPath)
    {
        if (string.IsNullOrWhiteSpace(configDirectory))
        {
            return;
        }

        Directory.CreateDirectory(configDirectory);

        var normalizedConfigDir = SafeGetFullPath(configDirectory);
        var storedPath = NormalizeStoredPath(normalizedConfigDir, string.IsNullOrWhiteSpace(projectPath) ? "." : projectPath);

        var payload = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["ProjectPath"] = storedPath
        };

        var json = JsonSerializer.Serialize(payload, SerializerOptions);
        var configPath = Path.Combine(normalizedConfigDir, ConfigFileName);
        File.WriteAllText(configPath, json + Environment.NewLine);
    }

    /// <summary>
    /// Convenience helper used during bootstrap scenarios where the project folder is the config directory.
    /// </summary>
    /// <param name="projectRoot">Active project root.</param>
    public static void WriteDefaultProjectPath(string projectRoot)
    {
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return;
        }

        var normalizedRoot = SafeGetFullPath(projectRoot);
        var configPath = Path.Combine(normalizedRoot, ConfigFileName);
        if (File.Exists(configPath))
        {
            return;
        }

        Write(normalizedRoot, new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Attempts to resolve the project root starting from <paramref name="startDirectory"/>.
    /// </summary>
    /// <param name="startDirectory">Directory used as the root for discovery.</param>
    /// <returns>Resolved project root when found; otherwise a normalized version of <paramref name="startDirectory"/>.</returns>
    public static string ResolveProjectRoot(string startDirectory)
    {
        if (string.IsNullOrWhiteSpace(startDirectory))
        {
            return Directory.GetCurrentDirectory();
        }

        var normalizedStart = SafeGetFullPath(startDirectory);
        var configDirectory = LocateConfigDirectory(normalizedStart);
        return ResolveRedirectTargets(configDirectory) ?? normalizedStart;
    }

    /// <summary>
    /// Locates the directory that owns the tracked configuration file starting at <paramref name="startDirectory"/>.
    /// </summary>
    /// <param name="startDirectory">Directory used as the root for discovery.</param>
    /// <returns>Directory containing .xtraqconfig or <paramref name="startDirectory"/> when none is found.</returns>
    public static string LocateConfigDirectory(string startDirectory)
    {
        if (string.IsNullOrWhiteSpace(startDirectory))
        {
            return Directory.GetCurrentDirectory();
        }

        var current = new DirectoryInfo(SafeGetFullPath(startDirectory));
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, ConfigFileName);
            if (File.Exists(candidate))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return SafeGetFullPath(startDirectory);
    }

    /// <summary>
    /// Reads the configured project path using <paramref name="configDirectory"/> as the base directory.
    /// </summary>
    /// <param name="configDirectory">Directory containing the .xtraqconfig file.</param>
    /// <returns>Resolved project root when successful; otherwise <c>null</c>.</returns>
    public static string? ResolveRedirectTargets(string configDirectory)
    {
        if (string.IsNullOrWhiteSpace(configDirectory))
        {
            return null;
        }

        var normalizedConfigDir = SafeGetFullPath(configDirectory);
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var currentDirectory = normalizedConfigDir;
        for (var depth = 0; depth < MaxRedirectDepth; depth++)
        {
            if (!visited.Add(currentDirectory))
            {
                return currentDirectory;
            }

            var candidateFile = Path.Combine(currentDirectory, ConfigFileName);
            if (!File.Exists(candidateFile))
            {
                return currentDirectory;
            }

            try
            {
                using var stream = File.OpenRead(candidateFile);
                using var document = JsonDocument.Parse(stream);
                if (!document.RootElement.TryGetProperty("ProjectPath", out var projectPathElement) || projectPathElement.ValueKind != JsonValueKind.String)
                {
                    return currentDirectory;
                }

                var raw = projectPathElement.GetString();
                if (string.IsNullOrWhiteSpace(raw))
                {
                    return currentDirectory;
                }

                var resolved = ResolveCandidatePath(currentDirectory, raw);
                if (string.IsNullOrWhiteSpace(resolved))
                {
                    return currentDirectory;
                }

                currentDirectory = resolved;
            }
            catch
            {
                return currentDirectory;
            }
        }

        return currentDirectory;
    }

    /// <summary>
    /// Attempts to read the project path without resolving nested redirects.
    /// </summary>
    /// <param name="configDirectory">Directory containing the .xtraqconfig file.</param>
    /// <returns>Stored project path string or <c>null</c> when unavailable.</returns>
    public static string? TryReadProjectPath(string configDirectory)
    {
        if (string.IsNullOrWhiteSpace(configDirectory))
        {
            return null;
        }

        var candidateFile = Path.Combine(SafeGetFullPath(configDirectory), ConfigFileName);
        if (!File.Exists(candidateFile))
        {
            return null;
        }

        try
        {
            using var stream = File.OpenRead(candidateFile);
            using var document = JsonDocument.Parse(stream);
            if (document.RootElement.TryGetProperty("ProjectPath", out var projectPathElement) && projectPathElement.ValueKind == JsonValueKind.String)
            {
                return projectPathElement.GetString();
            }
        }
        catch
        {
            // ignore malformed files and treat as missing
        }

        return null;
    }

    /// <summary>
    /// Writes a default project reference by reading the supplied .env file.
    /// </summary>
    /// <param name="projectRoot">Project root directory.</param>
    /// <param name="envPath">Absolute path to the .env file.</param>
    public static void WriteFromEnvFile(string projectRoot, string envPath)
    {
        if (string.IsNullOrWhiteSpace(projectRoot) || string.IsNullOrWhiteSpace(envPath))
        {
            return;
        }

        if (!File.Exists(envPath))
        {
            return;
        }

        var envValues = BuildEnvMap(File.ReadAllLines(envPath));
        Write(projectRoot, envValues);
    }

    /// <summary>
    /// Writes a legacy payload representing non-sensitive defaults captured from environment values.
    /// </summary>
    /// <param name="projectRoot">Target project root.</param>
    /// <param name="envValues">Environment key/value pairs gathered from .env.</param>
    public static void Write(string projectRoot, IReadOnlyDictionary<string, string?>? envValues)
    {
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return;
        }

        var normalizedRoot = SafeGetFullPath(projectRoot);
        var configPath = Path.Combine(normalizedRoot, ConfigFileName);

        if (File.Exists(configPath) && ContainsRedirect(configPath))
        {
            return;
        }

        Directory.CreateDirectory(normalizedRoot);

        var payload = BuildPayload(normalizedRoot, envValues);
        var json = JsonSerializer.Serialize(payload, SerializerOptions);
        File.WriteAllText(configPath, json + Environment.NewLine);
    }

    /// <summary>
    /// Builds an environment map by parsing .env file lines and extracting XTRAQ_* entries.
    /// </summary>
    /// <param name="lines">The raw lines from a .env file.</param>
    /// <returns>Dictionary keyed by environment variable names.</returns>
    public static Dictionary<string, string?> BuildEnvMap(IEnumerable<string> lines)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (lines is null)
        {
            return map;
        }

        foreach (var raw in lines)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var separatorIndex = line.IndexOf('=');
            if (separatorIndex <= 0)
            {
                continue;
            }

            var key = line.Substring(0, separatorIndex).Trim();
            if (!key.StartsWith("XTRAQ_", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = line.Substring(separatorIndex + 1).Trim();
            if (value.Length > 1 && ((value[0] == '"' && value[^1] == '"') || (value[0] == '\'' && value[^1] == '\'')))
            {
                value = value.Substring(1, value.Length - 2);
            }

            map[key] = value;
        }

        return map;
    }

    private static string NormalizeStoredPath(string configDirectory, string projectPath)
    {
        try
        {
            if (projectPath == ".")
            {
                return projectPath;
            }

            if (Path.IsPathRooted(projectPath))
            {
                var target = SafeGetFullPath(projectPath);
                if (target.StartsWith(configDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    var relative = Path.GetRelativePath(configDirectory, target);
                    return string.IsNullOrWhiteSpace(relative) ? "." : relative;
                }

                return target;
            }

            var combined = Path.GetFullPath(Path.Combine(configDirectory, projectPath));
            var relativePath = Path.GetRelativePath(configDirectory, combined);
            return string.IsNullOrWhiteSpace(relativePath) ? "." : relativePath;
        }
        catch
        {
            return projectPath;
        }
    }

    private static string SafeGetFullPath(string value)
    {
        try
        {
            return Path.GetFullPath(value);
        }
        catch
        {
            return value;
        }
    }

    private static string? ResolveCandidatePath(string configDirectory, string storedPath)
    {
        try
        {
            if (string.Equals(storedPath, ".", StringComparison.OrdinalIgnoreCase))
            {
                return SafeGetFullPath(configDirectory);
            }

            if (Path.IsPathRooted(storedPath))
            {
                var target = SafeGetFullPath(storedPath);
                return Directory.Exists(target) ? target : target;
            }

            var combined = Path.Combine(configDirectory, storedPath);
            return SafeGetFullPath(combined);
        }
        catch
        {
            return null;
        }
    }

    private static TrackableConfigPayload BuildPayload(string projectRoot, IReadOnlyDictionary<string, string?>? envValues)
    {
        var existing = ReadExistingPayload(projectRoot);
        var ns = ResolveValue(envValues, "XTRAQ_NAMESPACE") ?? existing?.Namespace;
        var outputDir = ResolveValue(envValues, "XTRAQ_OUTPUT_DIR") ?? existing?.OutputDir ?? "Xtraq";
        var targetFramework = ResolveValue(envValues, "XTRAQ_TARGET_FRAMEWORK") ?? existing?.TargetFramework ?? Constants.DefaultTargetFramework.ToFrameworkString();

        var buildSchemasRaw = ResolveValue(envValues, "XTRAQ_BUILD_SCHEMAS")
            ?? (existing is null ? null : string.Join(',', existing.BuildSchemas));
        var buildSchemas = ParseSchemas(buildSchemasRaw);

        return new TrackableConfigPayload
        {
            Namespace = ns,
            OutputDir = outputDir,
            TargetFramework = targetFramework,
            BuildSchemas = buildSchemas
        };
    }

    private static TrackableConfigPayload? ReadExistingPayload(string projectRoot)
    {
        var configPath = Path.Combine(projectRoot, ConfigFileName);
        if (!File.Exists(configPath))
        {
            return null;
        }

        try
        {
            using var stream = File.OpenRead(configPath);
            using var document = JsonDocument.Parse(stream);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (root.TryGetProperty("ProjectPath", out var redirectProperty) && redirectProperty.ValueKind == JsonValueKind.String)
            {
                var redirectValue = redirectProperty.GetString();
                if (!string.IsNullOrWhiteSpace(redirectValue) && !string.Equals(redirectValue.Trim(), ".", StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }
            }

            var ns = root.TryGetProperty("Namespace", out var nsElement) && nsElement.ValueKind == JsonValueKind.String ? nsElement.GetString() : null;
            var outputDir = root.TryGetProperty("OutputDir", out var outputElement) && outputElement.ValueKind == JsonValueKind.String ? outputElement.GetString() : null;
            var targetFramework = root.TryGetProperty("TargetFramework", out var targetElement) && targetElement.ValueKind == JsonValueKind.String ? targetElement.GetString() : null;
            var schemas = Array.Empty<string>();
            if (root.TryGetProperty("BuildSchemas", out var schemaElement) && schemaElement.ValueKind == JsonValueKind.Array)
            {
                schemas = schemaElement.EnumerateArray()
                    .Where(static item => item.ValueKind == JsonValueKind.String)
                    .Select(static item => item.GetString())
                    .Where(static value => !string.IsNullOrWhiteSpace(value))
                    .Select(static value => value!.Trim())
                    .Where(static value => value.Length > 0)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();
            }

            return new TrackableConfigPayload
            {
                Namespace = ns,
                OutputDir = outputDir,
                TargetFramework = targetFramework,
                BuildSchemas = schemas
            };
        }
        catch
        {
            return null;
        }
    }

    private static string? ResolveValue(IReadOnlyDictionary<string, string?>? values, string key)
    {
        if (values is null)
        {
            return null;
        }

        if (values.TryGetValue(key, out var raw) && !string.IsNullOrWhiteSpace(raw))
        {
            return raw.Trim();
        }

        return null;
    }

    private static IReadOnlyList<string> ParseSchemas(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Array.Empty<string>();
        }

        var parts = raw
            .Split(SchemaDelimiters, StringSplitOptions.RemoveEmptyEntries)
            .Select(static segment => segment.Trim())
            .Where(static segment => segment.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        return parts.Count == 0 ? Array.Empty<string>() : parts;
    }

    private static bool ContainsRedirect(string configPath)
    {
        try
        {
            using var stream = File.OpenRead(configPath);
            using var document = JsonDocument.Parse(stream);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (!document.RootElement.TryGetProperty("ProjectPath", out var projectPathElement))
            {
                return false;
            }

            if (projectPathElement.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            var raw = projectPathElement.GetString();
            if (string.IsNullOrWhiteSpace(raw))
            {
                return false;
            }

            return !string.Equals(raw.Trim(), ".", StringComparison.OrdinalIgnoreCase);
        }
        catch
        {
            return false;
        }
    }

    private sealed record TrackableConfigPayload
    {
        public string? Namespace { get; init; }
        public string? OutputDir { get; init; }
        public string? TargetFramework { get; init; }
        public IReadOnlyList<string> BuildSchemas { get; init; } = Array.Empty<string>();
    }
}
