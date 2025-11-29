using System.Text.Json.Serialization;

namespace Xtraq.Configuration;

/// <summary>
/// Manages the tracked configuration file (.xtraqconfig) that points to the active project root.
/// </summary>
internal static class TrackableConfigManager
{
    private const string ConfigFileName = ".xtraqconfig";
    private const string LocalConfigFileName = ".xtraqconfig.local";
    private const int MaxRedirectDepth = 10;
    private const string SchemaUrl = "https://nuetzliches.github.io/xtraq/xtraqconfig.schema.json";
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

        var payload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ProjectPath"] = storedPath
        };

        AttachSchemaMetadata(payload);

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
        AttachSchemaMetadata(payload);
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

    private static Dictionary<string, object?> BuildPayload(string projectRoot, IReadOnlyDictionary<string, string?>? envValues)
    {
        var existing = ReadConfigPayload(projectRoot, ConfigFileName);
        var ns = ResolveValue(envValues, "XTRAQ_NAMESPACE") ?? existing?.Namespace;
        var outputDir = ResolveValue(envValues, "XTRAQ_OUTPUT_DIR") ?? existing?.OutputDir ?? "Xtraq";
        var targetFramework = ResolveValue(envValues, "XTRAQ_TARGET_FRAMEWORK") ?? existing?.TargetFramework ?? Constants.DefaultTargetFramework.ToFrameworkString();

        var includeNullValues = existing?.ResultSet?.Json?.IncludeNullValues;
        var includeNullValuesRaw = ResolveValue(envValues, "XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES");
        if (includeNullValuesRaw is not null)
        {
            var parsed = ParseBoolean(includeNullValuesRaw);
            if (parsed.HasValue)
            {
                includeNullValues = parsed;
            }
        }

        var buildSchemasRaw = ResolveValue(envValues, "XTRAQ_BUILD_SCHEMAS")
            ?? (existing is null ? null : string.Join(',', existing.BuildSchemas));
        var buildSchemas = ParseSchemas(buildSchemasRaw);

        var apiModeRaw = ResolveValue(envValues, "XTRAQ_API_MODE") ?? existing?.Api?.Mode;
        var apiRequestsAutoBindRaw = ResolveValue(envValues, "XTRAQ_API_AUTOBIND")
            ?? (existing?.Api?.Requests is null ? null : string.Join(',', existing.Api.Requests.AutoBind));
        var apiRequestsAutoBind = ParseSchemas(apiRequestsAutoBindRaw);
        var apiRequestsAutoBindProceduresRaw = ResolveValue(envValues, "XTRAQ_API_AUTOBIND_PROCEDURES")
            ?? (existing?.Api?.Requests is null ? null : string.Join(',', existing.Api.Requests.AutoBindProcedures));
        var apiRequestsAutoBindProcedures = ParseSchemas(apiRequestsAutoBindProceduresRaw);

        var entityFrameworkRaw = ResolveValue(envValues, "XTRAQ_ENTITY_FRAMEWORK_ENABLED");
        var entityFrameworkEnabled = existing?.EntityFramework?.Enabled;
        if (entityFrameworkRaw is not null)
        {
            var parsed = ParseBoolean(entityFrameworkRaw);
            if (parsed.HasValue)
            {
                entityFrameworkEnabled = parsed;
            }
        }

        var payload = new TrackableConfigPayload
        {
            Namespace = ns,
            OutputDir = outputDir,
            TargetFramework = targetFramework,
            BuildSchemas = buildSchemas,
            Api = new ApiPayload
            {
                Mode = apiModeRaw,
                Requests = new ApiRequestPayload
                {
                    AutoBind = apiRequestsAutoBind,
                    AutoBindProcedures = apiRequestsAutoBindProcedures
                }
            },
            EntityFramework = new EntityFrameworkPayload { Enabled = entityFrameworkEnabled },
            ResultSet = new ResultSetPayload { Json = new JsonPayload { IncludeNullValues = includeNullValues } }
        };

        return NormalizePayload(payload);
    }

    private static Dictionary<string, object?> NormalizePayload(TrackableConfigPayload payload)
    {
        var map = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(payload.Namespace))
        {
            map["Namespace"] = payload.Namespace.Trim();
        }

        if (!string.IsNullOrWhiteSpace(payload.OutputDir) && !string.Equals(payload.OutputDir.Trim(), "Xtraq", StringComparison.OrdinalIgnoreCase))
        {
            map["OutputDir"] = payload.OutputDir.Trim();
        }

        if (!string.IsNullOrWhiteSpace(payload.TargetFramework) && !string.Equals(payload.TargetFramework.Trim(), Constants.DefaultTargetFramework.ToFrameworkString(), StringComparison.OrdinalIgnoreCase))
        {
            map["TargetFramework"] = payload.TargetFramework.Trim();
        }

        if (payload.BuildSchemas.Count > 0)
        {
            map["BuildSchemas"] = payload.BuildSchemas;
        }

        var api = NormalizeApiPayload(payload.Api);
        if (api is not null)
        {
            map["Api"] = api;
        }

        var entityFramework = NormalizeEntityFrameworkPayload(payload.EntityFramework);
        if (entityFramework is not null)
        {
            map["EntityFramework"] = entityFramework;
        }

        var resultSet = NormalizeResultSetPayload(payload.ResultSet);
        if (resultSet is not null)
        {
            map["ResultSet"] = resultSet;
        }

        return map;
    }

    private static Dictionary<string, object?>? NormalizeApiPayload(ApiPayload? api)
    {
        if (api is null)
        {
            return null;
        }

        var apiMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(api.Mode) && !api.Mode.Trim().Equals("None", StringComparison.OrdinalIgnoreCase))
        {
            apiMap["Mode"] = api.Mode.Trim();
        }

        var requests = NormalizeApiRequests(api.Requests);
        if (requests is not null)
        {
            apiMap["Requests"] = requests;
        }

        return apiMap.Count == 0 ? null : apiMap;
    }

    private static Dictionary<string, object?>? NormalizeApiRequests(ApiRequestPayload? requests)
    {
        if (requests is null)
        {
            return null;
        }

        var requestsMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (requests.AutoBind.Count > 0)
        {
            requestsMap["AutoBind"] = requests.AutoBind;
        }

        if (requests.AutoBindProcedures.Count > 0)
        {
            requestsMap["AutoBindProcedures"] = requests.AutoBindProcedures;
        }

        return requestsMap.Count == 0 ? null : requestsMap;
    }

    private static void AttachSchemaMetadata(IDictionary<string, object?> payload)
    {
        if (payload is null)
        {
            return;
        }

        if (!payload.ContainsKey("$schema"))
        {
            payload["$schema"] = SchemaUrl;
        }
    }

    private static Dictionary<string, object?>? NormalizeEntityFrameworkPayload(EntityFrameworkPayload? entityFramework)
    {
        if (entityFramework?.Enabled != true)
        {
            return null;
        }

        return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Enabled"] = true
        };
    }

    private static Dictionary<string, object?>? NormalizeResultSetPayload(ResultSetPayload? resultSet)
    {
        if (resultSet?.Json?.IncludeNullValues != true)
        {
            return null;
        }

        return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Json"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["IncludeNullValues"] = true
            }
        };
    }

    private static TrackableConfigPayload? ReadConfigPayload(string baseDirectory, string fileName)
    {
        if (string.IsNullOrWhiteSpace(baseDirectory) || string.IsNullOrWhiteSpace(fileName))
        {
            return null;
        }

        var configPath = Path.Combine(baseDirectory, fileName);
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

            var ns = TryReadTrimmedString(root, "Namespace");
            var outputDir = TryReadTrimmedString(root, "OutputDir");
            var targetFramework = TryReadTrimmedString(root, "TargetFramework");
            var schemas = ReadSchemaArray(root, "BuildSchemas");

            var api = ReadApiSettings(root);
            var entityFramework = ReadEntityFrameworkSettings(root);
            var resultSet = ReadResultSetSettings(root);

            return new TrackableConfigPayload
            {
                Namespace = ns,
                OutputDir = outputDir,
                TargetFramework = targetFramework,
                BuildSchemas = schemas,
                Api = api,
                EntityFramework = entityFramework,
                ResultSet = resultSet
            };
        }
        catch
        {
            return null;
        }
    }

    private static TrackableConfigPayload? MergePayloads(TrackableConfigPayload? baseline, TrackableConfigPayload? overrides)
    {
        if (baseline is null && overrides is null)
        {
            return null;
        }

        if (baseline is null)
        {
            return overrides is null ? null : ClonePayload(overrides);
        }

        if (overrides is null)
        {
            return ClonePayload(baseline);
        }

        var namespaceValue = SelectString(overrides.Namespace, baseline.Namespace);
        var outputDirValue = SelectString(overrides.OutputDir, baseline.OutputDir);
        var targetFrameworkValue = SelectString(overrides.TargetFramework, baseline.TargetFramework);
        var jsonIncludeNullValues = overrides.ResultSet?.Json?.IncludeNullValues ?? baseline.ResultSet?.Json?.IncludeNullValues;
        var apiMode = SelectString(overrides.Api?.Mode, baseline.Api?.Mode);
        var apiAutoBind = overrides.Api?.Requests?.AutoBind.Count > 0
            ? overrides.Api!.Requests!.AutoBind
            : baseline.Api?.Requests?.AutoBind ?? Array.Empty<string>();
        var apiAutoBindProcedures = overrides.Api?.Requests?.AutoBindProcedures.Count > 0
            ? overrides.Api!.Requests!.AutoBindProcedures
            : baseline.Api?.Requests?.AutoBindProcedures ?? Array.Empty<string>();
        var entityFramework = overrides.EntityFramework?.Enabled ?? baseline.EntityFramework?.Enabled;
        var schemas = overrides.BuildSchemas.Count > 0
            ? overrides.BuildSchemas
            : baseline.BuildSchemas;

        return new TrackableConfigPayload
        {
            Namespace = namespaceValue,
            OutputDir = outputDirValue,
            TargetFramework = targetFrameworkValue,
            BuildSchemas = schemas.Count > 0 ? schemas.ToArray() : Array.Empty<string>(),
            Api = new ApiPayload
            {
                Mode = apiMode,
                Requests = new ApiRequestPayload
                {
                    AutoBind = apiAutoBind.Count > 0 ? apiAutoBind.ToArray() : Array.Empty<string>(),
                    AutoBindProcedures = apiAutoBindProcedures.Count > 0 ? apiAutoBindProcedures.ToArray() : Array.Empty<string>()
                }
            },
            EntityFramework = new EntityFrameworkPayload { Enabled = entityFramework },
            ResultSet = new ResultSetPayload { Json = new JsonPayload { IncludeNullValues = jsonIncludeNullValues } }
        };
    }

    private static string? SelectString(string? primary, string? fallback)
    {
        if (!string.IsNullOrWhiteSpace(primary))
        {
            return primary.Trim();
        }

        return string.IsNullOrWhiteSpace(fallback) ? null : fallback.Trim();
    }

    private static TrackableConfigPayload ClonePayload(TrackableConfigPayload source)
    {
        return new TrackableConfigPayload
        {
            Namespace = string.IsNullOrWhiteSpace(source.Namespace) ? null : source.Namespace.Trim(),
            OutputDir = string.IsNullOrWhiteSpace(source.OutputDir) ? null : source.OutputDir.Trim(),
            TargetFramework = string.IsNullOrWhiteSpace(source.TargetFramework) ? null : source.TargetFramework.Trim(),
            BuildSchemas = source.BuildSchemas.Count > 0 ? source.BuildSchemas.ToArray() : Array.Empty<string>(),
            Api = source.Api is null
                ? null
                : new ApiPayload
                {
                    Mode = source.Api.Mode,
                    Requests = source.Api.Requests is null
                        ? null
                        : new ApiRequestPayload
                        {
                            AutoBind = source.Api.Requests.AutoBind.Count > 0 ? source.Api.Requests.AutoBind.ToArray() : Array.Empty<string>(),
                            AutoBindProcedures = source.Api.Requests.AutoBindProcedures.Count > 0 ? source.Api.Requests.AutoBindProcedures.ToArray() : Array.Empty<string>()
                        }
                },
            EntityFramework = source.EntityFramework is null ? null : new EntityFrameworkPayload { Enabled = source.EntityFramework.Enabled },
            ResultSet = source.ResultSet is null ? null : new ResultSetPayload { Json = source.ResultSet.Json is null ? null : new JsonPayload { IncludeNullValues = source.ResultSet.Json.IncludeNullValues } }
        };
    }

    private static ApiPayload? ReadApiSettings(JsonElement root)
    {
        if (!root.TryGetProperty("Api", out var apiElement) || apiElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var mode = TryReadTrimmedString(apiElement, "Mode");
        ApiRequestPayload? requests = null;
        if (apiElement.TryGetProperty("Requests", out var requestsElement) && requestsElement.ValueKind == JsonValueKind.Object)
        {
            requests = new ApiRequestPayload
            {
                AutoBind = ReadStringArray(requestsElement, "AutoBind"),
                AutoBindProcedures = ReadStringArray(requestsElement, "AutoBindProcedures")
            };
        }

        return new ApiPayload { Mode = mode, Requests = requests };
    }

    private static EntityFrameworkPayload? ReadEntityFrameworkSettings(JsonElement root)
    {
        if (!root.TryGetProperty("EntityFramework", out var efElement) || efElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return new EntityFrameworkPayload { Enabled = TryReadNullableBoolean(efElement, "Enabled") };
    }

    private static ResultSetPayload? ReadResultSetSettings(JsonElement root)
    {
        if (!root.TryGetProperty("ResultSet", out var rsElement) || rsElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        JsonPayload? json = null;
        if (rsElement.TryGetProperty("Json", out var jsonElement) && jsonElement.ValueKind == JsonValueKind.Object)
        {
            json = new JsonPayload { IncludeNullValues = TryReadNullableBoolean(jsonElement, "IncludeNullValues") };
        }

        return new ResultSetPayload { Json = json };
    }

    private static IReadOnlyList<string> ReadStringArray(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var arrayElement))
        {
            return Array.Empty<string>();
        }

        if (arrayElement.ValueKind == JsonValueKind.String)
        {
            var single = arrayElement.GetString();
            return string.IsNullOrWhiteSpace(single) ? Array.Empty<string>() : new[] { single.Trim() };
        }

        if (arrayElement.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<string>();
        }

        var buffer = new List<string>();
        foreach (var item in arrayElement.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            var raw = item.GetString();
            if (string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            var value = raw.Trim();
            if (value.Length == 0)
            {
                continue;
            }

            if (!buffer.Any(existing => string.Equals(existing, value, StringComparison.OrdinalIgnoreCase)))
            {
                buffer.Add(value);
            }
        }

        return buffer.Count == 0 ? Array.Empty<string>() : buffer.ToArray();
    }

    private static string? TryReadTrimmedString(JsonElement root, string propertyName)
    {
        if (root.TryGetProperty(propertyName, out var element) && element.ValueKind == JsonValueKind.String)
        {
            var raw = element.GetString();
            return string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
        }

        return null;
    }

    private static bool? TryReadNullableBoolean(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var element))
        {
            return null;
        }

        return element.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => ParseBoolean(element.GetString()),
            JsonValueKind.Number => element.TryGetInt32(out var number) ? number != 0 : null,
            _ => null
        };
    }

    private static IReadOnlyList<string> ReadSchemaArray(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var arrayElement) || arrayElement.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<string>();
        }

        var buffer = new List<string>();
        foreach (var item in arrayElement.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            var raw = item.GetString();
            if (string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            var value = raw.Trim();
            if (value.Length == 0)
            {
                continue;
            }

            if (!buffer.Any(existing => string.Equals(existing, value, StringComparison.OrdinalIgnoreCase)))
            {
                buffer.Add(value);
            }
        }

        return buffer.Count == 0 ? Array.Empty<string>() : buffer.ToArray();
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

    private static bool? ParseBoolean(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var value = raw.Trim();
        if (value.Length == 1)
        {
            return value switch
            {
                "1" => true,
                "0" => false,
                _ => null
            };
        }

        if (value.Equals("true", StringComparison.OrdinalIgnoreCase)
            || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || value.Equals("on", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (value.Equals("false", StringComparison.OrdinalIgnoreCase)
            || value.Equals("no", StringComparison.OrdinalIgnoreCase)
            || value.Equals("off", StringComparison.OrdinalIgnoreCase))
        {
            return false;
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

    /// <summary>
    /// Reads tracked defaults from the project configuration file and converts them to environment keys.
    /// </summary>
    /// <param name="projectRoot">Resolved project root that hosts the tracked configuration.</param>
    /// <returns>Dictionary containing tracked defaults keyed by their corresponding environment variables.</returns>
    public static IReadOnlyDictionary<string, string?> ReadDefaults(string projectRoot)
    {
        var defaults = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return defaults;
        }

        var normalizedRoot = SafeGetFullPath(projectRoot);
        var trackedPayload = ReadConfigPayload(normalizedRoot, ConfigFileName);
        var localPayload = ReadConfigPayload(normalizedRoot, LocalConfigFileName);
        var payload = MergePayloads(trackedPayload, localPayload);

        if (payload is null)
        {
            return defaults;
        }

        if (!string.IsNullOrWhiteSpace(payload.Namespace))
        {
            defaults["XTRAQ_NAMESPACE"] = payload.Namespace!.Trim();
        }

        if (!string.IsNullOrWhiteSpace(payload.OutputDir))
        {
            defaults["XTRAQ_OUTPUT_DIR"] = payload.OutputDir!.Trim();
        }

        if (!string.IsNullOrWhiteSpace(payload.TargetFramework))
        {
            defaults["XTRAQ_TARGET_FRAMEWORK"] = payload.TargetFramework!.Trim();
        }

        if (payload.BuildSchemas.Count > 0)
        {
            defaults["XTRAQ_BUILD_SCHEMAS"] = string.Join(',', payload.BuildSchemas);
        }

        if (payload.ResultSet?.Json?.IncludeNullValues is not null)
        {
            defaults["XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES"] = payload.ResultSet.Json.IncludeNullValues!.Value ? "1" : "0";
        }

        if (!string.IsNullOrWhiteSpace(payload.Api?.Mode))
        {
            defaults["XTRAQ_API_MODE"] = payload.Api!.Mode!.Trim();
            if (string.Equals(payload.Api.Mode, "Minimal", StringComparison.OrdinalIgnoreCase))
            {
                defaults["XTRAQ_API_MODE_MINIMAL"] = "1";
            }
        }

        if (payload.Api?.Requests?.AutoBind is { Count: > 0 })
        {
            defaults["XTRAQ_API_AUTOBIND"] = string.Join(',', payload.Api.Requests.AutoBind);
        }

        if (payload.Api?.Requests?.AutoBindProcedures is { Count: > 0 })
        {
            defaults["XTRAQ_API_AUTOBIND_PROCEDURES"] = string.Join(',', payload.Api.Requests.AutoBindProcedures);
        }

        if (payload.EntityFramework?.Enabled is not null)
        {
            defaults["XTRAQ_ENTITY_FRAMEWORK_ENABLED"] = payload.EntityFramework.Enabled!.Value ? "1" : "0";
        }

        return defaults;
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
        public ApiPayload? Api { get; init; }
        public EntityFrameworkPayload? EntityFramework { get; init; }
        public ResultSetPayload? ResultSet { get; init; }
    }

    private sealed record ApiPayload
    {
        public string? Mode { get; init; }
        public ApiRequestPayload? Requests { get; init; }
    }

    private sealed record ApiRequestPayload
    {
        public IReadOnlyList<string> AutoBind { get; init; } = Array.Empty<string>();
        public IReadOnlyList<string> AutoBindProcedures { get; init; } = Array.Empty<string>();
    }

    private sealed record EntityFrameworkPayload
    {
        public bool? Enabled { get; init; }
    }

    private sealed record ResultSetPayload
    {
        public JsonPayload? Json { get; init; }
    }

    private sealed record JsonPayload
    {
        public bool? IncludeNullValues { get; init; }
    }
}
