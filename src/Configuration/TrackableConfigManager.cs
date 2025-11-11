namespace Xtraq.Configuration;

/// <summary>
/// Manages the tracked configuration file (.xtraqconfig) derived from non-sensitive .env values.
/// </summary>
internal static class TrackableConfigManager
{
    /// <summary>
    /// Builds a key/value map from raw .env lines, filtering comments and malformed entries.
    /// </summary>
    /// <param name="envLines">Enumerable sequence containing .env lines.</param>
    /// <returns>Dictionary containing environment variable keys and values.</returns>
    public static Dictionary<string, string?> BuildEnvMap(IEnumerable<string> envLines)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (envLines == null)
        {
            return map;
        }

        foreach (var rawLine in envLines)
        {
            if (string.IsNullOrEmpty(rawLine))
            {
                continue;
            }

            var trimmed = rawLine.Trim();
            if (trimmed.Length == 0 || trimmed.StartsWith("#", StringComparison.Ordinal))
            {
                continue;
            }

            var separatorIndex = trimmed.IndexOf('=');
            if (separatorIndex <= 0)
            {
                continue;
            }

            var key = trimmed[..separatorIndex].Trim();
            if (key.Length == 0)
            {
                continue;
            }

            var value = trimmed[(separatorIndex + 1)..].Trim();
            map[key] = value;
        }

        return map;
    }

    /// <summary>
    /// Writes the tracked configuration JSON file using the supplied environment values.
    /// </summary>
    /// <param name="projectRoot">Project root directory.</param>
    /// <param name="envValues">Resolved environment variable values to persist.</param>
    public static void Write(string projectRoot, IReadOnlyDictionary<string, string?> envValues)
    {
        if (string.IsNullOrWhiteSpace(projectRoot) || envValues is null)
        {
            return;
        }

        Directory.CreateDirectory(projectRoot);

        var existing = LoadTrackableConfigPairs(projectRoot);

        string? ResolveValue(string key)
        {
            if (envValues.TryGetValue(key, out var candidate) && !string.IsNullOrWhiteSpace(candidate))
            {
                return candidate.Trim();
            }

            if (existing.TryGetValue(key, out var current) && !string.IsNullOrWhiteSpace(current))
            {
                return current.Trim();
            }

            return null;
        }

        var schemasRaw = ResolveValue("XTRAQ_BUILD_SCHEMAS");
        var buildSchemas = Array.Empty<string>();
        if (!string.IsNullOrWhiteSpace(schemasRaw))
        {
            var buffer = new List<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var schema in schemasRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (schema.Length == 0 || !seen.Add(schema))
                {
                    continue;
                }

                buffer.Add(schema);
            }

            buildSchemas = buffer.ToArray();
        }

        var nsValue = ResolveValue("XTRAQ_NAMESPACE");
        var outputDirValue = ResolveValue("XTRAQ_OUTPUT_DIR");
        var tfmValue = ResolveValue("XTRAQ_TFM");

        var payload = new
        {
            Namespace = nsValue ?? string.Empty,
            OutputDir = string.IsNullOrWhiteSpace(outputDirValue) ? "Xtraq" : outputDirValue!,
            TargetFramework = tfmValue ?? string.Empty,
            BuildSchemas = buildSchemas
        };

        var configPath = Path.Combine(projectRoot, ".xtraqconfig");
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(configPath, json + Environment.NewLine);
    }

    /// <summary>
    /// Reads a .env file located at <paramref name="envPath"/> and writes the tracked configuration file.
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

        var map = BuildEnvMap(File.ReadAllLines(envPath));
        Write(projectRoot, map);
    }

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
                        var ordered = new List<string>();
                        var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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
}
