namespace Xtraq.Configuration;

/// <summary>
/// Manages the tracked configuration file (debug/.xtraqconfig) derived from non-sensitive .env values.
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

        var debugDir = Path.Combine(projectRoot, "debug");
        Directory.CreateDirectory(debugDir);

        var buildSchemas = Array.Empty<string>();
        if (envValues.TryGetValue("XTRAQ_BUILD_SCHEMAS", out var schemasRaw) && !string.IsNullOrWhiteSpace(schemasRaw))
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

        envValues.TryGetValue("XTRAQ_NAMESPACE", out var nsValue);
        envValues.TryGetValue("XTRAQ_OUTPUT_DIR", out var outputDirValue);
        envValues.TryGetValue("XTRAQ_TFM", out var tfmValue);

        var payload = new
        {
            Namespace = nsValue?.Trim() ?? string.Empty,
            OutputDir = string.IsNullOrWhiteSpace(outputDirValue) ? "Xtraq" : outputDirValue!.Trim(),
            TargetFramework = tfmValue?.Trim() ?? string.Empty,
            BuildSchemas = buildSchemas
        };

        var configPath = Path.Combine(debugDir, ".xtraqconfig");
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
}
