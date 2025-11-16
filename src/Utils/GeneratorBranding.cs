namespace Xtraq.Utils;

/// <summary>
/// Provides a centralized generator version and label consistent across templates and cache state.
/// </summary>
public static class GeneratorBranding
{
    private static readonly Lazy<string> VersionValue = new(GetVersion, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Normalized generator version derived from assembly metadata.</summary>
    public static string GeneratorVersion => VersionValue.Value;

    /// <summary>
    /// Human-readable generator label displayed in generated artifacts.
    /// The label omits the version to avoid churn across generated files while keeping
    /// <see cref="GeneratorVersion"/> available for cache invalidation and diagnostics.
    /// </summary>
    public static string Label => "Xtraq Code Generator";

    /// <summary>Generator label with version, preserved for logging and telemetry scenarios.</summary>
    public static string LabelWithVersion => $"{Label} v{GeneratorVersion}";

    private static string GetVersion()
    {
        try
        {
            var assembly = typeof(GeneratorBranding).Assembly;
            var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            var normalizedInformational = Normalize(informational);
            if (!string.IsNullOrWhiteSpace(normalizedInformational))
            {
                return normalizedInformational!;
            }

            var nameVersion = assembly.GetName().Version;
            if (nameVersion != null)
            {
                var normalized = Normalize(nameVersion);
                if (!string.IsNullOrWhiteSpace(normalized))
                {
                    return normalized;
                }
            }
        }
        catch
        {
            // Swallow and fallback to default label; version retrieval should not block generation.
        }

        return "1.0.0";
    }

    private static string? Normalize(string? version)
    {
        if (string.IsNullOrWhiteSpace(version))
        {
            return null;
        }

        var cleaned = version.Trim();
        var plusIdx = cleaned.IndexOf('+');
        if (plusIdx >= 0)
        {
            cleaned = cleaned[..plusIdx];
        }

        if (System.Version.TryParse(cleaned, out var parsed))
        {
            return Normalize(parsed);
        }

        return cleaned;
    }

    private static string Normalize(System.Version version)
    {
        if (version.Revision > 0 && version.Build >= 0)
        {
            return $"{version.Major}.{version.Minor}.{Math.Max(0, version.Build)}.{version.Revision}";
        }

        if (version.Build > 0)
        {
            return $"{version.Major}.{version.Minor}.{version.Build}";
        }

        if (version.Build == 0)
        {
            return $"{version.Major}.{version.Minor}.0";
        }

        return $"{version.Major}.{version.Minor}";
    }
}
