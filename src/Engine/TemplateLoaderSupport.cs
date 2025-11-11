namespace Xtraq.Engine;

/// <summary>
/// Shared helpers for template loader implementations.
/// </summary>
internal static class TemplateLoaderSupport
{
    /// <summary>
    /// Adds template content to the provided catalog, normalising variant identifiers.
    /// </summary>
    /// <param name="catalog">Target catalog keyed by logical template name.</param>
    /// <param name="logicalName">Logical name optionally suffixed with a variant (e.g. TableType.net10).</param>
    /// <param name="content">Template body.</param>
    public static void AddTemplate(Dictionary<string, Dictionary<string, string>> catalog, string logicalName, string content)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(logicalName);
        ArgumentNullException.ThrowIfNull(content);

        var variantKey = "base";
        var idx = logicalName.LastIndexOf('.');
        if (idx > 0 && idx < logicalName.Length - 1)
        {
            var tail = logicalName[(idx + 1)..];
            if (tail.StartsWith("net", StringComparison.OrdinalIgnoreCase) && tail.Length > 3 && tail.Skip(3).All(char.IsDigit))
            {
                variantKey = tail.ToLowerInvariant();
                logicalName = logicalName[..idx];
            }
        }

        if (!catalog.TryGetValue(logicalName, out var variants))
        {
            variants = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            catalog[logicalName] = variants;
        }

        variants[variantKey] = content;
    }

    /// <summary>
    /// Attempts to resolve a template variant based on the current target framework preference.
    /// </summary>
    /// <param name="variants">Available variants for the logical template.</param>
    /// <param name="currentTfmMajor">Current target framework moniker (e.g. net8).</param>
    /// <param name="content">Resolved template content.</param>
    /// <returns>True when a matching template is found; otherwise false.</returns>
    public static bool TryResolveTemplate(IReadOnlyDictionary<string, string> variants, string currentTfmMajor, out string content)
    {
        ArgumentNullException.ThrowIfNull(variants);
        ArgumentNullException.ThrowIfNull(currentTfmMajor);

        if (variants.TryGetValue(currentTfmMajor, out content!))
        {
            return true;
        }

        if (variants.TryGetValue("base", out content!))
        {
            return true;
        }

        var candidate = variants.Keys
            .Where(static k => k.StartsWith("net", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(static k => k.Length)
            .ThenByDescending(static k => k, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();

        if (candidate is not null && variants.TryGetValue(candidate, out content!))
        {
            return true;
        }

        content = null!;
        return false;
    }

    /// <summary>
    /// Resolves the TFM major identifier used to prioritise template variants.
    /// </summary>
    /// <returns>Normalised moniker (e.g. net8, net10).</returns>
    public static string ResolveCurrentTfmMajor()
    {
        var tfm = Environment.GetEnvironmentVariable("XTRAQ_TFM");
        if (!string.IsNullOrWhiteSpace(tfm))
        {
            var major = ExtractMajor(tfm);
            if (major is not null)
            {
                return major;
            }
        }

        // Default to the latest templates; callers may override via XTRAQ_TFM when required.
        return "net10";
    }

    private static string? ExtractMajor(string tfm)
    {
        tfm = tfm.Trim().ToLowerInvariant();
        if (tfm.StartsWith("net", StringComparison.Ordinal))
        {
            var digits = new string(tfm.Skip(3).TakeWhile(char.IsDigit).ToArray());
            if (digits.Length > 0)
            {
                return "net" + digits;
            }
        }

        return null;
    }
}
