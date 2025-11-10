namespace Xtraq.SnapshotBuilder.Analyzers;

internal static class AggregateFunctionCatalog
{
    private static readonly string[] AggregateFunctionNames =
    {
        "COUNT",
        "COUNT_BIG",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "EXISTS"
    };

    private static readonly HashSet<string> NameLookup = new(AggregateFunctionNames, StringComparer.OrdinalIgnoreCase);
    private static readonly string DetectionAlternation = string.Join("|", AggregateFunctionNames.Select(name => name.ToLowerInvariant()));
    private static readonly Regex DetectionRegex = new($"\\b({DetectionAlternation})\\s*\\(", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    internal static IReadOnlyCollection<string> Names => AggregateFunctionNames;

    internal static bool IsAggregateName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return NameLookup.Contains(name);
    }

    internal static string? NormalizeName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var trimmed = name.Trim();
        return trimmed.Length == 0 ? null : trimmed.ToLowerInvariant();
    }

    internal static string? DetectInExpression(string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
        {
            return null;
        }

        var match = DetectionRegex.Match(expression);
        if (!match.Success)
        {
            return null;
        }

        return match.Groups[1].Value.ToLowerInvariant();
    }

    internal static Regex DetectionPattern => DetectionRegex;
}
