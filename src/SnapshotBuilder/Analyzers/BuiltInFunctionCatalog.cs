namespace Xtraq.SnapshotBuilder.Analyzers;

internal static class BuiltInFunctionCatalog
{
    private static readonly string[] AdditionalFunctionNames =
    {
        "JSON_QUERY",
        "JSON_VALUE",
        "JSON_MODIFY",
        "IIF",
        "ISNULL",
        "COALESCE",
        "NULLIF",
        "LAG",
        "LEAD",
        "FIRST_VALUE",
        "LAST_VALUE",
        "ROW_NUMBER",
        "CONCAT",
        "STRING_AGG",
        "DB_NAME"
    };

    private static readonly string[] MetadataPropagationFunctionNames =
    {
        "CONCAT",
        "STRING_AGG"
    };

    private static readonly string[] CombinedFunctionNames = AggregateFunctionCatalog.Names
        .Concat(AdditionalFunctionNames)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();

    private static readonly HashSet<string> CombinedFunctionLookup = new(CombinedFunctionNames, StringComparer.OrdinalIgnoreCase);
    private static readonly HashSet<string> MetadataPropagationLookup = new(MetadataPropagationFunctionNames, StringComparer.OrdinalIgnoreCase);

    internal static IReadOnlyCollection<string> Names => CombinedFunctionNames;

    internal static bool Contains(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return CombinedFunctionLookup.Contains(name);
    }

    internal static bool IsMetadataPropagationOnly(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return MetadataPropagationLookup.Contains(name);
    }
}
