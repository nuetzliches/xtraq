namespace Xtraq.Utils;

/// <summary>
/// Helper utilities for working with three-part type references (catalog.schema.name).
/// </summary>
internal static class TypeRefUtilities
{
    public static string? Combine(string? catalog, string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var trimmedName = name.Trim();
        if (trimmedName.Length == 0)
        {
            return null;
        }

        var parts = new List<string>(3);
        if (!string.IsNullOrWhiteSpace(catalog))
        {
            parts.Add(catalog.Trim());
        }

        if (!string.IsNullOrWhiteSpace(schema))
        {
            parts.Add(schema.Trim());
        }

        parts.Add(trimmedName);
        return string.Join('.', parts);
    }

    public static (string? Catalog, string? Schema, string? Name) Split(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return (null, null, null);
        }

        var parts = typeRef.Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return (null, null, null);
        }

        return parts.Length switch
        {
            1 => (null, null, parts[0]),
            2 => (null, parts[0], parts[1]),
            _ => (parts[0], parts[1], parts[^1])
        };
    }
}
