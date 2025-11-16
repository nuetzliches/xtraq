namespace Xtraq.Services;

internal static class TableTypeRefFormatter
{
    public static string? Combine(string? schema, string? name)
        => Combine(null, schema, name);

    public static string? Combine(string? catalog, string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var segments = new List<string>(3);
        if (!string.IsNullOrWhiteSpace(catalog))
        {
            segments.Add(catalog.Trim());
        }
        if (!string.IsNullOrWhiteSpace(schema))
        {
            segments.Add(schema.Trim());
        }
        segments.Add(name.Trim());

        return string.Join('.', segments);
    }

    public static string? Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var segments = value
            .Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        if (segments.Length == 0)
        {
            return null;
        }

        if (segments.Length > 3)
        {
            segments = segments[^3..];
        }

        return string.Join('.', segments);
    }

    public static (string? Catalog, string? Schema, string? Name) Split(string? value)
    {
        var normalized = Normalize(value);
        if (normalized == null)
        {
            return (null, null, null);
        }

        var segments = normalized.Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
        {
            return (null, null, null);
        }

        if (segments.Length == 1)
        {
            return (null, null, segments[0]);
        }

        if (segments.Length == 2)
        {
            return (null, segments[0], segments[1]);
        }

        return (segments[0], segments[1], segments[2]);
    }
}
