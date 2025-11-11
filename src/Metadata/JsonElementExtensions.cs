
namespace Xtraq.Metadata;

/// <summary>
/// Shared JSON helpers for snapshot metadata to avoid duplicating JsonElement extension methods.
/// </summary>
internal static class JsonElementExtensions
{
    public static string? GetPropertyOrDefault(this JsonElement element, string name)
    {
        if (!TryGetPropertyCaseInsensitive(element, name, out var value))
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.Null ? null : value.GetString();
    }

    public static bool GetPropertyOrDefaultBool(this JsonElement element, string name)
    {
        if (!TryGetPropertyCaseInsensitive(element, name, out var value)) return false;
        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            JsonValueKind.Number => value.TryGetInt32(out var numeric) && numeric != 0,
            _ => false
        };
    }

    public static bool GetPropertyOrDefaultBoolStrict(this JsonElement element, string name)
    {
        if (!TryGetPropertyCaseInsensitive(element, name, out var value)) return false;
        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            JsonValueKind.Number => value.TryGetInt32(out var numeric) && numeric != 0,
            _ => false
        };
    }

    public static int? GetPropertyOrDefaultInt(this JsonElement element, string name)
    {
        if (!TryGetPropertyCaseInsensitive(element, name, out var value)) return null;
        if (value.ValueKind != JsonValueKind.Number) return null;
        return value.TryGetInt32(out var parsed) ? parsed : (int?)null;
    }

    public static bool? GetPropertyOrNullBool(this JsonElement element, string name)
    {
        if (!TryGetPropertyCaseInsensitive(element, name, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(value.GetString(), out var parsed) => parsed,
            JsonValueKind.Number when value.TryGetInt32(out var numeric) => numeric != 0,
            _ => null
        };
    }

    internal static bool TryGetPropertyCaseInsensitive(JsonElement element, string name, out JsonElement value)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

        if (element.TryGetProperty(name, out value))
        {
            return true;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
