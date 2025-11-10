
namespace Xtraq.Metadata;

/// <summary>
/// Resolves <c>schema.name</c> type references coming from expanded snapshot artifacts into
/// concrete SQL type descriptors (including base type, formatted type string, precision metadata).
/// The resolver caches scalar user-defined types sourced from <c>.xtraq/snapshots/types</c>.
/// </summary>
internal sealed class TypeMetadataResolver
{
    private readonly Dictionary<string, ScalarTypeInfo> _scalarTypes;

    public TypeMetadataResolver(string? projectRoot = null)
    {
        var root = string.IsNullOrWhiteSpace(projectRoot)
            ? Directory.GetCurrentDirectory()
            : Path.GetFullPath(projectRoot!);
        _scalarTypes = LoadScalarTypes(root);
    }

    public ResolvedType? Resolve(string? typeRef, int? maxLength, int? precision, int? scale)
    {
        var parts = SplitTypeRefParts(typeRef);
        if (string.IsNullOrWhiteSpace(parts.Schema) || string.IsNullOrWhiteSpace(parts.Name))
        {
            return null;
        }

        string baseSqlType;
        var effectiveMax = NormalizeLength(maxLength);
        var effectivePrecision = NormalizePrecision(precision);
        var effectiveScale = NormalizePrecision(scale);

        bool? isNullable = null;

        if (string.Equals(parts.Schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            baseSqlType = NormalizeBaseType(parts.Name);
        }
        else
        {
            ScalarTypeInfo? scalar = null;
            if (!string.IsNullOrWhiteSpace(parts.Catalog))
            {
                var catalogKey = BuildCatalogKey(parts.Catalog!, parts.Schema!, parts.Name!);
                _scalarTypes.TryGetValue(catalogKey, out scalar);
            }

            if (scalar is null)
            {
                var baseKey = BuildKey(parts.Schema!, parts.Name!);
                _scalarTypes.TryGetValue(baseKey, out scalar);
            }

            if (scalar is null)
            {
                return null;
            }

            baseSqlType = NormalizeBaseType(scalar.BaseSqlTypeName ?? scalar.Name ?? parts.Name);
            isNullable = scalar.IsNullable;
            effectiveMax ??= NormalizeLength(scalar.MaxLength);
            effectivePrecision ??= NormalizePrecision(scalar.Precision);
            effectiveScale ??= NormalizePrecision(scalar.Scale);
        }

        var sqlType = FormatSqlType(baseSqlType, effectiveMax, effectivePrecision, effectiveScale);
        return new ResolvedType(baseSqlType, sqlType, effectiveMax, effectivePrecision, effectiveScale, isNullable);
    }

    public static (string? Schema, string? Name) SplitTypeRef(string? typeRef)
    {
        var parts = SplitTypeRefParts(typeRef);
        return (parts.Schema, parts.Name);
    }

    private static TypeRefParts SplitTypeRefParts(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return default;
        }

        var tokens = typeRef.Trim().Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0)
        {
            return default;
        }

        if (tokens.Length == 1)
        {
            return new TypeRefParts(null, null, tokens[0]);
        }

        if (tokens.Length == 2)
        {
            return new TypeRefParts(null, tokens[0], tokens[1]);
        }

        var catalog = tokens[0];
        var schema = tokens[^2];
        var name = tokens[^1];
        return new TypeRefParts(catalog, schema, name);
    }

    private static Dictionary<string, ScalarTypeInfo> LoadScalarTypes(string projectRoot)
    {
        var map = new Dictionary<string, ScalarTypeInfo>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var typesDir = Path.Combine(projectRoot, ".xtraq", "snapshots", "types");
            if (!Directory.Exists(typesDir)) return map;
            var files = Directory.GetFiles(typesDir, "*.json", SearchOption.TopDirectoryOnly);
            foreach (var file in files)
            {
                try
                {
                    using var fs = File.OpenRead(file);
                    using var doc = JsonDocument.Parse(fs);
                    var root = doc.RootElement;
                    var catalog = root.GetPropertyOrDefault("Catalog");
                    var schema = root.GetPropertyOrDefault("Schema") ?? "dbo";
                    var name = root.GetPropertyOrDefault("Name") ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(name)) continue;
                    bool? isNullable = null;
                    if (root.TryGetProperty("IsNullable", out var nullableToken) && nullableToken.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    {
                        isNullable = nullableToken.GetBoolean();
                    }

                    var info = new ScalarTypeInfo(
                        Catalog: catalog,
                        Schema: schema,
                        Name: name,
                        BaseSqlTypeName: root.GetPropertyOrDefault("BaseSqlTypeName") ?? root.GetPropertyOrDefault("SqlTypeName"),
                        MaxLength: root.GetPropertyOrDefaultInt("MaxLength"),
                        Precision: root.GetPropertyOrDefaultInt("Precision"),
                        Scale: root.GetPropertyOrDefaultInt("Scale"),
                        IsNullable: isNullable
                    );
                    var baseKey = BuildKey(schema, name);
                    if (!map.ContainsKey(baseKey))
                    {
                        map[baseKey] = info;
                    }

                    if (!string.IsNullOrWhiteSpace(catalog))
                    {
                        var catalogKey = BuildCatalogKey(catalog!, schema, name);
                        map[catalogKey] = info;
                    }
                }
                catch
                {
                    // ignore individual file parse issues; remaining entries still help resolve types
                }
            }
        }
        catch
        {
            // ignore directory enumeration issues
        }
        return map;
    }

    private static string BuildKey(string schema, string name)
        => string.Concat(schema?.Trim() ?? string.Empty, ".", name?.Trim() ?? string.Empty);

    private static string BuildCatalogKey(string catalog, string schema, string name)
        => string.Concat(catalog?.Trim() ?? string.Empty, ".", BuildKey(schema, name));

    private static string NormalizeBaseType(string value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim();

    private static int? NormalizeLength(int? value)
    {
        if (!value.HasValue) return null;
        var val = value.Value;
        if (val < 0) return -1; // e.g. MAX types
        if (val == 0) return null;
        return val;
    }

    private static int? NormalizePrecision(int? value)
    {
        if (!value.HasValue) return null;
        var val = value.Value;
        return val <= 0 ? null : val;
    }

    private static string FormatSqlType(string baseType, int? maxLength, int? precision, int? scale)
    {
        if (string.IsNullOrWhiteSpace(baseType)) return string.Empty;
        var normalized = baseType.Trim().ToLowerInvariant();
        switch (normalized)
        {
            case "decimal":
            case "numeric":
                if (precision.HasValue)
                {
                    var effectiveScale = scale ?? 0;
                    return $"{normalized}({precision.Value},{effectiveScale})";
                }
                return normalized;
            case "varchar":
            case "nvarchar":
            case "varbinary":
            case "char":
            case "nchar":
            case "binary":
                if (maxLength.HasValue)
                {
                    if (maxLength.Value < 0) return $"{normalized}(max)";
                    return $"{normalized}({maxLength.Value})";
                }
                return $"{normalized}(max)";
            case "datetime2":
            case "datetimeoffset":
            case "time":
                if (scale.HasValue)
                {
                    return $"{normalized}({scale.Value})";
                }
                return normalized;
            default:
                return normalized;
        }
    }
}

internal sealed record ScalarTypeInfo(
    string? Catalog,
    string Schema,
    string Name,
    string? BaseSqlTypeName,
    int? MaxLength,
    int? Precision,
    int? Scale,
    bool? IsNullable
);

internal readonly record struct ResolvedType(
    string BaseSqlType,
    string SqlType,
    int? MaxLength,
    int? Precision,
    int? Scale,
    bool? IsNullable
);

internal readonly record struct TypeRefParts(string? Catalog, string? Schema, string? Name);
