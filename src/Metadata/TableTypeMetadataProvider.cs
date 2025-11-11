
namespace Xtraq.Metadata;

/// <summary>
/// Minimal reader for user defined table type metadata sourced from the latest snapshot under .xtraq/snapshots.
/// Avoids legacy runtime model dependencies; produces a lightweight immutable model collection for code generation.
/// </summary>
public interface ITableTypeMetadataProvider
{
    /// <summary>
    /// Retrieves all user-defined table type descriptors discovered in the snapshot metadata.
    /// </summary>
    /// <returns>A read-only list of table type descriptors.</returns>
    IReadOnlyList<TableTypeInfo> GetAll();
}

/// <summary>
/// Represents a user-defined table type descriptor.
/// </summary>
public sealed class TableTypeInfo
{
    /// <summary>
    /// Gets or sets the schema that owns the table type.
    /// </summary>
    public string Schema { get; init; } = string.Empty;
    /// <summary>
    /// Gets or sets the name of the table type.
    /// </summary>
    public string Name { get; init; } = string.Empty;
    /// <summary>
    /// Gets or sets the columns that compose the table type.
    /// </summary>
    public IReadOnlyList<ColumnInfo> Columns { get; init; } = Array.Empty<ColumnInfo>();
}

/// <summary>
/// Represents a column within a user-defined table type.
/// </summary>
public sealed class ColumnInfo
{
    /// <summary>
    /// Gets or sets the column name.
    /// </summary>
    public string Name { get; init; } = string.Empty;
    /// <summary>
    /// Gets or sets the raw type reference extracted from metadata.</summary>
    public string TypeRef { get; init; } = string.Empty;
    /// <summary>
    /// Gets or sets the resolved SQL type name.</summary>
    public string SqlType { get; init; } = string.Empty;
    /// <summary>
    /// Gets or sets a value indicating whether the column allows null values.</summary>
    public bool IsNullable { get; init; }
    /// <summary>
    /// Gets or sets the optional maximum length for variable-sized columns.</summary>
    public int? MaxLength { get; init; }
    /// <summary>
    /// Gets or sets the optional precision for numeric columns.</summary>
    public int? Precision { get; init; }
    /// <summary>
    /// Gets or sets the optional scale for numeric columns.</summary>
    public int? Scale { get; init; }
    /// <summary>
    /// Gets or sets a value indicating whether the column is an identity column.</summary>
    public bool IsIdentity { get; init; }
}

/// <summary>
/// Loads user-defined table type metadata from stored snapshots.
/// </summary>
internal sealed class TableTypeMetadataProvider : ITableTypeMetadataProvider
{
    private IReadOnlyList<TableTypeInfo>? _cache;
    private readonly string _projectRoot;
    private readonly TypeMetadataResolver _typeResolver;

    /// <summary>
    /// Initializes a new instance of the <see cref="TableTypeMetadataProvider"/> class.
    /// </summary>
    /// <param name="projectRoot">Optional project root used to resolve the snapshot directory.</param>
    public TableTypeMetadataProvider(string? projectRoot = null)
    {
        _projectRoot = string.IsNullOrWhiteSpace(projectRoot) ? Directory.GetCurrentDirectory() : Path.GetFullPath(projectRoot!);
        _typeResolver = new TypeMetadataResolver(_projectRoot);
    }

    /// <inheritdoc />
    public IReadOnlyList<TableTypeInfo> GetAll()
    {
        if (_cache != null) return _cache;
        var schemaDir = Path.Combine(_projectRoot, ".xtraq", "snapshots");
        if (!Directory.Exists(schemaDir)) return _cache = Array.Empty<TableTypeInfo>();

        // Strategy:
        // 1. Prefer expanded snapshot index.json if it contains UserDefinedTableTypes.
        // 2. Fallback to latest monolith json containing UserDefinedTableTypes.
        // 3. If none found -> empty.
        JsonElement udtts = default;
        JsonDocument? udttsDocument = null;
        bool found = false;
        var indexPath = Path.Combine(schemaDir, "index.json");
        if (File.Exists(indexPath))
        {
            try
            {
                using var ifs = File.OpenRead(indexPath);
                using var idoc = JsonDocument.Parse(ifs);
                if (idoc.RootElement.TryGetProperty("UserDefinedTableTypes", out var idxUdtts) && idxUdtts.ValueKind == JsonValueKind.Array)
                {
                    udttsDocument = JsonDocument.Parse(idxUdtts.GetRawText());
                    udtts = udttsDocument.RootElement;
                    found = true;
                }
            }
            catch { /* ignore and fallback */ }
        }
        if (!found)
        {
            var files = Directory.GetFiles(schemaDir, "*.json")
                .Where(f => !string.Equals(Path.GetFileName(f), "index.json", StringComparison.OrdinalIgnoreCase))
                .ToArray();
            foreach (var fi in files.Select(f => new FileInfo(f)).OrderByDescending(fi => fi.LastWriteTimeUtc))
            {
                try
                {
                    using var fs = File.OpenRead(fi.FullName);
                    using var doc = JsonDocument.Parse(fs);
                    if (doc.RootElement.TryGetProperty("UserDefinedTableTypes", out var monolithUdtts) && monolithUdtts.ValueKind == JsonValueKind.Array)
                    {
                        udttsDocument = JsonDocument.Parse(monolithUdtts.GetRawText());
                        udtts = udttsDocument.RootElement;
                        found = true;
                        break;
                    }
                }
                catch { /* continue search */ }
            }
        }
        if (!found)
        {
            // Extended layout: a dedicated tabletypes directory may contain split JSON files.
            var tableTypesDir = Path.Combine(schemaDir, "tabletypes");
            if (Directory.Exists(tableTypesDir))
            {
                var ttFiles = Directory.GetFiles(tableTypesDir, "*.json");
                var listT = new List<TableTypeInfo>();
                foreach (var tf in ttFiles)
                {
                    try
                    {
                        using var tfs = File.OpenRead(tf);
                        using var tdoc = JsonDocument.Parse(tfs);
                        var root = tdoc.RootElement;
                        var schema = root.GetPropertyOrDefault("Schema") ?? "dbo";
                        var name = root.GetPropertyOrDefault("Name") ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(name)) continue;
                        var cols = new List<ColumnInfo>();
                        if (root.TryGetProperty("Columns", out var colsEl) && colsEl.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var c in colsEl.EnumerateArray())
                            {
                                var typeRef = c.GetPropertyOrDefault("TypeRef");
                                var maxLen = c.GetPropertyOrDefaultInt("MaxLength");
                                var prec = c.GetPropertyOrDefaultInt("Precision");
                                var scale = c.GetPropertyOrDefaultInt("Scale");
                                var resolved = _typeResolver.Resolve(typeRef, maxLen, prec, scale);
                                var nullableFlag = c.GetPropertyOrNullBool("IsNullable");
                                cols.Add(new ColumnInfo
                                {
                                    Name = c.GetPropertyOrDefault("Name") ?? string.Empty,
                                    TypeRef = typeRef ?? string.Empty,
                                    SqlType = resolved?.SqlType ?? c.GetPropertyOrDefault("SqlTypeName") ?? string.Empty,
                                    IsNullable = nullableFlag ?? resolved?.IsNullable ?? false,
                                    MaxLength = resolved?.MaxLength ?? maxLen,
                                    Precision = resolved?.Precision ?? prec,
                                    Scale = resolved?.Scale ?? scale,
                                    IsIdentity = c.GetPropertyOrDefaultBool("IsIdentity")
                                });
                            }
                        }
                        listT.Add(new TableTypeInfo { Schema = schema, Name = name, Columns = cols });
                    }
                    catch { /* skip file */ }
                }
                if (listT.Count > 0)
                {
                    _cache = listT.OrderBy(t => t.Schema).ThenBy(t => t.Name).ToList();
                    return _cache; // success via expanded folder
                }
            }
            // Fallback: reconstruct from procedure inputs
            var procDir = Path.Combine(schemaDir, "procedures");
            if (!Directory.Exists(procDir)) return _cache = Array.Empty<TableTypeInfo>();
            var procFiles = Directory.GetFiles(procDir, "*.json");
            var inferred = new Dictionary<string, TableTypeInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var pf in procFiles)
            {
                try
                {
                    using var pfs = File.OpenRead(pf);
                    using var pdoc = JsonDocument.Parse(pfs);
                    var root = pdoc.RootElement;
                    var procSchema = root.GetPropertyOrDefault("Schema") ?? "dbo";
                    if ((root.TryGetProperty("Parameters", out var inputsEl) && inputsEl.ValueKind == JsonValueKind.Array) ||
                        (root.TryGetProperty("Inputs", out inputsEl) && inputsEl.ValueKind == JsonValueKind.Array))
                    {
                        foreach (var ip in inputsEl.EnumerateArray())
                        {
                            var typeRef = ip.GetPropertyOrDefault("TypeRef");
                            bool isTt = ip.GetPropertyOrDefaultBool("IsTableType");
                            var ttSchema = ip.GetPropertyOrDefault("TableTypeSchema");
                            var ttName = ip.GetPropertyOrDefault("TableTypeName") ?? ip.GetPropertyOrDefault("Name")?.TrimStart('@') ?? string.Empty;

                            if (!isTt && !string.IsNullOrWhiteSpace(ttName))
                            {
                                isTt = true;
                            }

                            if (!isTt && !string.IsNullOrWhiteSpace(typeRef))
                            {
                                var (schemaFromRef, nameFromRef) = SplitTypeRef(typeRef);
                                if (!string.IsNullOrWhiteSpace(schemaFromRef) && !string.Equals(schemaFromRef, "sys", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(nameFromRef))
                                {
                                    isTt = true;
                                    ttSchema ??= schemaFromRef;
                                    if (string.IsNullOrWhiteSpace(ttName))
                                    {
                                        ttName = nameFromRef;
                                    }
                                }
                            }

                            if (!isTt) continue;
                            ttSchema ??= procSchema;
                            if (string.IsNullOrWhiteSpace(ttName)) continue;
                            var key = ttSchema + "." + ttName;
                            if (!inferred.ContainsKey(key))
                            {
                                inferred[key] = new TableTypeInfo { Schema = ttSchema, Name = ttName, Columns = Array.Empty<ColumnInfo>() };
                            }
                        }
                    }
                }
                catch { }
            }
            if (inferred.Count == 0)
            {
                udttsDocument?.Dispose();
                return _cache = Array.Empty<TableTypeInfo>();
            }
            var result = inferred.Values.OrderBy(t => t.Schema).ThenBy(t => t.Name).ToList();
            udttsDocument?.Dispose();
            return _cache = result;
        }
        var list = new List<TableTypeInfo>();
        foreach (var tt in udtts.EnumerateArray())
        {
            var schema = tt.GetPropertyOrDefault("Schema") ?? "dbo";
            var name = tt.GetPropertyOrDefault("Name") ?? "";
            var cols = new List<ColumnInfo>();
            if (tt.TryGetProperty("Columns", out var colsEl) && colsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var c in colsEl.EnumerateArray())
                {
                    var typeRef = c.GetPropertyOrDefault("TypeRef");
                    var maxLen = c.GetPropertyOrDefaultInt("MaxLength");
                    var prec = c.GetPropertyOrDefaultInt("Precision");
                    var scale = c.GetPropertyOrDefaultInt("Scale");
                    var resolved = _typeResolver.Resolve(typeRef, maxLen, prec, scale);
                    var nullableFlag = c.GetPropertyOrNullBool("IsNullable");
                    cols.Add(new ColumnInfo
                    {
                        Name = c.GetPropertyOrDefault("Name") ?? string.Empty,
                        TypeRef = typeRef ?? string.Empty,
                        SqlType = resolved?.SqlType ?? c.GetPropertyOrDefault("SqlTypeName") ?? string.Empty,
                        IsNullable = nullableFlag ?? resolved?.IsNullable ?? false,
                        MaxLength = resolved?.MaxLength ?? maxLen,
                        Precision = resolved?.Precision ?? prec,
                        Scale = resolved?.Scale ?? scale,
                        IsIdentity = c.GetPropertyOrDefaultBool("IsIdentity")
                    });
                }
            }
            if (!string.IsNullOrWhiteSpace(name))
            {
                list.Add(new TableTypeInfo
                {
                    Schema = schema,
                    Name = name,
                    Columns = cols
                });
            }
        }
        var ordered = list.OrderBy(t => t.Schema).ThenBy(t => t.Name).ToList();
        udttsDocument?.Dispose();
        _cache = ordered;
        return _cache;
    }

    private static (string? Schema, string? Name) SplitTypeRef(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef)) return (null, null);
        var parts = typeRef.Trim().Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0) return (null, null);

        var name = string.IsNullOrWhiteSpace(parts[^1]) ? null : parts[^1];
        var schema = parts.Length >= 2 ? (string.IsNullOrWhiteSpace(parts[^2]) ? null : parts[^2]) : null;
        return (schema, name);
    }
}

