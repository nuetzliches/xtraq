namespace Xtraq.Metadata;

/// <summary>
/// Reads table metadata from the expanded snapshot (.xtraq/snapshots/tables).
/// Lightweight read-only layer (no heuristics and no fallback to legacy monolithic files).
/// </summary>
internal interface ITableMetadataProvider
{
    IReadOnlyList<TableInfo> GetAll();
    TableInfo? TryGet(string schema, string name);
}

internal sealed class TableInfo
{
    public string? Catalog { get; init; }
    public string Schema { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public IReadOnlyList<ColumnInfo> Columns { get; init; } = Array.Empty<ColumnInfo>();
}

// Reuse ColumnInfo from TableTypeMetadataProvider when available; if a namespace conflict appears, define a compatible type.
// The existing ColumnInfo lives in Xtraq.Metadata (same namespace), so we can reference it directly.
// If a build error occurs (type not found), declare a minimal type instead.
// Extra guard: if already defined, this partial definition is ignored.
#if false
internal sealed class ColumnInfo
{
    public string Name { get; init; } = string.Empty;
    public string SqlType { get; init; } = string.Empty;
    public bool IsNullable { get; init; }
    public int? MaxLength { get; init; }
}
#endif
internal sealed class TableMetadataProvider : ITableMetadataProvider
{
    private readonly string _projectRoot;
    private readonly ITableMetadataCache _cache;

    public TableMetadataProvider(string? projectRoot = null, TimeSpan? ttl = null)
    {
        _projectRoot = string.IsNullOrWhiteSpace(projectRoot)
            ? Directory.GetCurrentDirectory()
            : Path.GetFullPath(projectRoot!);
        _cache = TableMetadataCacheRegistry.GetOrCreate(_projectRoot, ttl);
    }

    public IReadOnlyList<TableInfo> GetAll()
    {
        return _cache.GetAll();
    }

    public TableInfo? TryGet(string schema, string name)
    {
        return _cache.TryGet(schema, name);
    }

    public void Invalidate()
    {
        _cache.Invalidate();
    }
}

