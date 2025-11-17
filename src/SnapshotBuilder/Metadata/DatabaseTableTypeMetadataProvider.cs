using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Services;

namespace Xtraq.SnapshotBuilder.Metadata;

internal sealed class DatabaseTableTypeMetadataProvider : ITableTypeMetadataProvider
{
    private readonly DbContext _dbContext;
    private readonly IConsoleService _console;
    private readonly SchemaSnapshotFileLayoutService _layoutService;
    private readonly Dictionary<string, IReadOnlyList<TableTypeMetadata>> _schemaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _cacheLock = new();
    private IReadOnlyDictionary<string, IReadOnlyList<TableTypeMetadata>>? _snapshotCache;
    private bool _snapshotCacheInitialized;

    public DatabaseTableTypeMetadataProvider(DbContext dbContext, IConsoleService console, SchemaSnapshotFileLayoutService layoutService)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _layoutService = layoutService ?? throw new ArgumentNullException(nameof(layoutService));
    }

    public async Task<IReadOnlyList<TableTypeMetadata>> GetTableTypesAsync(ISet<string> schemas, bool skipCache, CancellationToken cancellationToken)
    {
        if (schemas == null || schemas.Count == 0)
        {
            return Array.Empty<TableTypeMetadata>();
        }

        var normalizedSchemas = new HashSet<string>(schemas.Where(static s => !string.IsNullOrWhiteSpace(s)), StringComparer.OrdinalIgnoreCase);
        if (normalizedSchemas.Count == 0)
        {
            return Array.Empty<TableTypeMetadata>();
        }

        var aggregated = new Dictionary<string, TableTypeMetadata>(StringComparer.OrdinalIgnoreCase);
        var pendingSchemas = new HashSet<string>(normalizedSchemas, StringComparer.OrdinalIgnoreCase);

        if (!skipCache)
        {
            lock (_cacheLock)
            {
                foreach (var schema in normalizedSchemas)
                {
                    if (_schemaCache.TryGetValue(schema, out var cached))
                    {
                        foreach (var metadata in cached)
                        {
                            var key = BuildMetadataKey(metadata);
                            if (key != null)
                            {
                                aggregated[key] = metadata;
                            }
                        }

                        pendingSchemas.Remove(schema);
                    }
                }
            }
        }

        if (pendingSchemas.Count > 0)
        {
            var resolved = await ResolveSchemasAsync(pendingSchemas, skipCache, cancellationToken).ConfigureAwait(false);
            foreach (var (key, metadata) in resolved)
            {
                aggregated[key] = metadata;
            }
        }

        return aggregated.Values
            .OrderBy(static meta => meta.TableType.SchemaName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static meta => meta.TableType.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private async Task<Dictionary<string, TableTypeMetadata>> ResolveSchemasAsync(ISet<string> schemas, bool skipCache, CancellationToken cancellationToken)
    {
        var resolved = new Dictionary<string, TableTypeMetadata>(StringComparer.OrdinalIgnoreCase);
        if (schemas == null || schemas.Count == 0)
        {
            return resolved;
        }

        var snapshotEntries = Array.Empty<TableTypeMetadata>();
        if (!skipCache)
        {
            snapshotEntries = LoadFromSnapshot(schemas);
        }

        foreach (var entry in snapshotEntries)
        {
            var key = BuildMetadataKey(entry);
            if (key != null)
            {
                resolved[key] = entry;
            }
        }

        IReadOnlyList<TableTypeMetadata> databaseEntries = Array.Empty<TableTypeMetadata>();
        {
            databaseEntries = await LoadFromDatabaseAsync(schemas, cancellationToken).ConfigureAwait(false);
        }

        foreach (var entry in databaseEntries)
        {
            var key = BuildMetadataKey(entry);
            if (key != null)
            {
                resolved[key] = entry;
            }
        }

        lock (_cacheLock)
        {
            var grouped = resolved.Values
                .GroupBy(static meta => meta.TableType.SchemaName, StringComparer.OrdinalIgnoreCase);
            foreach (var group in grouped)
            {
                if (string.IsNullOrWhiteSpace(group.Key))
                {
                    continue;
                }

                _schemaCache[group.Key] = group
                    .OrderBy(static meta => meta.TableType.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
        }

        return resolved;
    }

    private TableTypeMetadata[] LoadFromSnapshot(ISet<string> schemas)
    {
        var cache = LoadSnapshotCache();
        if (cache == null || cache.Count == 0)
        {
            return Array.Empty<TableTypeMetadata>();
        }

        var collector = new List<TableTypeMetadata>();
        foreach (var schema in schemas)
        {
            if (string.IsNullOrWhiteSpace(schema))
            {
                continue;
            }

            if (cache.TryGetValue(schema, out var entries) && entries != null && entries.Count > 0)
            {
                collector.AddRange(entries);
            }
        }

        return collector.ToArray();
    }

    private IReadOnlyDictionary<string, IReadOnlyList<TableTypeMetadata>>? LoadSnapshotCache()
    {
        lock (_cacheLock)
        {
            if (_snapshotCacheInitialized)
            {
                return _snapshotCache;
            }

            _snapshotCacheInitialized = true;

            try
            {
                var snapshot = _layoutService.LoadExpanded();
                if (snapshot?.UserDefinedTableTypes == null || snapshot.UserDefinedTableTypes.Count == 0)
                {
                    _snapshotCache = null;
                    return _snapshotCache;
                }

                var bySchema = new Dictionary<string, Dictionary<string, TableTypeMetadata>>(StringComparer.OrdinalIgnoreCase);
                foreach (var udtt in snapshot.UserDefinedTableTypes)
                {
                    var converted = ConvertSnapshotUdtt(udtt);
                    if (converted == null)
                    {
                        continue;
                    }

                    var schema = converted.TableType.SchemaName;
                    if (string.IsNullOrWhiteSpace(schema))
                    {
                        continue;
                    }

                    if (!bySchema.TryGetValue(schema, out var schemaMap))
                    {
                        schemaMap = new Dictionary<string, TableTypeMetadata>(StringComparer.OrdinalIgnoreCase);
                        bySchema[schema] = schemaMap;
                    }

                    var key = BuildMetadataKey(converted);
                    if (key != null)
                    {
                        schemaMap[key] = converted;
                    }
                }

                _snapshotCache = bySchema.ToDictionary(
                    pair => pair.Key,
                    pair => (IReadOnlyList<TableTypeMetadata>)pair.Value.Values
                        .OrderBy(static meta => meta.TableType.Name, StringComparer.OrdinalIgnoreCase)
                        .ToList(),
                    StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-tabletype] failed to load snapshot cache: {ex.Message}");
                _snapshotCache = null;
            }

            return _snapshotCache;
        }
    }

    private async Task<IReadOnlyList<TableTypeMetadata>> LoadFromDatabaseAsync(ISet<string> schemas, CancellationToken cancellationToken)
    {
        var escapedSchemas = schemas
            .Where(static s => !string.IsNullOrWhiteSpace(s))
            .Select(static s => $"'{s.Replace("'", "''")}'")
            .ToArray();

        if (escapedSchemas.Length == 0)
        {
            return Array.Empty<TableTypeMetadata>();
        }

        var schemaListString = string.Join(',', escapedSchemas);

        List<TableType> tableTypes;
        try
        {
            var list = await _dbContext.TableTypeListAsync(schemaListString, cancellationToken).ConfigureAwait(false);
            tableTypes = list ?? new List<TableType>();
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-tabletype] failed to enumerate table types: {ex.Message}");
            return Array.Empty<TableTypeMetadata>();
        }

        if (tableTypes.Count == 0)
        {
            return Array.Empty<TableTypeMetadata>();
        }

        var results = new List<TableTypeMetadata>(tableTypes.Count);
        foreach (var tableType in tableTypes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (tableType == null || string.IsNullOrWhiteSpace(tableType.SchemaName) || string.IsNullOrWhiteSpace(tableType.Name))
            {
                continue;
            }

            List<Column> columns = new();
            if (tableType.UserTypeId.HasValue)
            {
                try
                {
                    var list = await _dbContext.TableTypeColumnListAsync(tableType.UserTypeId.Value, cancellationToken).ConfigureAwait(false);
                    if (list != null)
                    {
                        columns = list;
                    }
                }
                catch (Exception ex)
                {
                    _console.Verbose($"[snapshot-tabletype] failed to load columns for {tableType.SchemaName}.{tableType.Name}: {ex.Message}");
                }
            }

            results.Add(new TableTypeMetadata(tableType, columns));
        }

        return results;
    }

    private static TableTypeMetadata? ConvertSnapshotUdtt(SnapshotUdtt? udtt)
    {
        if (udtt == null || string.IsNullOrWhiteSpace(udtt.Schema) || string.IsNullOrWhiteSpace(udtt.Name))
        {
            return null;
        }

        var tableType = new TableType
        {
            SchemaName = udtt.Schema,
            Name = udtt.Name,
            UserTypeId = udtt.UserTypeId
        };

        var columns = new List<Column>();
        if (udtt.Columns != null)
        {
            foreach (var column in udtt.Columns)
            {
                var converted = ConvertSnapshotColumn(column);
                if (converted != null)
                {
                    columns.Add(converted);
                }
            }
        }

        tableType.Columns = columns;
        return new TableTypeMetadata(tableType, columns);
    }

    private static Column? ConvertSnapshotColumn(SnapshotUdttColumn? source)
    {
        if (source == null || string.IsNullOrWhiteSpace(source.Name))
        {
            return null;
        }

        var column = new Column
        {
            Name = source.Name,
            IsNullable = source.IsNullable ?? false,
            MaxLength = source.MaxLength ?? 0,
            Precision = source.Precision,
            Scale = source.Scale,
            SqlTypeName = string.Empty
        };

        var normalizedTypeRef = TableTypeRefFormatter.Normalize(source.TypeRef);
        if (!string.IsNullOrWhiteSpace(normalizedTypeRef))
        {
            var (catalog, schema, name) = TableTypeRefFormatter.Split(normalizedTypeRef);
            if (!string.IsNullOrWhiteSpace(schema) && !string.IsNullOrWhiteSpace(name))
            {
                if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
                {
                    column.SqlTypeName = name;
                }
                else
                {
                    column.CatalogName = catalog;
                    column.UserTypeSchemaName = schema;
                    column.UserTypeName = name;
                }
            }
            else if (!string.IsNullOrWhiteSpace(name))
            {
                column.SqlTypeName = name;
            }
        }

        if (string.IsNullOrWhiteSpace(column.SqlTypeName) && !string.IsNullOrWhiteSpace(source.TypeRef))
        {
            var parts = source.TypeRef.Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length > 0)
            {
                column.SqlTypeName = parts[^1];
            }
        }

        return column;
    }

    private static string? BuildMetadataKey(TableTypeMetadata metadata)
    {
        if (metadata?.TableType == null)
        {
            return null;
        }

        var schema = metadata.TableType.SchemaName;
        var name = metadata.TableType.Name;
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return string.Concat(schema.Trim(), ".", name.Trim());
    }
}
