using Xtraq.Data;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Metadata;

internal sealed class DatabaseDependencyMetadataProvider : IDependencyMetadataProvider
{
    private readonly DbContext _dbContext;
    private readonly IConsoleService _console;
    private readonly ConcurrentDictionary<DependencyKey, DateTime?> _modifyDateCache = new();
    private readonly ConcurrentDictionary<ProcedureDependencyKind, IReadOnlyDictionary<DependencyKey, DateTime?>> _catalogSnapshots = new();
    private static readonly IReadOnlyDictionary<DependencyKey, DateTime?> EmptyCatalog = new Dictionary<DependencyKey, DateTime?>();

    public DatabaseDependencyMetadataProvider(DbContext dbContext, IConsoleService console)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task<IReadOnlyList<ProcedureDependency>> ResolveAsync(IEnumerable<ProcedureDependency> dependencies, CancellationToken cancellationToken)
    {
        if (dependencies == null) return Array.Empty<ProcedureDependency>();
        var list = dependencies.ToList();
        if (list.Count == 0) return Array.Empty<ProcedureDependency>();

        await WarmCatalogsAsync(list, cancellationToken).ConfigureAwait(false);

        var results = new List<ProcedureDependency>(list.Count);
        foreach (var dependency in list)
        {
            var resolved = await ResolveAsync(dependency, cancellationToken).ConfigureAwait(false);
            results.Add(resolved ?? dependency);
        }
        return results;
    }

    public async Task<ProcedureDependency?> ResolveAsync(ProcedureDependency dependency, CancellationToken cancellationToken)
    {
        if (dependency == null) return null;
        try
        {
            await WarmCatalogsAsync(new[] { dependency }, cancellationToken).ConfigureAwait(false);
            var lastModified = await GetLastModifiedAsync(dependency, cancellationToken).ConfigureAwait(false);
            if (lastModified == null && dependency.LastModifiedUtc == null)
            {
                return dependency;
            }
            return new ProcedureDependency
            {
                Kind = dependency.Kind,
                Schema = dependency.Schema,
                Name = dependency.Name,
                LastModifiedUtc = lastModified
            };
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-dependency] failed to resolve {dependency}: {ex.Message}");
            return dependency;
        }
    }

    private async Task<DateTime?> GetLastModifiedAsync(ProcedureDependency dependency, CancellationToken cancellationToken)
    {
        if (dependency == null || string.IsNullOrWhiteSpace(dependency.Name))
        {
            return null;
        }

        var cacheKey = CreateKey(dependency.Kind, dependency.Schema, dependency.Name);

        if (_modifyDateCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        await EnsureCatalogLoadedAsync(dependency.Kind, cancellationToken).ConfigureAwait(false);

        if (_modifyDateCache.TryGetValue(cacheKey, out var refreshed))
        {
            return refreshed;
        }

        _modifyDateCache[cacheKey] = null;
        return null;
    }

    private async Task WarmCatalogsAsync(IEnumerable<ProcedureDependency> dependencies, CancellationToken cancellationToken)
    {
        var groups = dependencies
            .Where(dep => dep != null && dep.Kind != ProcedureDependencyKind.Unknown && !string.IsNullOrWhiteSpace(dep.Name))
            .GroupBy(dep => dep.Kind);

        foreach (var group in groups)
        {
            var snapshot = await EnsureCatalogLoadedAsync(group.Key, cancellationToken).ConfigureAwait(false);
            if (snapshot.Count == 0)
            {
                continue;
            }

            foreach (var dependency in group)
            {
                var key = CreateKey(dependency.Kind, dependency.Schema, dependency.Name);
                if (snapshot.TryGetValue(key, out var modifyDate))
                {
                    _modifyDateCache[key] = modifyDate;
                }
                else if (!_modifyDateCache.ContainsKey(key))
                {
                    _modifyDateCache[key] = null;
                }
            }
        }
    }

    private async Task<IReadOnlyDictionary<DependencyKey, DateTime?>> EnsureCatalogLoadedAsync(ProcedureDependencyKind kind, CancellationToken cancellationToken)
    {
        if (_catalogSnapshots.TryGetValue(kind, out var snapshot))
        {
            return snapshot;
        }

        var loaded = await LoadCatalogAsync(kind, cancellationToken).ConfigureAwait(false);
        return _catalogSnapshots.GetOrAdd(kind, loaded);
    }

    private async Task<IReadOnlyDictionary<DependencyKey, DateTime?>> LoadCatalogAsync(ProcedureDependencyKind kind, CancellationToken cancellationToken)
    {
        IReadOnlyList<SchemaObjectRecord> records = kind switch
        {
            ProcedureDependencyKind.Procedure => await QuerySchemaObjectsAsync(new[] { "P" }, cancellationToken).ConfigureAwait(false),
            ProcedureDependencyKind.Function => await QuerySchemaObjectsAsync(new[] { "FN", "TF", "IF" }, cancellationToken).ConfigureAwait(false),
            ProcedureDependencyKind.View => await QuerySchemaObjectsAsync(new[] { "V" }, cancellationToken).ConfigureAwait(false),
            ProcedureDependencyKind.Table => await QuerySchemaObjectsAsync(new[] { "U" }, cancellationToken).ConfigureAwait(false),
            ProcedureDependencyKind.UserDefinedTableType => await QueryUserDefinedTableTypesAsync(cancellationToken).ConfigureAwait(false),
            ProcedureDependencyKind.UserDefinedType => await QueryUserDefinedScalarTypesAsync(cancellationToken).ConfigureAwait(false),
            _ => Array.Empty<SchemaObjectRecord>()
        };

        if (records.Count == 0)
        {
            return EmptyCatalog;
        }

        var snapshot = new Dictionary<DependencyKey, DateTime?>(records.Count);
        foreach (var record in records)
        {
            if (string.IsNullOrWhiteSpace(record.SchemaName) || string.IsNullOrWhiteSpace(record.ObjectName))
            {
                continue;
            }

            var key = CreateKey(kind, record.SchemaName, record.ObjectName);
            var modifyDate = record.ModifyDate?.ToUniversalTime();
            snapshot[key] = modifyDate;
            _modifyDateCache.AddOrUpdate(key, modifyDate, (_, _) => modifyDate);
        }

        return snapshot;
    }

    private async Task<IReadOnlyList<SchemaObjectRecord>> QuerySchemaObjectsAsync(IReadOnlyList<string> typeCodes, CancellationToken cancellationToken)
    {
        if (typeCodes == null || typeCodes.Count == 0)
        {
            return Array.Empty<SchemaObjectRecord>();
        }

        var predicate = string.Join(", ", typeCodes.Select(static code => $"'{code}'"));
        var query = $@"SELECT s.name AS SchemaName,
                              o.name AS ObjectName,
                              o.modify_date AS ModifyDate
                       FROM sys.objects AS o
                       INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
                       WHERE o.type IN ({predicate})
                         AND o.is_ms_shipped = 0";

        return await _dbContext.ListAsync<SchemaObjectRecord>(query, null, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<SchemaObjectRecord>> QueryUserDefinedTableTypesAsync(CancellationToken cancellationToken)
    {
        const string query = @"SELECT s.name AS SchemaName,
                                       tt.name AS ObjectName,
                                       COALESCE(o.modify_date, o.create_date, CAST('0001-01-01T00:00:00' AS datetime2)) AS ModifyDate
                                FROM sys.table_types AS tt
                                INNER JOIN sys.schemas AS s ON s.schema_id = tt.schema_id
                                LEFT JOIN sys.objects AS o ON o.object_id = tt.type_table_object_id
                                WHERE tt.is_user_defined = 1";

        var records = await _dbContext.ListAsync<SchemaObjectRecord>(query, null, cancellationToken).ConfigureAwait(false);
        return records;
    }

    private async Task<IReadOnlyList<SchemaObjectRecord>> QueryUserDefinedScalarTypesAsync(CancellationToken cancellationToken)
    {
        const string query = @"SELECT s.name AS SchemaName,
                                       t.name AS ObjectName,
                                       COALESCE(o.modify_date, o.create_date, CAST('0001-01-01T00:00:00' AS datetime2)) AS ModifyDate
                                FROM sys.types AS t
                                INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                                LEFT JOIN sys.objects AS o ON o.object_id = t.user_type_id
                                WHERE t.is_user_defined = 1 AND t.is_table_type = 0";

        var records = await _dbContext.ListAsync<SchemaObjectRecord>(query, null, cancellationToken).ConfigureAwait(false);
        return records;
    }

    private static DependencyKey CreateKey(ProcedureDependencyKind kind, string? schema, string name)
    {
        var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? string.Empty : schema.Trim();
        var normalizedName = string.IsNullOrWhiteSpace(name) ? string.Empty : name.Trim();
        return new DependencyKey(kind, normalizedSchema, normalizedName);
    }

    private sealed class SchemaObjectRecord
    {
        public string? SchemaName { get; set; }
        public string? ObjectName { get; set; }
        public DateTime? ModifyDate { get; set; }
    }

    private readonly record struct DependencyKey(ProcedureDependencyKind Kind, string Schema, string Name);
}
