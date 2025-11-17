using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Cache;

/// <summary>
/// Comprehensive cache manager for all SQL Server schema objects.
/// Provides delta-detection and dependency-based invalidation for:
/// - Stored Procedures
/// - Functions (Scalar and Table-Valued)
/// - Views
/// - Tables
/// - User-Defined Table Types (UDTT)
/// - User-Defined Data Types (UDT)
/// </summary>
internal interface ISchemaObjectCacheManager
{
    /// <summary>
    /// Initialize the cache manager for the current project.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get the last known modification timestamp for a schema object.
    /// Returns null if the object is not in the cache.
    /// </summary>
    DateTime? GetLastModified(SchemaObjectType objectType, string schema, string name);

    /// <summary>
    /// Get the timestamp of the most recent cache persistence.
    /// Returns null if the cache has not been persisted yet.
    /// </summary>
    DateTime? GetLastUpdatedUtc();

    /// <summary>
    /// Persist the reference timestamp representing the latest catalog modification observed.
    /// </summary>
    Task UpdateReferenceTimestampAsync(DateTime referenceTimestampUtc);

    /// <summary>
    /// Update the modification timestamp for a schema object.
    /// </summary>
    Task UpdateLastModifiedAsync(SchemaObjectType objectType, string schema, string name, DateTime lastModifiedUtc);
    /// <summary>
    /// Remove cached entry for the specified schema object and detach it from the dependency graph.
    /// </summary>
    Task RemoveAsync(SchemaObjectRef objectRef, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get objects that have been modified since the specified timestamp.
    /// Used for delta-detection to only snapshot changed objects.
    /// </summary>
    Task<IReadOnlyList<SchemaObjectRef>> GetModifiedSinceAsync(SchemaObjectType objectType, DateTime sinceUtc);

    /// <summary>
    /// Invalidate cache entries for objects that depend on the specified object.
    /// For example, invalidate all procedures that use a UDT when the UDT changes.
    /// </summary>
    Task InvalidateDependentsAsync(SchemaObjectRef changedObject);

    /// <summary>
    /// Get direct dependents for a given schema object.
    /// </summary>
    IReadOnlyList<SchemaObjectRef> GetDependents(SchemaObjectRef dependency);

    /// <summary>
    /// Record a dependency relationship between two schema objects.
    /// </summary>
    Task RecordDependencyAsync(SchemaObjectRef dependent, SchemaObjectRef dependency);

    /// <summary>
    /// Replace the dependency relationships tracked for a schema object.
    /// </summary>
    Task SetDependenciesAsync(SchemaObjectRef dependent, IReadOnlyList<SchemaObjectRef> dependencies);

    /// <summary>
    /// Flush all pending changes to disk.
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Clear all cache entries (force full reload).
    /// </summary>
    Task ClearAllAsync();
}

/// <summary>
/// Types of SQL Server schema objects that can be cached.
/// </summary>
public enum SchemaObjectType
{
    /// <summary>
    /// Represents a stored procedure object.
    /// </summary>
    StoredProcedure,
    /// <summary>
    /// Represents a scalar user-defined function.
    /// </summary>
    ScalarFunction,
    /// <summary>
    /// Represents a table-valued user-defined function.
    /// </summary>
    TableValuedFunction,
    /// <summary>
    /// Represents a view definition.
    /// </summary>
    View,
    /// <summary>
    /// Represents a table definition.
    /// </summary>
    Table,
    /// <summary>
    /// Represents a user-defined table type.
    /// </summary>
    UserDefinedTableType,
    /// <summary>
    /// Represents a user-defined data type.
    /// </summary>
    UserDefinedDataType
}

/// <summary>
/// Reference to a schema object (schema.name).
/// </summary>
public record SchemaObjectRef(SchemaObjectType Type, string Schema, string Name)
{
    /// <summary>
    /// Gets the fully qualified name in the format <c>schema.object</c>.
    /// </summary>
    public string FullName => $"{Schema}.{Name}";

    /// <inheritdoc />
    public override string ToString() => $"{Type}:{FullName}";
}

/// <summary>
/// Cache entry for a schema object with modification tracking.
/// </summary>
public class SchemaObjectCacheEntry
{
    /// <summary>
    /// Gets or sets the schema object type represented by the cache entry.
    /// </summary>
    public SchemaObjectType Type { get; set; }
    /// <summary>
    /// Gets or sets the owning schema for the object.
    /// </summary>
    public string Schema { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the object name within the schema.
    /// </summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the last modification timestamp observed for the object.
    /// </summary>
    public DateTime LastModifiedUtc { get; set; }
    /// <summary>
    /// Gets or sets the timestamp when the entry was cached.
    /// </summary>
    public DateTime CachedUtc { get; set; } = DateTime.UtcNow;
    /// <summary>
    /// Gets or sets the schema object dependencies tracked for invalidation.
    /// </summary>
    public List<SchemaObjectRef> Dependencies { get; set; } = new();
    /// <summary>
    /// Gets or sets an optional hash of the object content for change detection.
    /// </summary>
    public string? ContentHash { get; set; }
}

internal class SchemaObjectCacheManager : ISchemaObjectCacheManager
{
    private readonly IConsoleService _console;
    private readonly object _sync = new();
    private readonly Dictionary<string, SchemaObjectCacheEntry> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<string>> _dependencyGraph = new(StringComparer.OrdinalIgnoreCase);

    private bool _initialized;
    private bool _dirty;
    private string _cacheDirectory = string.Empty;
    private string _cacheFilePath = string.Empty;
    private string _dependencyGraphFilePath = string.Empty;
    private DateTime? _referenceTimestampUtc;

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    };

    private static readonly SchemaObjectRefEqualityComparer ObjectComparer = SchemaObjectRefEqualityComparer.Instance;

    public SchemaObjectCacheManager(IConsoleService console)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized) return;

        var projectRoot = ProjectRootResolver.ResolveCurrent();
        _cacheDirectory = Path.Combine(projectRoot, ".xtraq", "cache");
        _cacheFilePath = Path.Combine(_cacheDirectory, "schema-objects.json");
        _dependencyGraphFilePath = Path.Combine(_cacheDirectory, "dependency-graph.json");

        Directory.CreateDirectory(_cacheDirectory);

        if (File.Exists(_cacheFilePath))
        {
            await LoadCacheAsync(cancellationToken).ConfigureAwait(false);
        }

        _initialized = true;
    }

    public DateTime? GetLastModified(SchemaObjectType objectType, string schema, string name)
    {
        var key = BuildKey(objectType, schema, name);
        lock (_sync)
        {
            return _cache.TryGetValue(key, out var entry) ? entry.LastModifiedUtc : null;
        }
    }

    public DateTime? GetLastUpdatedUtc()
    {
        lock (_sync)
        {
            return _referenceTimestampUtc;
        }
    }

    public Task UpdateReferenceTimestampAsync(DateTime referenceTimestampUtc)
    {
        lock (_sync)
        {
            var normalized = DateTime.SpecifyKind(referenceTimestampUtc, DateTimeKind.Utc);
            if (_referenceTimestampUtc.HasValue && _referenceTimestampUtc.Value >= normalized)
            {
                return Task.CompletedTask;
            }

            _referenceTimestampUtc = normalized;
            _dirty = true;
        }

        return Task.CompletedTask;
    }

    public Task UpdateLastModifiedAsync(SchemaObjectType objectType, string schema, string name, DateTime lastModifiedUtc)
    {
        var key = BuildKey(objectType, schema, name);
        lock (_sync)
        {
            if (_cache.TryGetValue(key, out var existing))
            {
                existing.LastModifiedUtc = lastModifiedUtc;
                existing.CachedUtc = DateTime.UtcNow;
            }
            else
            {
                _cache[key] = new SchemaObjectCacheEntry
                {
                    Type = objectType,
                    Schema = schema,
                    Name = name,
                    LastModifiedUtc = lastModifiedUtc,
                    CachedUtc = DateTime.UtcNow
                };
            }
            _dirty = true;
        }
        return Task.CompletedTask;
    }

    public Task RemoveAsync(SchemaObjectRef objectRef, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(objectRef);
        cancellationToken.ThrowIfCancellationRequested();

        var key = BuildKey(objectRef);
        lock (_sync)
        {
            if (_cache.Remove(key, out var entry) && entry.Dependencies.Count > 0)
            {
                foreach (var dependency in entry.Dependencies)
                {
                    var dependencyKey = BuildKey(dependency);
                    if (_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
                    {
                        dependents.Remove(key);
                        if (dependents.Count == 0)
                        {
                            _dependencyGraph.Remove(dependencyKey);
                        }
                    }
                }
            }

            // Remove dependent edges pointing at the removed node
            if (_dependencyGraph.Remove(key, out var downstreamDependents) && downstreamDependents.Count > 0)
            {
                foreach (var dependentKey in downstreamDependents)
                {
                    if (_cache.TryGetValue(dependentKey, out var dependentEntry))
                    {
                        dependentEntry.Dependencies.RemoveAll(dep => ObjectComparer.Equals(dep, objectRef));
                    }
                }
            }

            _dirty = true;
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<SchemaObjectRef>> GetModifiedSinceAsync(SchemaObjectType objectType, DateTime sinceUtc)
    {
        List<SchemaObjectRef> modified;
        lock (_sync)
        {
            modified = _cache.Values
                .Where(entry => entry.Type == objectType && entry.LastModifiedUtc > sinceUtc)
                .Select(entry => new SchemaObjectRef(entry.Type, entry.Schema, entry.Name))
                .ToList();
        }
        return Task.FromResult<IReadOnlyList<SchemaObjectRef>>(modified);
    }

    public Task InvalidateDependentsAsync(SchemaObjectRef changedObject)
    {
        if (changedObject is null)
        {
            throw new ArgumentNullException(nameof(changedObject));
        }

        var changedKey = BuildKey(changedObject);
        List<string> invalidatedKeys;

        lock (_sync)
        {
            if (!_dependencyGraph.TryGetValue(changedKey, out var directDependents) || directDependents.Count == 0)
            {
                return Task.CompletedTask;
            }

            invalidatedKeys = TraverseAndMarkDependentsLocked(directDependents);
        }

        if (invalidatedKeys.Count > 0)
        {
            _console.Verbose($"[schema-cache] Invalidating {invalidatedKeys.Count} objects dependent on {changedObject}");
        }

        return Task.CompletedTask;
    }

    public IReadOnlyList<SchemaObjectRef> GetDependents(SchemaObjectRef dependency)
    {
        var dependencyKey = BuildKey(dependency);
        lock (_sync)
        {
            if (!_dependencyGraph.TryGetValue(dependencyKey, out var dependents) || dependents.Count == 0)
            {
                return Array.Empty<SchemaObjectRef>();
            }

            return dependents.Select(ParseKey).Where(static dep => dep is not null).Cast<SchemaObjectRef>().ToArray();
        }
    }

    public Task SetDependenciesAsync(SchemaObjectRef dependent, IReadOnlyList<SchemaObjectRef> dependencies)
    {
        if (dependent is null)
        {
            throw new ArgumentNullException(nameof(dependent));
        }

        lock (_sync)
        {
            var dependentKey = BuildKey(dependent);
            var entry = EnsureCacheEntry(dependentKey, dependent);

            var targetSet = dependencies is null
                ? new HashSet<SchemaObjectRef>(ObjectComparer)
                : new HashSet<SchemaObjectRef>(dependencies.Where(static d => d is not null), ObjectComparer);

            var existingSet = new HashSet<SchemaObjectRef>(entry.Dependencies, ObjectComparer);
            var duplicatesDetected = existingSet.Count != entry.Dependencies.Count;

            if (!duplicatesDetected && existingSet.SetEquals(targetSet))
            {
                return Task.CompletedTask;
            }

            foreach (var dependencyKey in existingSet.Select(static dep => BuildKey(dep)))
            {
                if (_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
                {
                    dependents.Remove(dependentKey);
                    if (dependents.Count == 0)
                    {
                        _dependencyGraph.Remove(dependencyKey);
                    }
                }
            }

            foreach (var dependencyRef in targetSet)
            {
                var dependencyKey = BuildKey(dependencyRef);
                if (!_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
                {
                    dependents = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    _dependencyGraph[dependencyKey] = dependents;
                }

                dependents.Add(dependentKey);
            }

            entry.Dependencies = targetSet
                .OrderBy(static dep => dep.Type)
                .ThenBy(static dep => dep.Schema, StringComparer.OrdinalIgnoreCase)
                .ThenBy(static dep => dep.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            _dirty = true;
        }

        return Task.CompletedTask;
    }

    public Task RecordDependencyAsync(SchemaObjectRef dependent, SchemaObjectRef dependency)
    {
        if (dependent is null)
        {
            throw new ArgumentNullException(nameof(dependent));
        }

        if (dependency is null)
        {
            throw new ArgumentNullException(nameof(dependency));
        }

        lock (_sync)
        {
            var dependentKey = BuildKey(dependent);
            var dependencyKey = BuildKey(dependency);
            var entry = EnsureCacheEntry(dependentKey, dependent);

            if (!entry.Dependencies.Any(dep => ObjectComparer.Equals(dep, dependency)))
            {
                entry.Dependencies.Add(dependency);
                _dirty = true;
            }

            if (!_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
            {
                dependents = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _dependencyGraph[dependencyKey] = dependents;
            }

            dependents.Add(dependentKey);
        }

        return Task.CompletedTask;
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (!_dirty) return;

        List<SchemaObjectCacheEntry> entries;
        List<DependencyGraphNode> graphSnapshot;
        lock (_sync)
        {
            entries = _cache.Values.ToList();
            graphSnapshot = _cache.Values
                .Select(entry => new DependencyGraphNode
                {
                    Type = entry.Type,
                    Schema = entry.Schema,
                    Name = entry.Name,
                    Dependencies = entry.Dependencies.Select(static dependency => new SchemaObjectRef(dependency.Type, dependency.Schema, dependency.Name)).ToList(),
                    Dependents = _dependencyGraph.TryGetValue(BuildKey(entry.Type, entry.Schema, entry.Name), out var dependents)
                        ? dependents.Select(ParseKey).Where(static dep => dep is not null).Cast<SchemaObjectRef>().ToList()
                        : new List<SchemaObjectRef>()
                })
                .ToList();
            _dirty = false;
        }

        try
        {
            var now = DateTime.UtcNow;
            DateTime referenceTimestamp;
            lock (_sync)
            {
                referenceTimestamp = _referenceTimestampUtc ?? now;
            }
            var document = new SchemaCacheDocument
            {
                Version = 1,
                LastUpdatedUtc = referenceTimestamp,
                Entries = entries.OrderBy(e => e.Type).ThenBy(e => e.Schema).ThenBy(e => e.Name).ToList()
            };
            var dependencyDocument = new DependencyGraphDocument
            {
                Version = 1,
                LastUpdatedUtc = now,
                Nodes = graphSnapshot
                    .OrderBy(node => node.Type)
                    .ThenBy(node => node.Schema, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(node => node.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList()
            };

            var tempFile = _cacheFilePath + ".tmp";
            await using (var stream = File.Create(tempFile))
            {
                await JsonSerializer.SerializeAsync(stream, document, SerializerOptions, cancellationToken).ConfigureAwait(false);
            }

            var tempGraphFile = _dependencyGraphFilePath + ".tmp";
            await using (var stream = File.Create(tempGraphFile))
            {
                await JsonSerializer.SerializeAsync(stream, dependencyDocument, SerializerOptions, cancellationToken).ConfigureAwait(false);
            }

            if (File.Exists(_cacheFilePath))
            {
                File.Replace(tempFile, _cacheFilePath, null);
            }
            else
            {
                File.Move(tempFile, _cacheFilePath);
            }

            if (File.Exists(_dependencyGraphFilePath))
            {
                File.Replace(tempGraphFile, _dependencyGraphFilePath, null);
            }
            else
            {
                File.Move(tempGraphFile, _dependencyGraphFilePath);
            }

            lock (_sync)
            {
                if (!_referenceTimestampUtc.HasValue)
                {
                    _referenceTimestampUtc = referenceTimestamp;
                }
            }
            _console.Verbose($"[schema-cache] Persisted {entries.Count} schema object entries to cache");
        }
        catch (Exception ex)
        {
            _dirty = true;
            _console.Verbose($"[schema-cache] Failed to persist cache: {ex.Message}");
        }
    }

    public Task ClearAllAsync()
    {
        lock (_sync)
        {
            _cache.Clear();
            _dependencyGraph.Clear();
            _dirty = true;
            _referenceTimestampUtc = null;
        }

        try
        {
            if (File.Exists(_cacheFilePath))
            {
                File.Delete(_cacheFilePath);
            }
            if (File.Exists(_dependencyGraphFilePath))
            {
                File.Delete(_dependencyGraphFilePath);
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-cache] Failed to delete cache file: {ex.Message}");
        }

        return Task.CompletedTask;
    }

    private SchemaObjectCacheEntry EnsureCacheEntry(string key, SchemaObjectRef reference)
    {
        if (!_cache.TryGetValue(key, out var entry))
        {
            entry = new SchemaObjectCacheEntry
            {
                Type = reference.Type,
                Schema = reference.Schema,
                Name = reference.Name,
                LastModifiedUtc = DateTime.MinValue,
                CachedUtc = DateTime.UtcNow
            };
            _cache[key] = entry;
        }
        else
        {
            entry.Type = reference.Type;
            entry.Schema = reference.Schema;
            entry.Name = reference.Name;
        }

        return entry;
    }

    private List<string> TraverseAndMarkDependentsLocked(IEnumerable<string> startingKeys)
    {
        // Called from within _sync lock to walk dependency graph breadth-first
        var queue = new Queue<string>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var invalidated = new List<string>();

        foreach (var key in startingKeys)
        {
            if (visited.Add(key))
            {
                queue.Enqueue(key);
            }
        }

        while (queue.Count > 0)
        {
            var currentKey = queue.Dequeue();
            invalidated.Add(currentKey);

            if (_cache.TryGetValue(currentKey, out var entry) && entry.LastModifiedUtc != DateTime.MinValue)
            {
                entry.LastModifiedUtc = DateTime.MinValue;
                _dirty = true;
            }

            if (_dependencyGraph.TryGetValue(currentKey, out var dependents) && dependents.Count > 0)
            {
                foreach (var dependentKey in dependents)
                {
                    if (visited.Add(dependentKey))
                    {
                        queue.Enqueue(dependentKey);
                    }
                }
            }
        }

        return invalidated;
    }

    private async Task LoadCacheAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = File.OpenRead(_cacheFilePath);
            var document = await JsonSerializer.DeserializeAsync<SchemaCacheDocument>(stream, SerializerOptions, cancellationToken).ConfigureAwait(false);

            if (document?.Entries == null) return;

            lock (_sync)
            {
                _cache.Clear();
                _dependencyGraph.Clear();
                _referenceTimestampUtc = document.LastUpdatedUtc;

                foreach (var entry in document.Entries)
                {
                    var key = BuildKey(entry.Type, entry.Schema, entry.Name);
                    _cache[key] = entry;

                    // Rebuild dependency graph
                    foreach (var dependency in entry.Dependencies)
                    {
                        var dependencyKey = BuildKey(dependency);
                        if (!_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
                        {
                            dependents = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _dependencyGraph[dependencyKey] = dependents;
                        }
                        dependents.Add(key);
                    }
                }
            }

            _console.Verbose($"[schema-cache] Loaded {document.Entries.Count} schema object entries from cache");
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-cache] Failed to load cache: {ex.Message}");
        }
    }

    private static string BuildKey(SchemaObjectType objectType, string schema, string name)
    {
        return $"{objectType}:{schema}.{name}";
    }

    private static string BuildKey(SchemaObjectRef objectRef)
    {
        return BuildKey(objectRef.Type, objectRef.Schema, objectRef.Name);
    }

    private static SchemaObjectRef? ParseKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return null;
        }

        var typeSeparator = key.IndexOf(':');
        if (typeSeparator < 0 || typeSeparator == key.Length - 1)
        {
            return null;
        }

        var typeSegment = key[..typeSeparator];
        var identifier = key[(typeSeparator + 1)..];
        var nameSeparator = identifier.IndexOf('.');
        if (nameSeparator <= 0 || nameSeparator == identifier.Length - 1)
        {
            return null;
        }

        if (!Enum.TryParse(typeSegment, out SchemaObjectType type))
        {
            return null;
        }

        var schema = identifier[..nameSeparator];
        var name = identifier[(nameSeparator + 1)..];
        return new SchemaObjectRef(type, schema, name);
    }

    private sealed class SchemaObjectRefEqualityComparer : IEqualityComparer<SchemaObjectRef>
    {
        internal static SchemaObjectRefEqualityComparer Instance { get; } = new();

        public bool Equals(SchemaObjectRef? x, SchemaObjectRef? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (x is null || y is null)
            {
                return false;
            }

            return x.Type == y.Type
                && string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(SchemaObjectRef obj)
        {
            unchecked
            {
                var hash = (int)obj.Type;
                hash = (hash * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Schema);
                hash = (hash * 397) ^ StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Name);
                return hash;
            }
        }
    }

    private sealed class SchemaCacheDocument
    {
        public int Version { get; set; }
        public DateTime LastUpdatedUtc { get; set; }
        public List<SchemaObjectCacheEntry> Entries { get; set; } = new();
    }

    private sealed class DependencyGraphDocument
    {
        public int Version { get; set; }
        public DateTime LastUpdatedUtc { get; set; }
        public List<DependencyGraphNode> Nodes { get; set; } = new();
    }

    private sealed class DependencyGraphNode
    {
        public SchemaObjectType Type { get; set; }
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public List<SchemaObjectRef> Dependencies { get; set; } = new();
        public List<SchemaObjectRef> Dependents { get; set; } = new();
    }
}
