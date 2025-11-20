using Xtraq.Cache;
using Xtraq.Data;

namespace Xtraq.Services;

/// <summary>
/// Orchestrates schema object cache invalidation based on dependency relationships.
/// Ensures that when an object changes, all dependent objects are properly invalidated.
/// </summary>
internal interface ISchemaInvalidationOrchestrator
{
    /// <summary>
    /// Initialize the orchestrator for the current project.
    /// </summary>
    Task InitializeAsync(string connectionString, CancellationToken cancellationToken = default);

    /// <summary>
    /// Analyze schema changes and perform dependency-based cache invalidation.
    /// Returns list of objects that need to be refreshed.
    /// </summary>
    Task<SchemaInvalidationResult> AnalyzeAndInvalidateAsync(
        IReadOnlyList<string>? schemaFilter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Force invalidation of specific objects and their dependents.
    /// </summary>
    Task InvalidateObjectsAsync(
        IReadOnlyList<SchemaObjectRef> objects,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of schema invalidation analysis.
/// </summary>
internal sealed class SchemaInvalidationResult
{
    /// <summary>
    /// Objects that have been modified since last cache update.
    /// </summary>
    public IReadOnlyList<SchemaObjectRef> ModifiedObjects { get; set; } = Array.Empty<SchemaObjectRef>();

    /// <summary>
    /// Objects that were invalidated due to dependency changes.
    /// </summary>
    public IReadOnlyList<SchemaObjectRef> InvalidatedObjects { get; set; } = Array.Empty<SchemaObjectRef>();

    /// <summary>
    /// Objects that were deleted from the catalog since the last refresh.
    /// </summary>
    public IReadOnlyList<SchemaObjectRef> RemovedObjects { get; set; } = Array.Empty<SchemaObjectRef>();

    /// <summary>
    /// All objects that need to be refreshed (modified + invalidated).
    /// </summary>
    public IReadOnlyList<SchemaObjectRef> ObjectsToRefresh =>
        RefreshPlan.Count > 0
            ? RefreshPlan
                .SelectMany(batch => batch.Entries)
                .Select(entry => entry.Object)
                .Distinct(SchemaInvalidationExtensions.SchemaObjectRefComparer.Instance)
                .ToList()
            : ModifiedObjects
                .Concat(InvalidatedObjects)
                .Distinct(SchemaInvalidationExtensions.SchemaObjectRefComparer.Instance)
                .ToList();

    /// <summary>
    /// Reference timestamp for next delta query.
    /// </summary>
    public DateTime NextReferenceTimestamp { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Planned refresh batches grouped by schema.
    /// </summary>
    public IReadOnlyList<SchemaRefreshBatch> RefreshPlan { get; set; } = Array.Empty<SchemaRefreshBatch>();

    /// <summary>
    /// Objects that were skipped because they do not match the configured schema filter.
    /// </summary>
    public IReadOnlyList<SchemaObjectRef> SkippedObjects { get; set; } = Array.Empty<SchemaObjectRef>();
}

/// <summary>
/// Reason why a schema object is scheduled for refresh.
/// </summary>
internal enum SchemaRefreshReason
{
    Modified = 0,
    Dependency = 1
}

/// <summary>
/// Represents a single refresh entry for a schema object.
/// </summary>
internal sealed class SchemaRefreshEntry
{
    public SchemaRefreshEntry(SchemaObjectRef @object, SchemaRefreshReason reason)
    {
        Object = @object ?? throw new ArgumentNullException(nameof(@object));
        Reason = reason;
    }

    /// <summary>
    /// The schema object scheduled for refresh.
    /// </summary>
    public SchemaObjectRef Object { get; }

    /// <summary>
    /// Indicates whether the object was directly modified or scheduled due to dependencies.
    /// </summary>
    public SchemaRefreshReason Reason { get; }
}

/// <summary>
/// Groups refresh entries by schema to align with XTRAQ_BUILD_SCHEMAS batching.
/// </summary>
internal sealed class SchemaRefreshBatch
{
    public SchemaRefreshBatch(string schema, IReadOnlyList<SchemaRefreshEntry> entries)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        Entries = entries ?? throw new ArgumentNullException(nameof(entries));
    }

    /// <summary>
    /// Schema name (case-preserved as observed from metadata).
    /// </summary>
    public string Schema { get; }

    /// <summary>
    /// Objects scheduled for refresh within the schema.
    /// </summary>
    public IReadOnlyList<SchemaRefreshEntry> Entries { get; }

    /// <summary>
    /// Count of entries in this batch.
    /// </summary>
    public int Count => Entries.Count;
}

internal sealed class SchemaInvalidationOrchestrator : ISchemaInvalidationOrchestrator
{
    private readonly ISchemaObjectCacheManager _cacheManager;
    private readonly ISchemaChangeDetectionService _changeDetection;
    private readonly IConsoleService _console;

    private bool _initialized;

    public SchemaInvalidationOrchestrator(
        ISchemaObjectCacheManager cacheManager,
        ISchemaChangeDetectionService changeDetection,
        IConsoleService console)
    {
        _cacheManager = cacheManager ?? throw new ArgumentNullException(nameof(cacheManager));
        _changeDetection = changeDetection ?? throw new ArgumentNullException(nameof(changeDetection));
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task InitializeAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        if (_initialized) return;

        _changeDetection.Initialize(connectionString);
        await _cacheManager.InitializeAsync(cancellationToken).ConfigureAwait(false);

        _initialized = true;
    }

    public async Task<SchemaInvalidationResult> AnalyzeAndInvalidateAsync(
        IReadOnlyList<string>? schemaFilter = null,
        CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            throw new InvalidOperationException("Orchestrator not initialized. Call InitializeAsync() first.");
        }

        var result = new SchemaInvalidationResult();
        var modifiedObjects = new List<SchemaObjectRef>();
        var invalidatedObjects = new HashSet<SchemaObjectRef>();
        var removedObjects = new List<SchemaObjectRef>();
        var schemaFilterSet = schemaFilter is { Count: > 0 }
            ? new HashSet<string>(schemaFilter, StringComparer.OrdinalIgnoreCase)
            : null;

        // Check each object type for modifications
        var objectTypes = Enum.GetValues<SchemaObjectType>();
        foreach (var objectType in objectTypes)
        {
            await ProcessObjectTypeAsync(objectType, schemaFilter, modifiedObjects, invalidatedObjects, removedObjects, cancellationToken)
                .ConfigureAwait(false);
        }

        // Update reference timestamp
        result.NextReferenceTimestamp = await _changeDetection.GetMaxModificationTimeAsync(cancellationToken)
            .ConfigureAwait(false);

        result.ModifiedObjects = modifiedObjects;
        result.InvalidatedObjects = invalidatedObjects.ToList();
        result.RemovedObjects = removedObjects.ToList();

        var (plan, skipped) = BuildRefreshPlan(modifiedObjects, invalidatedObjects, schemaFilterSet);
        result.RefreshPlan = plan;
        result.SkippedObjects = skipped;

        await _cacheManager.UpdateReferenceTimestampAsync(result.NextReferenceTimestamp).ConfigureAwait(false);

        // Flush cache changes
        await _cacheManager.FlushAsync(cancellationToken).ConfigureAwait(false);
        await _changeDetection.FlushIndexAsync(cancellationToken).ConfigureAwait(false);

        if (result.ObjectsToRefresh.Count > 0 || result.RemovedObjects.Count > 0)
        {
            var removedSummary = result.RemovedObjects.Count > 0
                ? $", {result.RemovedObjects.Count} removed"
                : string.Empty;
            _console.Output($"[schema-invalidation] {modifiedObjects.Count} modified, {invalidatedObjects.Count} invalidated objects{removedSummary}");
        }

        if (result.SkippedObjects.Count > 0)
        {
            var summary = string.Join(", ", result.SkippedObjects.Select(static obj => obj.FullName));
            _console.Verbose($"[schema-invalidation] Skipped {result.SkippedObjects.Count} object(s) due to schema filter: {summary}");
        }

        return result;
    }

    public async Task InvalidateObjectsAsync(
        IReadOnlyList<SchemaObjectRef> objects,
        CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            throw new InvalidOperationException("Orchestrator not initialized. Call InitializeAsync() first.");
        }

        foreach (var obj in objects)
        {
            await _cacheManager.InvalidateDependentsAsync(obj).ConfigureAwait(false);
        }

        await _cacheManager.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessObjectTypeAsync(
        SchemaObjectType objectType,
        IReadOnlyList<string>? schemaFilter,
        List<SchemaObjectRef> modifiedObjects,
        HashSet<SchemaObjectRef> invalidatedObjects,
        List<SchemaObjectRef> removedObjects,
        CancellationToken cancellationToken)
    {
        try
        {
            // Get the earliest cached timestamp for this object type
            var sinceUtc = await GetEarliestCacheTimestampAsync(objectType).ConfigureAwait(false);

            // Get modified objects from database
            var changeSet = await _changeDetection.GetObjectChangesAsync(objectType, sinceUtc, schemaFilter, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            var dbObjects = changeSet.Modified;

            foreach (var dbObject in dbObjects)
            {
                var objectRef = new SchemaObjectRef(dbObject.Type, dbObject.Schema, dbObject.Name);

                // Check if object is truly modified (not just different cache state)
                var cachedModified = _cacheManager.GetLastModified(objectType, dbObject.Schema, dbObject.Name);
                if (cachedModified.HasValue && dbObject.ModifiedUtc <= cachedModified.Value)
                {
                    continue; // No actual change
                }

                modifiedObjects.Add(objectRef);

                // Update cache with new modification time
                await _cacheManager.UpdateLastModifiedAsync(objectType, dbObject.Schema, dbObject.Name, dbObject.ModifiedUtc)
                    .ConfigureAwait(false);

                // Get and record dependencies
                await UpdateDependenciesAsync(objectRef, cancellationToken).ConfigureAwait(false);

                // Invalidate dependent objects
                await _cacheManager.InvalidateDependentsAsync(objectRef).ConfigureAwait(false);

                // Add invalidated dependents to result
                await CollectInvalidatedDependentsAsync(objectRef, invalidatedObjects, cancellationToken).ConfigureAwait(false);
            }

            if (changeSet.Removed.Count > 0)
            {
                foreach (var removedObject in changeSet.Removed)
                {
                    await _cacheManager.InvalidateDependentsAsync(removedObject).ConfigureAwait(false);
                    await CollectInvalidatedDependentsAsync(removedObject, invalidatedObjects, cancellationToken).ConfigureAwait(false);
                    await _cacheManager.RemoveAsync(removedObject, cancellationToken).ConfigureAwait(false);
                    removedObjects.Add(removedObject);
                }
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-invalidation] Failed to process {objectType}: {ex.Message}");
        }
    }

    private Task<DateTime?> GetEarliestCacheTimestampAsync(SchemaObjectType objectType)
    {
        // When the cache has not been persisted yet (or was cleared), force a full scan by returning null.
        // Otherwise reuse the persisted reference timestamp so delta queries can execute.
        _ = objectType;
        var reference = _cacheManager.GetLastUpdatedUtc();
        return Task.FromResult(reference);
    }

    private async Task UpdateDependenciesAsync(SchemaObjectRef objectRef, CancellationToken cancellationToken)
    {
        try
        {
            var dependencies = await _changeDetection.GetDependenciesAsync(objectRef, cancellationToken)
                .ConfigureAwait(false) ?? Array.Empty<SchemaObjectRef>();

            await _cacheManager.SetDependenciesAsync(objectRef, dependencies).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-invalidation] Failed to update dependencies for {objectRef}: {ex.Message}");
        }
    }

    private Task CollectInvalidatedDependentsAsync(
        SchemaObjectRef changedObject,
        HashSet<SchemaObjectRef> invalidatedObjects,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        var comparer = SchemaInvalidationExtensions.SchemaObjectRefComparer.Instance;
        var visited = new HashSet<SchemaObjectRef>(comparer);
        var queue = new Queue<SchemaObjectRef>();

        foreach (var dependent in _cacheManager.GetDependents(changedObject))
        {
            if (visited.Add(dependent))
            {
                invalidatedObjects.Add(dependent);
                queue.Enqueue(dependent);
            }
        }

        // Breadth-first traversal covers transitive dependents without revisiting nodes.
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            foreach (var next in _cacheManager.GetDependents(current))
            {
                if (visited.Add(next))
                {
                    invalidatedObjects.Add(next);
                    queue.Enqueue(next);
                }
            }
        }

        return Task.CompletedTask;
    }

    private static (IReadOnlyList<SchemaRefreshBatch> Plan, IReadOnlyList<SchemaObjectRef> Skipped) BuildRefreshPlan(
        IReadOnlyList<SchemaObjectRef> modifiedObjects,
        IReadOnlyCollection<SchemaObjectRef> invalidatedObjects,
        HashSet<string>? schemaFilter)
    {
        var reasonMap = new Dictionary<SchemaObjectRef, SchemaRefreshReason>(SchemaInvalidationExtensions.SchemaObjectRefComparer.Instance);

        foreach (var modified in modifiedObjects)
        {
            reasonMap[modified] = SchemaRefreshReason.Modified;
        }

        foreach (var invalidated in invalidatedObjects)
        {
            if (!reasonMap.ContainsKey(invalidated))
            {
                reasonMap[invalidated] = SchemaRefreshReason.Dependency;
            }
        }

        var planMap = new Dictionary<string, List<SchemaRefreshEntry>>(StringComparer.OrdinalIgnoreCase);
        var skipped = new List<SchemaObjectRef>();

        foreach (var pair in reasonMap)
        {
            var obj = pair.Key;
            if (schemaFilter != null && schemaFilter.Count > 0 && !schemaFilter.Contains(obj.Schema))
            {
                skipped.Add(obj);
                continue;
            }

            if (!planMap.TryGetValue(obj.Schema, out var entries))
            {
                entries = new List<SchemaRefreshEntry>();
                planMap[obj.Schema] = entries;
            }

            entries.Add(new SchemaRefreshEntry(obj, pair.Value));
        }

        var plan = planMap
            .OrderBy(static kvp => kvp.Key, StringComparer.OrdinalIgnoreCase)
            .Select(kvp =>
            {
                var orderedEntries = kvp.Value
                    .OrderBy(static entry => entry.Reason)
                    .ThenBy(static entry => entry.Object.Type)
                    .ThenBy(static entry => entry.Object.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                return new SchemaRefreshBatch(kvp.Key, orderedEntries);
            })
            .ToList();

        skipped.Sort(static (left, right) => string.Compare(left.FullName, right.FullName, StringComparison.OrdinalIgnoreCase));

        return (plan, skipped);
    }
}

/// <summary>
/// Extension methods for integrating schema invalidation with existing services.
/// </summary>
internal static class SchemaInvalidationExtensions
{
    /// <summary>
    /// Check if cache invalidation indicates that a full refresh is needed.
    /// </summary>
    internal static bool RequiresFullRefresh(this SchemaInvalidationResult result)
    {
        // Consider a full refresh if more than 50% of typical objects need updating
        // or if critical dependency objects (like UDTs) have changed

        const int fullRefreshThreshold = 100;
        if (result.ObjectsToRefresh.Count > fullRefreshThreshold)
        {
            return true;
        }

        // Check for critical dependencies
        var criticalTypes = new[] { SchemaObjectType.UserDefinedTableType, SchemaObjectType.UserDefinedDataType };
        var hasCriticalChanges = result.ModifiedObjects
            .Concat(result.RemovedObjects)
            .Any(obj => criticalTypes.Contains(obj.Type));

        return hasCriticalChanges;
    }

    /// <summary>
    /// Get objects grouped by type for efficient processing.
    /// </summary>
    internal static IReadOnlyDictionary<SchemaObjectType, IReadOnlyList<SchemaObjectRef>> GetObjectsByType(
        this SchemaInvalidationResult result)
    {
        return result.ObjectsToRefresh
            .GroupBy(obj => obj.Type)
            .ToDictionary(g => g.Key, g => (IReadOnlyList<SchemaObjectRef>)g.ToList());
    }

    /// <summary>
    /// Provides consistent equality semantics for schema object references.
    /// </summary>
    internal sealed class SchemaObjectRefComparer : IEqualityComparer<SchemaObjectRef>
    {
        public static SchemaObjectRefComparer Instance { get; } = new();

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
}
