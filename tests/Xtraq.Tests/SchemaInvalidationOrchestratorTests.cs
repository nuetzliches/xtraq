using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Cache;
using Xtraq.Data;
using Xtraq.Services;
using Xunit;

namespace Xtraq.Tests.Cache;

/// <summary>
/// Regression coverage for <see cref="SchemaInvalidationOrchestrator"/> focusing on cache invalidation edge cases.
/// </summary>
public sealed class SchemaInvalidationOrchestratorTests
{
    private static readonly SchemaObjectChangeSet EmptyChangeSet = new()
    {
        Modified = Array.Empty<SchemaObjectMetadata>(),
        Removed = Array.Empty<SchemaObjectRef>()
    };

    [Fact]
    public async Task AnalyzeAndInvalidateAsync_WhenStoredProcedureRemoved_RemovesCacheEntry()
    {
        var console = new TestConsoleService();
        var cache = new FakeCacheManager();
        var changeDetection = new FakeChangeDetectionService();
        var procedureRef = new SchemaObjectRef(SchemaObjectType.StoredProcedure, "dbo", "CleanupArtifacts");

        changeDetection.EnqueueChangeSet(
            SchemaObjectType.StoredProcedure,
            new SchemaObjectChangeSet
            {
                Modified = Array.Empty<SchemaObjectMetadata>(),
                Removed = new[] { procedureRef }
            });

        var orchestrator = CreateOrchestrator(cache, changeDetection, console);
        await orchestrator.InitializeAsync("Server=(local);Database=Fake;", CancellationToken.None);

        var result = await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        Assert.Contains(procedureRef, cache.RemovedObjects);
        Assert.Contains(result.InvalidatedObjects, candidate => SchemaEquals(candidate, procedureRef));
        Assert.True(cache.FlushInvoked);
    }

    [Fact]
    public async Task AnalyzeAndInvalidateAsync_WhenProcedureUsesUdtt_RecordsDependency()
    {
        var console = new TestConsoleService();
        var cache = new FakeCacheManager();
        var changeDetection = new FakeChangeDetectionService();

        var procedureRef = new SchemaObjectRef(SchemaObjectType.StoredProcedure, "sales", "SyncOrders");
        var udttRef = new SchemaObjectRef(SchemaObjectType.UserDefinedTableType, "sales", "OrderTableType");

        changeDetection.EnqueueChangeSet(
            SchemaObjectType.StoredProcedure,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.StoredProcedure,
                        Schema = procedureRef.Schema,
                        Name = procedureRef.Name,
                        ModifiedUtc = DateTime.UtcNow,
                        ObjectId = 42
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });
        changeDetection.SetDependencies(procedureRef, new[] { udttRef });

        var orchestrator = CreateOrchestrator(cache, changeDetection, console);
        await orchestrator.InitializeAsync("Server=(local);Database=Fake;", CancellationToken.None);
        var result = await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        var dependencies = cache.GetDependenciesFor(procedureRef);
        Assert.Contains(dependencies, dependency => SchemaEquals(dependency, udttRef));
        Assert.Contains(result.ModifiedObjects, candidate => SchemaEquals(candidate, procedureRef));
    }

    [Fact]
    public async Task AnalyzeAndInvalidateAsync_WhenUdttModified_InvalidatesDependentProcedure()
    {
        var console = new TestConsoleService();
        var cache = new FakeCacheManager();
        var changeDetection = new FakeChangeDetectionService();

        var procedureRef = new SchemaObjectRef(SchemaObjectType.StoredProcedure, "app", "ApplyChanges");
        var udttRef = new SchemaObjectRef(SchemaObjectType.UserDefinedTableType, "app", "ChangeBatch");

        changeDetection.EnqueueChangeSet(
            SchemaObjectType.StoredProcedure,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.StoredProcedure,
                        Schema = procedureRef.Schema,
                        Name = procedureRef.Name,
                        ModifiedUtc = DateTime.UtcNow,
                        ObjectId = 101
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });
        changeDetection.SetDependencies(procedureRef, new[] { udttRef });

        changeDetection.EnqueueChangeSet(SchemaObjectType.UserDefinedTableType, EmptyChangeSet);

        var orchestrator = CreateOrchestrator(cache, changeDetection, console);
        await orchestrator.InitializeAsync("Server=(local);Database=Fake;", CancellationToken.None);
        await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        changeDetection.EnqueueChangeSet(SchemaObjectType.StoredProcedure, EmptyChangeSet);
        changeDetection.EnqueueChangeSet(
            SchemaObjectType.UserDefinedTableType,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.UserDefinedTableType,
                        Schema = udttRef.Schema,
                        Name = udttRef.Name,
                        ModifiedUtc = DateTime.UtcNow.AddMinutes(1),
                        ObjectId = 202
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });

        var secondResult = await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        Assert.DoesNotContain(secondResult.ModifiedObjects, candidate => SchemaEquals(candidate, procedureRef));
        Assert.Contains(secondResult.InvalidatedObjects, candidate => SchemaEquals(candidate, procedureRef));
    }

    [Fact]
    public async Task AnalyzeAndInvalidateAsync_WhenDependencyChainChanges_InvalidatesTransitiveDependents()
    {
        var console = new TestConsoleService();
        var cache = new FakeCacheManager();
        var changeDetection = new FakeChangeDetectionService();

        var procedureRef = new SchemaObjectRef(SchemaObjectType.StoredProcedure, "sales", "RecalculateTotals");
        var viewRef = new SchemaObjectRef(SchemaObjectType.View, "sales", "vw_OrderTotals");
        var udttRef = new SchemaObjectRef(SchemaObjectType.UserDefinedTableType, "sales", "OrderItemTableType");

        // Prime the dependency graph so the cache records both direct and transitive relationships.
        changeDetection.EnqueueChangeSet(
            SchemaObjectType.View,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.View,
                        Schema = viewRef.Schema,
                        Name = viewRef.Name,
                        ModifiedUtc = DateTime.UtcNow,
                        ObjectId = 303
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });

        changeDetection.EnqueueChangeSet(
            SchemaObjectType.StoredProcedure,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.StoredProcedure,
                        Schema = procedureRef.Schema,
                        Name = procedureRef.Name,
                        ModifiedUtc = DateTime.UtcNow.AddSeconds(1),
                        ObjectId = 404
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });

        changeDetection.SetDependencies(viewRef, new[] { udttRef });
        changeDetection.SetDependencies(procedureRef, new[] { viewRef });

        var orchestrator = CreateOrchestrator(cache, changeDetection, console);
        await orchestrator.InitializeAsync("Server=(local);Database=Fake;", CancellationToken.None);
        await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        // Second pass: only the UDTT changes, but we expect the view and procedure to invalidate via the dependency chain.
        changeDetection.EnqueueChangeSet(SchemaObjectType.StoredProcedure, EmptyChangeSet);
        changeDetection.EnqueueChangeSet(SchemaObjectType.View, EmptyChangeSet);
        changeDetection.EnqueueChangeSet(
            SchemaObjectType.UserDefinedTableType,
            new SchemaObjectChangeSet
            {
                Modified = new[]
                {
                    new SchemaObjectMetadata
                    {
                        Type = SchemaObjectType.UserDefinedTableType,
                        Schema = udttRef.Schema,
                        Name = udttRef.Name,
                        ModifiedUtc = DateTime.UtcNow.AddMinutes(1),
                        ObjectId = 505
                    }
                },
                Removed = Array.Empty<SchemaObjectRef>()
            });

        var secondResult = await orchestrator.AnalyzeAndInvalidateAsync(cancellationToken: CancellationToken.None);

        Assert.Contains(secondResult.ModifiedObjects, candidate => SchemaEquals(candidate, udttRef));
        Assert.Contains(secondResult.InvalidatedObjects, candidate => SchemaEquals(candidate, viewRef));
        Assert.Contains(secondResult.InvalidatedObjects, candidate => SchemaEquals(candidate, procedureRef));
    }

    private static bool SchemaEquals(SchemaObjectRef? candidate, SchemaObjectRef expected)
    {
        return candidate is not null
            && candidate.Type == expected.Type
            && string.Equals(candidate.Schema, expected.Schema, StringComparison.OrdinalIgnoreCase)
            && string.Equals(candidate.Name, expected.Name, StringComparison.OrdinalIgnoreCase);
    }

    private static SchemaInvalidationOrchestrator CreateOrchestrator(
        FakeCacheManager cache,
        FakeChangeDetectionService changeDetection,
        IConsoleService console)
    {
        return new SchemaInvalidationOrchestrator(cache, changeDetection, console);
    }

    private sealed class FakeCacheManager : ISchemaObjectCacheManager
    {
        private readonly Dictionary<string, SchemaObjectCacheEntry> _entries = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _dependencyGraph = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, IReadOnlyList<SchemaObjectRef>> _recordedDependencies = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<SchemaObjectRef> _removed = new();
        private DateTime? _referenceTimestamp;

        public IReadOnlyList<SchemaObjectRef> RemovedObjects => _removed;
        public bool FlushInvoked { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public DateTime? GetLastModified(SchemaObjectType objectType, string schema, string name)
        {
            var key = BuildKey(objectType, schema, name);
            return _entries.TryGetValue(key, out var entry) ? entry.LastModifiedUtc : null;
        }

        public DateTime? GetLastUpdatedUtc() => _referenceTimestamp;

        public Task UpdateReferenceTimestampAsync(DateTime referenceTimestampUtc)
        {
            if (!_referenceTimestamp.HasValue || _referenceTimestamp < referenceTimestampUtc)
            {
                _referenceTimestamp = referenceTimestampUtc;
            }

            return Task.CompletedTask;
        }

        public Task UpdateLastModifiedAsync(SchemaObjectType objectType, string schema, string name, DateTime lastModifiedUtc)
        {
            var key = BuildKey(objectType, schema, name);
            if (!_entries.TryGetValue(key, out var entry))
            {
                entry = new SchemaObjectCacheEntry
                {
                    Type = objectType,
                    Schema = schema,
                    Name = name
                };
            }

            entry.LastModifiedUtc = lastModifiedUtc;
            _entries[key] = entry;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<SchemaObjectRef>> GetModifiedSinceAsync(SchemaObjectType objectType, DateTime sinceUtc)
        {
            return Task.FromResult<IReadOnlyList<SchemaObjectRef>>(Array.Empty<SchemaObjectRef>());
        }

        public Task InvalidateDependentsAsync(SchemaObjectRef changedObject)
        {
            foreach (var dependentKey in TraverseDependents(BuildKey(changedObject)))
            {
                if (_entries.TryGetValue(dependentKey, out var entry))
                {
                    entry.LastModifiedUtc = DateTime.MinValue;
                }
            }

            return Task.CompletedTask;
        }

        public IReadOnlyList<SchemaObjectRef> GetDependents(SchemaObjectRef dependency)
        {
            var key = BuildKey(dependency);
            if (!_dependencyGraph.TryGetValue(key, out var dependents) || dependents.Count == 0)
            {
                return Array.Empty<SchemaObjectRef>();
            }

            return dependents.Select(ParseKey).Where(static item => item is not null).Cast<SchemaObjectRef>().ToArray();
        }

        public Task RecordDependencyAsync(SchemaObjectRef dependent, SchemaObjectRef dependency)
        {
            return SetDependenciesAsync(dependent, new[] { dependency });
        }

        public Task SetDependenciesAsync(SchemaObjectRef dependent, IReadOnlyList<SchemaObjectRef> dependencies)
        {
            RemoveExistingEdgesFor(dependent);

            var key = BuildKey(dependent);
            if (!_entries.TryGetValue(key, out var entry))
            {
                entry = new SchemaObjectCacheEntry
                {
                    Type = dependent.Type,
                    Schema = dependent.Schema,
                    Name = dependent.Name
                };
            }

            entry.Dependencies = dependencies?.ToList() ?? new List<SchemaObjectRef>();
            _entries[key] = entry;
            _recordedDependencies[key] = entry.Dependencies;

            if (dependencies != null)
            {
                foreach (var dependency in dependencies)
                {
                    if (dependency is null)
                    {
                        continue;
                    }

                    var dependencyKey = BuildKey(dependency);
                    if (!_dependencyGraph.TryGetValue(dependencyKey, out var dependents))
                    {
                        dependents = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        _dependencyGraph[dependencyKey] = dependents;
                    }

                    dependents.Add(key);
                }
            }

            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            FlushInvoked = true;
            return Task.CompletedTask;
        }

        public Task ClearAllAsync()
        {
            _entries.Clear();
            _dependencyGraph.Clear();
            _recordedDependencies.Clear();
            _removed.Clear();
            _referenceTimestamp = null;
            return Task.CompletedTask;
        }

        public Task RemoveAsync(SchemaObjectRef objectRef, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _removed.Add(objectRef);
            var key = BuildKey(objectRef);
            _entries.Remove(key);
            _dependencyGraph.Remove(key);
            foreach (var dependents in _dependencyGraph.Values)
            {
                dependents.Remove(key);
            }

            return Task.CompletedTask;
        }

        public IReadOnlyList<SchemaObjectRef> GetDependenciesFor(SchemaObjectRef dependent)
        {
            var key = BuildKey(dependent);
            return _recordedDependencies.TryGetValue(key, out var dependencies)
                ? dependencies
                : Array.Empty<SchemaObjectRef>();
        }

        private static string BuildKey(SchemaObjectRef reference)
        {
            return BuildKey(reference.Type, reference.Schema, reference.Name);
        }

        private static string BuildKey(SchemaObjectType type, string schema, string name)
        {
            return $"{type}:{schema}.{name}";
        }

        private static SchemaObjectRef? ParseKey(string key)
        {
            var separatorIndex = key.IndexOf(':');
            if (separatorIndex < 0 || separatorIndex == key.Length - 1)
            {
                return null;
            }

            var typeSegment = key[..separatorIndex];
            if (!Enum.TryParse(typeSegment, out SchemaObjectType type))
            {
                return null;
            }

            var identifier = key[(separatorIndex + 1)..];
            var nameSeparator = identifier.IndexOf('.');
            if (nameSeparator <= 0 || nameSeparator == identifier.Length - 1)
            {
                return null;
            }

            var schema = identifier[..nameSeparator];
            var name = identifier[(nameSeparator + 1)..];
            return new SchemaObjectRef(type, schema, name);
        }

        private IEnumerable<string> TraverseDependents(string startKey)
        {
            if (!_dependencyGraph.TryGetValue(startKey, out var initial) || initial.Count == 0)
            {
                yield break;
            }

            var queue = new Queue<string>(initial);
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                if (!visited.Add(current))
                {
                    continue;
                }

                yield return current;

                if (_dependencyGraph.TryGetValue(current, out var next) && next.Count > 0)
                {
                    foreach (var candidate in next)
                    {
                        if (!visited.Contains(candidate))
                        {
                            queue.Enqueue(candidate);
                        }
                    }
                }
            }
        }

        private void RemoveExistingEdgesFor(SchemaObjectRef dependent)
        {
            var key = BuildKey(dependent);
            _recordedDependencies.Remove(key);
            foreach (var dependents in _dependencyGraph.Values)
            {
                dependents.Remove(key);
            }
        }
    }

    private sealed class FakeChangeDetectionService : ISchemaChangeDetectionService
    {
        private readonly Dictionary<SchemaObjectType, Queue<SchemaObjectChangeSet>> _changeSets = new();
        private readonly Dictionary<SchemaObjectRef, IReadOnlyList<SchemaObjectRef>> _dependencyMap = new(new SchemaObjectRefComparer());
        private DateTime _timestamp = DateTime.UtcNow;

        public void Initialize(string connectionString)
        {
            // Connection string is not required for the fake implementation.
        }

        public void EnqueueChangeSet(SchemaObjectType objectType, SchemaObjectChangeSet changeSet)
        {
            if (!_changeSets.TryGetValue(objectType, out var queue))
            {
                queue = new Queue<SchemaObjectChangeSet>();
                _changeSets[objectType] = queue;
            }

            queue.Enqueue(changeSet);
        }

        public void SetDependencies(SchemaObjectRef source, IReadOnlyList<SchemaObjectRef> dependencies)
        {
            _dependencyMap[source] = dependencies;
        }

        public Task<SchemaObjectChangeSet> GetObjectChangesAsync(
            SchemaObjectType objectType,
            DateTime? sinceUtc = null,
            IReadOnlyList<string>? schemaFilter = null,
            bool allowFullScanFallback = true,
            CancellationToken cancellationToken = default)
        {
            if (_changeSets.TryGetValue(objectType, out var queue) && queue.Count > 0)
            {
                return Task.FromResult(queue.Dequeue());
            }

            return Task.FromResult(EmptyChangeSet);
        }

        public Task<IReadOnlyList<SchemaObjectRef>> GetDependenciesAsync(
            SchemaObjectRef objectRef,
            CancellationToken cancellationToken = default)
        {
            if (_dependencyMap.TryGetValue(objectRef, out var dependencies))
            {
                return Task.FromResult(dependencies);
            }

            return Task.FromResult<IReadOnlyList<SchemaObjectRef>>(Array.Empty<SchemaObjectRef>());
        }

        public Task<DateTime> GetMaxModificationTimeAsync(CancellationToken cancellationToken = default)
        {
            _timestamp = _timestamp.AddSeconds(1);
            return Task.FromResult(_timestamp);
        }

        public Task FlushIndexAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        private sealed class SchemaObjectRefComparer : IEqualityComparer<SchemaObjectRef>
        {
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
                return HashCode.Combine(
                    obj.Type,
                    StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Schema),
                    StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Name));
            }
        }
    }
}
