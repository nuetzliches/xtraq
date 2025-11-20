using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Writers;

/// <summary>
/// Captures the schema artifacts that must be emitted for a snapshot based on procedure dependencies.
/// </summary>
internal sealed class SchemaDependencyFilter
{
    private readonly HashSet<SchemaObjectRef> _tables = new(SchemaObjectRefComparer.Instance);
    private readonly HashSet<SchemaObjectRef> _tableTypes = new(SchemaObjectRefComparer.Instance);
    private readonly HashSet<SchemaObjectRef> _functions = new(SchemaObjectRefComparer.Instance);
    private readonly HashSet<string> _schemas = new(StringComparer.OrdinalIgnoreCase);

    private SchemaDependencyFilter()
    {
    }

    public static SchemaDependencyFilter Build(
        IReadOnlyList<ProcedureAnalysisResult> procedures,
        ISet<string>? requiredTypeRefs,
        ISet<string>? requiredTableRefs)
    {
        var filter = new SchemaDependencyFilter();
        if (procedures != null && procedures.Count > 0)
        {
            var lookup = BuildProcedureLookup(procedures);
            var visiting = new HashSet<ProcedureKey>(ProcedureKeyComparer.Instance);
            foreach (var procedure in procedures)
            {
                if (procedure == null)
                {
                    continue;
                }

                var descriptorKey = ProcedureKey.FromDescriptor(procedure.Descriptor);
                if (!descriptorKey.HasValue)
                {
                    continue;
                }

                filter.RegisterSchema(descriptorKey.Value.Schema);
                CollectDependencies(procedure, lookup, visiting, filter);
            }
        }

        filter.AddTableRefs(requiredTableRefs);
        filter.AddTypeRefs(requiredTypeRefs);
        return filter;
    }

    public bool ShouldEmitTable(string? catalog, string? schema, string? name)
        => Contains(_tables, catalog, schema, name);

    public bool ShouldEmitTableType(string? catalog, string? schema, string? name)
        => Contains(_tableTypes, catalog, schema, name);

    public bool ShouldEmitFunction(string? schema, string? name)
        => Contains(_functions, null, schema, name);

    public IReadOnlyCollection<string> Schemas => _schemas;

    private void RegisterDependency(ProcedureDependency dependency)
    {
        if (dependency == null)
        {
            return;
        }

        RegisterSchema(dependency.Schema);

        switch (dependency.Kind)
        {
            case ProcedureDependencyKind.Table:
                TryAdd(_tables, dependency.Catalog, dependency.Schema, dependency.Name);
                break;
            case ProcedureDependencyKind.Function:
                TryAdd(_functions, null, dependency.Schema, dependency.Name);
                break;
            case ProcedureDependencyKind.UserDefinedTableType:
                TryAdd(_tableTypes, null, dependency.Schema, dependency.Name);
                break;
            case ProcedureDependencyKind.UserDefinedType:
            case ProcedureDependencyKind.View:
            case ProcedureDependencyKind.Procedure:
            case ProcedureDependencyKind.Unknown:
                break;
        }
    }

    private void RegisterSchema(string? schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
        {
            return;
        }

        var clean = schema.Trim();
        if (clean.Length > 0)
        {
            _schemas.Add(clean);
        }
    }

    private void AddTableRefs(ISet<string>? tableRefs)
    {
        if (tableRefs == null || tableRefs.Count == 0)
        {
            return;
        }

        foreach (var tableRef in tableRefs)
        {
            if (string.IsNullOrWhiteSpace(tableRef))
            {
                continue;
            }

            var parts = SnapshotWriterUtilities.SplitTableRefParts(tableRef);
            TryAdd(_tables, parts.Catalog, parts.Schema, parts.Name);
            RegisterSchema(parts.Schema);
        }
    }

    private void AddTypeRefs(ISet<string>? typeRefs)
    {
        if (typeRefs == null || typeRefs.Count == 0)
        {
            return;
        }

        foreach (var typeRef in typeRefs)
        {
            if (string.IsNullOrWhiteSpace(typeRef))
            {
                continue;
            }

            var parts = SnapshotWriterUtilities.SplitTypeRefParts(typeRef);
            RegisterSchema(parts.Schema);
        }
    }

    private static void CollectDependencies(
        ProcedureAnalysisResult procedure,
        IReadOnlyDictionary<ProcedureKey, ProcedureAnalysisResult> lookup,
        HashSet<ProcedureKey> visiting,
        SchemaDependencyFilter filter)
    {
        if (procedure == null)
        {
            return;
        }

        var descriptorKey = ProcedureKey.FromDescriptor(procedure.Descriptor);
        if (!descriptorKey.HasValue)
        {
            return;
        }

        if (!visiting.Add(descriptorKey.Value))
        {
            return;
        }

        if (procedure.Dependencies != null)
        {
            foreach (var dependency in procedure.Dependencies)
            {
                if (dependency == null)
                {
                    continue;
                }

                filter.RegisterDependency(dependency);

                if (dependency.Kind != ProcedureDependencyKind.Procedure)
                {
                    continue;
                }

                var dependencyKey = ProcedureKey.FromDependency(dependency);
                if (!dependencyKey.HasValue)
                {
                    continue;
                }

                if (lookup.TryGetValue(dependencyKey.Value, out var nested))
                {
                    CollectDependencies(nested, lookup, visiting, filter);
                }
            }
        }

        visiting.Remove(descriptorKey.Value);
    }

    private static IReadOnlyDictionary<ProcedureKey, ProcedureAnalysisResult> BuildProcedureLookup(IReadOnlyList<ProcedureAnalysisResult> procedures)
    {
        var map = new Dictionary<ProcedureKey, ProcedureAnalysisResult>(ProcedureKeyComparer.Instance);
        if (procedures == null)
        {
            return map;
        }

        foreach (var procedure in procedures)
        {
            var key = ProcedureKey.FromDescriptor(procedure?.Descriptor);
            if (!key.HasValue)
            {
                continue;
            }

            map[key.Value] = procedure!;
        }

        return map;
    }

    private static bool Contains(HashSet<SchemaObjectRef> set, string? catalog, string? schema, string? name)
    {
        if (set == null || set.Count == 0)
        {
            return false;
        }

        if (!TryCreateRef(catalog, schema, name, out var reference))
        {
            return false;
        }

        return set.Contains(reference);
    }

    private static void TryAdd(HashSet<SchemaObjectRef> set, string? catalog, string? schema, string? name)
    {
        if (set == null)
        {
            return;
        }

        if (!TryCreateRef(catalog, schema, name, out var reference))
        {
            return;
        }

        set.Add(reference);
    }

    private static bool TryCreateRef(string? catalog, string? schema, string? name, out SchemaObjectRef reference)
    {
        reference = default;
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        var cleanSchema = schema.Trim();
        var cleanName = name.Trim();
        if (cleanSchema.Length == 0 || cleanName.Length == 0)
        {
            return false;
        }

        var cleanCatalog = string.IsNullOrWhiteSpace(catalog) ? null : catalog.Trim();
        reference = new SchemaObjectRef(cleanCatalog, cleanSchema, cleanName);
        return true;
    }

    private readonly record struct SchemaObjectRef(string? Catalog, string Schema, string Name);

    private sealed class SchemaObjectRefComparer : IEqualityComparer<SchemaObjectRef>
    {
        internal static SchemaObjectRefComparer Instance { get; } = new();

        public bool Equals(SchemaObjectRef x, SchemaObjectRef y)
        {
            return string.Equals(x.Catalog, y.Catalog, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(SchemaObjectRef obj)
        {
            var catalogHash = obj.Catalog?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            var schemaHash = obj.Schema?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            var nameHash = obj.Name?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            return HashCode.Combine(catalogHash, schemaHash, nameHash);
        }
    }

    private readonly record struct ProcedureKey(string? Catalog, string Schema, string Name)
    {
        public static ProcedureKey? FromDescriptor(ProcedureDescriptor? descriptor)
        {
            if (descriptor == null)
            {
                return null;
            }

            var schema = descriptor.Schema?.Trim();
            var name = descriptor.Name?.Trim();
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var catalog = string.IsNullOrWhiteSpace(descriptor.Catalog) ? null : descriptor.Catalog!.Trim();
            return new ProcedureKey(catalog, schema, name);
        }

        public static ProcedureKey? FromDependency(ProcedureDependency dependency)
        {
            if (dependency == null)
            {
                return null;
            }

            var schema = dependency.Schema?.Trim();
            var name = dependency.Name?.Trim();
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var catalog = string.IsNullOrWhiteSpace(dependency.Catalog) ? null : dependency.Catalog!.Trim();
            return new ProcedureKey(catalog, schema, name);
        }
    }

    private sealed class ProcedureKeyComparer : IEqualityComparer<ProcedureKey>
    {
        internal static ProcedureKeyComparer Instance { get; } = new();

        public bool Equals(ProcedureKey x, ProcedureKey y)
        {
            return string.Equals(x.Catalog, y.Catalog, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(ProcedureKey obj)
        {
            var catalogHash = obj.Catalog?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            var schemaHash = obj.Schema?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            var nameHash = obj.Name?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            return HashCode.Combine(catalogHash, schemaHash, nameHash);
        }
    }
}
