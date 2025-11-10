namespace Xtraq.Services;

/// <summary>
/// Post-Snapshot central column enrichment pipeline (phase after table/view loading).
/// Goal: Supplements missing SqlTypeName / Nullability in function JSON columns based on table metadata.
/// Extensible for views / future ResultSet types.
/// </summary>
internal sealed class ColumnEnrichmentService
{
    internal void EnrichFunctions(SchemaSnapshot snapshot, IConsoleService console)
    {
        if (snapshot?.Functions == null || snapshot.Functions.Count == 0) return;
        // Build table lookup data when tables are available
        var tableLookup = new Dictionary<string, Dictionary<string, (string TypeRef, bool? IsNullable, int? MaxLength)>>(StringComparer.OrdinalIgnoreCase);
        if (snapshot.Tables != null)
        {
            foreach (var t in snapshot.Tables)
            {
                var key = t.Schema + "." + t.Name;
                var colMap = new Dictionary<string, (string, bool?, int?)>(StringComparer.OrdinalIgnoreCase);
                foreach (var c in t.Columns ?? new List<SnapshotTableColumn>())
                {
                    if (!string.IsNullOrWhiteSpace(c.Name) && !string.IsNullOrWhiteSpace(c.TypeRef))
                        colMap[c.Name] = (c.TypeRef!, c.IsNullable, c.MaxLength);
                }
                tableLookup[key] = colMap;
            }
        }
        int enriched = 0;
        foreach (var f in snapshot.Functions.Where(fn => fn.ReturnsJson == true && fn.Columns != null && fn.Columns.Count > 0))
        {
            foreach (var col in f.Columns!)
            {
                EnrichRecursive(f, col, tableLookup, ref enriched);
            }
        }
        console.Verbose($"[fn-enrich-post] enrichedColumns={enriched}");
    }

    private static void EnrichRecursive(SnapshotFunction fn, SnapshotFunctionColumn col,
        Dictionary<string, Dictionary<string, (string SqlType, bool? IsNullable, int? MaxLength)>> tableLookup,
        ref int enriched)
    {
        // Skip when a concrete type is already present (not the JSON container placeholder)
        if (!string.IsNullOrWhiteSpace(col.TypeRef))
        {
            if (col.Columns != null) foreach (var child in col.Columns) EnrichRecursive(fn, child, tableLookup, ref enriched);
            return;
        }
        var leaf = (col.Name?.Split('.', StringSplitOptions.RemoveEmptyEntries).LastOrDefault()) ?? col.Name;
        if (string.IsNullOrWhiteSpace(leaf))
        {
            if (col.Columns != null) foreach (var child in col.Columns) EnrichRecursive(fn, child, tableLookup, ref enriched);
            return;
        }
        // Targeted mappings: displayName, initials, userId, rowVersion
        TryMap("identity.User", leaf, col, tableLookup, ref enriched);
        if (leaf.Equals("displayName", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(col.TypeRef)) TryMap("identity.User", "UserName", col, tableLookup, ref enriched); // fallback lookup
        }
        else if (leaf.Equals("rowVersion", StringComparison.OrdinalIgnoreCase))
        {
            // Special case for rowVersion: fall back to a stable type when no mapping exists
            if (string.IsNullOrWhiteSpace(col.TypeRef)) { col.TypeRef = CombineTypeRef("sys", "rowversion"); enriched++; }
        }
        if (col.Columns != null) foreach (var child in col.Columns) EnrichRecursive(fn, child, tableLookup, ref enriched);
    }

    private static void TryMap(string tableKey, string columnName, SnapshotFunctionColumn target,
        Dictionary<string, Dictionary<string, (string TypeRef, bool? IsNullable, int? MaxLength)>> tableLookup,
        ref int enriched)
    {
        if (string.IsNullOrWhiteSpace(columnName))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(target.TypeRef) &&
            tableLookup.TryGetValue(tableKey, out var cols) &&
            cols.TryGetValue(columnName, out var meta))
        {
            target.TypeRef = meta.TypeRef;
            if (!target.IsNullable.HasValue) target.IsNullable = meta.IsNullable;
            if (!target.MaxLength.HasValue) target.MaxLength = meta.MaxLength;
            enriched++;
        }
    }

    private static string CombineTypeRef(string schema, string name)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name)) return string.Empty;
        return string.Concat(schema.Trim(), ".", name.Trim());
    }
}

