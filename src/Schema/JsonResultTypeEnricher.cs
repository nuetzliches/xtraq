using Xtraq.Data.Models;
using Xtraq.Metadata;
using Xtraq.Models;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Schema;

/// <summary>
/// Aggregated statistics for JSON result type enrichment across all procedures in a snapshot run.
/// </summary>
internal sealed class JsonTypeEnrichmentStats
{
    public long ResolvedColumns { get; private set; }
    public long NewConcrete { get; private set; }
    public long Upgrades { get; private set; }
    public void Accumulate(int resolved, int newlyConcrete, int upgrades)
    {
        ResolvedColumns += resolved;
        NewConcrete += newlyConcrete;
        Upgrades += upgrades;
    }
}

/// <summary>
/// Stage 2 JSON column type enrichment. Resolves unresolved JSON result columns to concrete SQL types
/// by inspecting base table metadata from snapshots. Also performs fallback -> concrete upgrades (parser v4).
/// </summary>
internal sealed class JsonResultTypeEnricher
{
    private readonly IConsoleService _console;
    private readonly TableMetadataProvider? _tableMetadataProvider;
    public JsonResultTypeEnricher(IConsoleService console)
    {
        _console = console;
        try
        {
            _tableMetadataProvider = new TableMetadataProvider(DirectoryUtils.GetWorkingDirectory());
        }
        catch
        {
            _tableMetadataProvider = null;
        }
    }

    public async Task EnrichAsync(StoredProcedureModel sp, bool verbose, JsonTypeLogLevel level, JsonTypeEnrichmentStats stats, System.Threading.CancellationToken ct)
    {
        if (sp?.Content?.ResultSets == null || sp.Content.ResultSets.Count == 0)
        {
            return;
        }

        var content = sp.Content;
        var sets = content.ResultSets;
        var jsonSets = sets.Where(s => s.ReturnsJson && s.Columns != null && s.Columns.Count > 0).ToList();
        if (jsonSets.Count == 0) return;
        var tableCache = new Dictionary<string, List<Column>>(StringComparer.OrdinalIgnoreCase);
        var anyModified = false;
        var newSetsStage2 = new List<StoredProcedureContentModel.ResultSet>();
        var loggedColumnResolutions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int loggedUpgrades = 0;
        int loggedNewConcrete = 0;

        // Recursive processor for a single column (and its nested columns)
        async Task<(StoredProcedureContentModel.ResultColumn Column, bool Modified)> ProcessColumnAsync(StoredProcedureContentModel.ResultColumn col, bool parentReturnsJson, StoredProcedureContentModel.ResultSet owningSet, System.Threading.CancellationToken token)
        {
            if (col == null) return (new StoredProcedureContentModel.ResultColumn(), false);
            bool isJsonContext = parentReturnsJson || col.ReturnsJson == true; // treat nested container as JSON context for fallback typing
            var sourceSchema = col.SourceSchema;
            var sourceTable = col.SourceTable;
            var sourceColumn = col.SourceColumn;
            bool hasSourceBinding = !string.IsNullOrWhiteSpace(sourceSchema) && !string.IsNullOrWhiteSpace(sourceTable) && !string.IsNullOrWhiteSpace(sourceColumn);
            if (hasSourceBinding) loggedColumnResolutions.Add($"__bind_only__{Guid.NewGuid():N}"); // binding progress
            bool modifiedLocal = false;

            // Removed UDTT 'record' mapping heuristic: Only AST-bound table metadata allowed. Leaving UserType* empty unless future AST capture implemented.
            // JSON payloads without explicit typing default to nvarchar(max) and nullable FOR JSON outputs obey this contract
            // JSON_QUERY heuristic
            // No fallback to nvarchar: missing type information must remain null in snapshot
            if (col.ExpressionKind == StoredProcedureContentModel.ResultColumnExpressionKind.JsonQuery)
            {
                if (col.IsNullable == null)
                {
                    col.IsNullable = true;
                    modifiedLocal = true;
                }
                if (string.IsNullOrWhiteSpace(col.SqlTypeName))
                {
                    col.SqlTypeName = "nvarchar(max)"; // FOR JSON payload is nvarchar(max) by contract
                    modifiedLocal = true;
                }
            }
            else if (col.ReturnsJson == true && (col.Columns == null || col.Columns.Count == 0))
            {
                if (col.IsNullable == null)
                {
                    col.IsNullable = true;
                    modifiedLocal = true;
                }
                if (string.IsNullOrWhiteSpace(col.SqlTypeName))
                {
                    col.SqlTypeName = "nvarchar(max)";
                    modifiedLocal = true;
                }
            }
            // Forced nullable adjustment (outer join heuristic)
            if (col.ForcedNullable == true && (col.IsNullable == false || col.IsNullable == null))
            {
                col.IsNullable = true; modifiedLocal = true;
                if (verbose && level == JsonTypeLogLevel.Detailed)
                    _console.Verbose($"[json-type-nullable-adjust] {sp.SchemaName}.{sp.Name} {col.Name} forced nullable (outer join)");
            }
            // CAST / CONVERT target typing
            if (col.ExpressionKind == StoredProcedureContentModel.ResultColumnExpressionKind.Cast && string.IsNullOrWhiteSpace(col.SqlTypeName) && !string.IsNullOrWhiteSpace(col.CastTargetType))
            {
                col.SqlTypeName = col.CastTargetType; modifiedLocal = true;
                if (verbose && level == JsonTypeLogLevel.Detailed)
                    _console.Verbose($"[json-type-cast] {sp.SchemaName}.{sp.Name} {col.Name} -> {col.SqlTypeName}");
            }

            bool hadFallback = (string.Equals(col.SqlTypeName, "nvarchar(max)", StringComparison.OrdinalIgnoreCase) || string.Equals(col.SqlTypeName, "unknown", StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(col.SqlTypeName)) && isJsonContext;
            bool hasConcrete = !string.IsNullOrWhiteSpace(col.SqlTypeName) && !hadFallback;
            if (hasSourceBinding && col.ExpressionKind != StoredProcedureContentModel.ResultColumnExpressionKind.Cast)
            {
                var bindingSchema = sourceSchema!;
                var bindingTable = sourceTable!;
                var tblKey = ($"{bindingSchema}.{bindingTable}");
                if (!tableCache.TryGetValue(tblKey, out var tblColumns))
                {
                    var loaded = new List<Column>();
                    if (_tableMetadataProvider != null)
                    {
                        var snapshot = _tableMetadataProvider.TryGet(bindingSchema, bindingTable);
                        if (snapshot?.Columns != null && snapshot.Columns.Count > 0)
                        {
                            loaded = snapshot.Columns.Select(c => new Column
                            {
                                Name = c.Name,
                                SqlTypeName = c.SqlType ?? string.Empty,
                                IsNullable = c.IsNullable,
                                MaxLength = c.MaxLength ?? 0,
                                BaseSqlTypeName = null,
                                Precision = null,
                                Scale = null,
                                UserTypeName = null,
                                UserTypeSchemaName = null,
                                IsIdentityRaw = null
                            }).ToList();
                        }
                    }
                    tblColumns = loaded;
                    tableCache[tblKey] = tblColumns;
                }
                // For dot aliases (e.g. type.typeId) SourceColumn might try the full alias fallback: last segment against table columns
                var match = tblColumns.FirstOrDefault(c => c.Name.Equals(sourceColumn!, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                {
                    modifiedLocal = true;
                    var logTag = hadFallback ? "[json-type-upgrade]" : "[json-type-table]";
                    var logKey = $"{sp.SchemaName}.{sp.Name}|{col.Name}->{match.SqlTypeName}";
                    if (verbose && level == JsonTypeLogLevel.Detailed && !loggedColumnResolutions.Contains(logKey))
                    {
                        loggedColumnResolutions.Add(logKey);
                        if (hadFallback) loggedUpgrades++; else loggedNewConcrete++;
                        _console.Verbose($"{logTag} {sp.SchemaName}.{sp.Name} {col.Name} -> {match.SqlTypeName}");
                    }
                    col = new StoredProcedureContentModel.ResultColumn
                    {
                        Name = col.Name,
                        SourceSchema = col.SourceSchema,
                        SourceTable = col.SourceTable,
                        SourceColumn = col.SourceColumn,
                        SqlTypeName = match.SqlTypeName,
                        IsNullable = match.IsNullable,
                        MaxLength = match.MaxLength,
                        SourceAlias = col.SourceAlias,
                        ExpressionKind = col.ExpressionKind,
                        IsNestedJson = col.IsNestedJson,
                        ReturnsJson = col.ReturnsJson,
                        ReturnsJsonArray = col.ReturnsJsonArray,
                        // removed flag
                        JsonRootProperty = col.JsonRootProperty,
                        Columns = col.Columns, // will be processed below if present
                        ForcedNullable = col.ForcedNullable,
                        IsAmbiguous = col.IsAmbiguous,
                        UserTypeSchemaName = col.UserTypeSchemaName,
                        UserTypeName = col.UserTypeName
                    };
                }
                else if (!hasConcrete)
                {
                    // AST-only: signal unresolved type for JSON column; no name-based fallback
                    var detail = $"[json-type-miss] {sp.SchemaName}.{sp.Name} {col.Name} source={bindingSchema}.{bindingTable}.{sourceColumn} no-column-match";
                    _console.WarnBuffered("[json-type-miss] unresolved JSON column (no column match)", detail);
                }
            }
            // No name-based targeted fallbacks (AST-only policy)
            // Common heuristics apply if still no concrete type determined (no binding or binding without match)
            if (!hasConcrete)
            {
                // Adapter for inference (uses Name + ExpressionKind -> SourceSql currently not available in enricher)
                var pseudo = new JsonFunctionAstColumn
                {
                    Name = col.Name,
                    IsNestedJson = col.IsNestedJson == true,
                    ReturnsJson = col.ReturnsJson == true,
                    ReturnsJsonArray = col.ReturnsJsonArray
                };
                // Heuristic removed: no name-based type guessing. Unbound or uncast columns keep an empty type.
            }

            // Emit generic warning for unresolved leaf columns (visible even without binding)
            if (!hasConcrete)
            {
                bool isLeaf = col.Columns == null || col.Columns.Count == 0;
                bool isContainer = col.ReturnsJson == true || col.IsNestedJson == true;
                if (isLeaf && !isContainer)
                {
                    var src = (string.IsNullOrWhiteSpace(sourceSchema) && string.IsNullOrWhiteSpace(sourceTable) && string.IsNullOrWhiteSpace(sourceColumn))
                        ? "?"
                        : $"{sourceSchema}.{sourceTable}.{sourceColumn}";
                    var detail = $"[json-type-miss] {sp.SchemaName}.{sp.Name} {col.Name} source={src} unresolved";
                    _console.WarnBuffered("[json-type-miss] unresolved JSON column", detail);
                }
            }

            // Recurse into nested JSON columns
            if ((col.IsNestedJson == true || col.ReturnsJson == true) && col.Columns != null && col.Columns.Count > 0)
            {
                var nestedModifiedAny = false;
                var newNested = new List<StoredProcedureContentModel.ResultColumn>();
                foreach (var nc in col.Columns)
                {
                    var processed = await ProcessColumnAsync(nc, col.ReturnsJson == true, owningSet, token);
                    if (processed.Modified) nestedModifiedAny = true;
                    newNested.Add(processed.Column);
                }
                if (nestedModifiedAny)
                {
                    // ensure we carry updated nested list
                    col = new StoredProcedureContentModel.ResultColumn
                    {
                        Name = col.Name,
                        SourceSchema = col.SourceSchema,
                        SourceTable = col.SourceTable,
                        SourceColumn = col.SourceColumn,
                        SqlTypeName = col.SqlTypeName,
                        IsNullable = col.IsNullable,
                        MaxLength = col.MaxLength,
                        SourceAlias = col.SourceAlias,
                        ExpressionKind = col.ExpressionKind,
                        IsNestedJson = col.IsNestedJson,
                        ReturnsJson = col.ReturnsJson,
                        ReturnsJsonArray = col.ReturnsJsonArray,
                        // removed flag
                        JsonRootProperty = col.JsonRootProperty,
                        Columns = newNested,
                        ForcedNullable = col.ForcedNullable,
                        IsAmbiguous = col.IsAmbiguous,
                        UserTypeSchemaName = col.UserTypeSchemaName,
                        UserTypeName = col.UserTypeName
                    };
                    modifiedLocal = true;
                }
            }
            return (col, modifiedLocal);
        }

        foreach (var set in sets)
        {
            if (!set.ReturnsJson || set.Columns == null || set.Columns.Count == 0)
            { newSetsStage2.Add(set); continue; }
            var newCols = new List<StoredProcedureContentModel.ResultColumn>();
            var modifiedLocal = false;
            foreach (var col in set.Columns)
            {
                var processed = await ProcessColumnAsync(col, set.ReturnsJson, set, ct);
                if (processed.Modified) modifiedLocal = true;
                newCols.Add(processed.Column);
            }
            if (modifiedLocal)
            {
                anyModified = true;
                newSetsStage2.Add(new StoredProcedureContentModel.ResultSet
                {
                    ReturnsJson = set.ReturnsJson,
                    ReturnsJsonArray = set.ReturnsJsonArray,
                    // removed flag
                    JsonRootProperty = set.JsonRootProperty,
                    Columns = newCols.ToArray()
                });
            }
            else { newSetsStage2.Add(set); }
        }
        if (anyModified)
        {
            sp.Content = new StoredProcedureContentModel
            {
                Definition = content.Definition,
                Statements = content.Statements ?? Array.Empty<string>(),
                ContainsSelect = content.ContainsSelect,
                ContainsInsert = content.ContainsInsert,
                ContainsUpdate = content.ContainsUpdate,
                ContainsDelete = content.ContainsDelete,
                ContainsMerge = content.ContainsMerge,
                ContainsOpenJson = content.ContainsOpenJson,
                ResultSets = newSetsStage2.ToArray(),
                UsedFallbackParser = content.UsedFallbackParser,
                ParseErrorCount = content.ParseErrorCount,
                FirstParseError = content.FirstParseError
            };
        }
        if (verbose && loggedColumnResolutions.Count > 0 && level != JsonTypeLogLevel.Off)
        {
            _console.Verbose($"[json-type-summary] {sp.SchemaName}.{sp.Name} resolved {loggedColumnResolutions.Count} columns (new={loggedNewConcrete}, upgrades={loggedUpgrades})");
        }
        else if (verbose && level == JsonTypeLogLevel.Detailed)
        {
            // still emit a summary line even if no resolutions, to show nullable adjustments / jsonquery activity
            _console.Verbose($"[json-type-summary] {sp.SchemaName}.{sp.Name} no new resolutions");
        }
        stats?.Accumulate(loggedColumnResolutions.Count, loggedNewConcrete, loggedUpgrades);
    }
}

