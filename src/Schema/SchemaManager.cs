using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Models;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Schema;

internal sealed class SchemaManager(
    DbContext dbContext,
    IConsoleService consoleService,
    Services.SchemaSnapshotFileLayoutService expandedSnapshotService,
    ILocalCacheService? localCacheService = null
)
{
    private static bool ShouldDiagJsonMissAst()
    {
        return LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug) || LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Trace);
    }
    // Logging prefixes used in this manager (overview):
    // [proc-fixup-json-reparse]   Added JSON sets after placeholder-only detection via reparse.
    // [proc-fixup-json-cols]      Extracted column names for synthetic JSON set.
    // [proc-fixup-json-synth]     Synthesized minimal JSON result set when only EXEC placeholder existed.
    // [proc-prune-json-nested]    Removed a nested FOR JSON expression-derived (inline) result set (not a real top-level output).
    // [proc-prune-json-nested-warn] Heuristic prune failure (exception swallowed, verbose only).
    // [proc-forward-multi]        Inserted multiple ProcedureRef placeholders for wrapper with >1 EXECs.
    // [proc-forward-refonly]      Inserted single ProcedureRef placeholder preserving local sets (reference-only forwarding).
    // [proc-forward-replace]      Replaced placeholder(s) entirely with forwarded target sets.
    // [proc-forward-append]       Appended forwarded sets after existing local sets.
    // [proc-forward-xschema]      Forwarded sets from cross-schema target (snapshot or direct load).
    // [proc-forward-xschema-upgrade] Variation when upgrading empty JSON placeholders.
    // [proc-exec-append]          Appended target sets for non-wrapper with single EXEC.
    // [proc-exec-append-xschema]  Cross-schema append variant.
    // [proc-wrapper-posthoc]      Post-hoc normalization to single ProcedureRef placeholder for pure wrapper.
    // [proc-dedupe]               Removed duplicate result sets after forwarding/append phases.
    // [proc-exec-miss]            EXEC keyword seen but AST failed to capture executed procedure (diagnostic).
    // [ignore]                    Schema ignore processing diagnostics.
    // [cache]                     Local cache snapshot load/save events.
    // [timing]                    Overall timing diagnostics.
    // These prefixes allow downstream log consumers to filter transformation phases precisely.
    public async Task<List<SchemaModel>> ListAsync(SchemaSelectionContext? context, bool noCache = false, CancellationToken cancellationToken = default)
    {
        context ??= new SchemaSelectionContext();

        SchemaSnapshot? expandedSnapshot = null;
        var userDefinedTypeLookup = new Dictionary<string, SnapshotUserDefinedType>(StringComparer.OrdinalIgnoreCase);
        try
        {
            expandedSnapshot = expandedSnapshotService.LoadExpanded();
            userDefinedTypeLookup = BuildUserDefinedTypeLookup(expandedSnapshot);
        }
        catch
        {
            expandedSnapshot = null;
            userDefinedTypeLookup = new Dictionary<string, SnapshotUserDefinedType>(StringComparer.OrdinalIgnoreCase);
        }
        // Ensure AST parser can resolve table column types for CTE type propagation into nested JSON
        if (StoredProcedureContentModel.ResolveTableColumnType == null)
        {
            // Prefer snapshot metadata (expanded) over live DB calls. Tables/Views/UDTTs/UDTs are loaded before procedures.
            var tableMeta = new Xtraq.Metadata.TableMetadataProvider(DirectoryUtils.GetWorkingDirectory());
            var tableIndex = tableMeta.GetAll()?.GroupBy(t => t.Schema + "." + t.Name, StringComparer.OrdinalIgnoreCase)
                                      .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase) ?? new Dictionary<string, Xtraq.Metadata.TableInfo>(StringComparer.OrdinalIgnoreCase);

            StoredProcedureContentModel.ResolveTableColumnType = (schema, table, column) =>
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(table) || string.IsNullOrWhiteSpace(column))
                        return (string.Empty, null, null);
                    var key = schema + "." + table;
                    if (tableIndex.TryGetValue(key, out var ti))
                    {
                        var col = ti.Columns?.FirstOrDefault(c => c.Name.Equals(column, StringComparison.OrdinalIgnoreCase));
                        if (col != null && !string.IsNullOrWhiteSpace(col.SqlType))
                        {
                            return (col.SqlType, col.MaxLength, col.IsNullable);
                        }
                    }
                }
                catch { }
                return (string.Empty, null, null);
            };
        }

        // Provide UDT resolver from expanded snapshot (UserDefinedTypes)
        if (StoredProcedureContentModel.ResolveUserDefinedType == null && userDefinedTypeLookup.Count > 0)
        {
            try
            {
                var udtMap = userDefinedTypeLookup;
                StoredProcedureContentModel.ResolveUserDefinedType = (schema, name) =>
                {
                    try
                    {
                        var normalizedName = NormalizeTypeSegment(name);
                        if (string.IsNullOrWhiteSpace(normalizedName))
                        {
                            return (string.Empty, null, null, null, null);
                        }

                        var normalizedSchema = NormalizeTypeSegment(schema);
                        var udt = FindUserDefinedType(udtMap, normalizedSchema, normalizedName);
                        if (udt != null)
                        {
                            var baseType = udt.BaseSqlTypeName;
                            int? maxLen = udt.MaxLength;
                            int? prec = udt.Precision;
                            int? scale = udt.Scale;
                            bool? isNull = udt.IsNullable;

                            if (normalizedName.StartsWith("_", StringComparison.Ordinal))
                            {
                                isNull = false;
                            }

                            return (baseType, maxLen, prec, scale, isNull);
                        }

                        if (normalizedName.StartsWith("_", StringComparison.Ordinal))
                        {
                            var trimmed = normalizedName.TrimStart('_');
                            var underscored = FindUserDefinedType(udtMap, normalizedSchema, trimmed);
                            if (underscored != null)
                            {
                                return (underscored.BaseSqlTypeName, underscored.MaxLength, underscored.Precision, underscored.Scale, false);
                            }
                        }
                    }
                    catch { }
                    return (string.Empty, null, null, null, null);
                };
            }
            catch { }
        }

        // Provide scalar function return type resolver from expanded snapshot (Functions)
        if (StoredProcedureContentModel.ResolveScalarFunctionReturnType == null && expandedSnapshot?.Functions?.Count > 0)
        {
            try
            {
                var fnMap = new Dictionary<string, (string SqlType, int? MaxLen, bool? IsNull)>(StringComparer.OrdinalIgnoreCase);
                foreach (var fn in expandedSnapshot.Functions)
                {
                    if (fn?.IsTableValued == true) continue; // only scalar
                    if (string.IsNullOrWhiteSpace(fn?.Schema) || string.IsNullOrWhiteSpace(fn?.Name)) continue;
                    if (string.IsNullOrWhiteSpace(fn.ReturnSqlType)) continue;
                    var key = $"{fn.Schema}.{fn.Name}";
                    var resolvedType = fn.ReturnSqlType;
                    fnMap[key] = (resolvedType, fn.ReturnMaxLength, fn.ReturnIsNullable);
                    if (!fnMap.ContainsKey(fn.Name))
                    {
                        fnMap[fn.Name] = (resolvedType, fn.ReturnMaxLength, fn.ReturnIsNullable);
                    }
                }
                StoredProcedureContentModel.ResolveScalarFunctionReturnType = (schema, name) =>
                {
                    try
                    {
                        if (string.IsNullOrWhiteSpace(name)) return (string.Empty, null, null);
                        var key1 = (schema ?? "dbo") + "." + name;
                        if (fnMap.TryGetValue(key1, out var meta) || fnMap.TryGetValue(name, out meta))
                        {
                            return (meta.SqlType, meta.MaxLen, meta.IsNull);
                        }
                    }
                    catch { }
                    return (string.Empty, null, null);
                };
            }
            catch { }
        }

        // Provide callback for ensuring remote scalar types are fetched on demand for cross-catalog references
        var dbSchemas = await dbContext.SchemaListAsync(cancellationToken);
        if (dbSchemas == null)
        {
            return new List<SchemaModel>();
        }

        var schemas = dbSchemas.Select(static i => new SchemaModel(i)).ToList();

        var statusEvaluator = new SchemaStatusEvaluator();
        var statusResult = statusEvaluator.Evaluate(context, schemas);
        schemas = statusResult.Schemas;
        var activeSchemas = statusResult.ActiveSchemas;

        if (statusResult.BuildSchemasChanged)
        {
            context.BuildSchemas = statusResult.BuildSchemas.ToList();
        }

        if (statusResult.BuildSchemas.Count > 0)
        {
            consoleService.Verbose($"[build-schemas] Build schema allow-list contains {statusResult.BuildSchemas.Count} schema(s)");
        }
        // Build list only for later filtering/persistence logic; we enumerate procedures for all schemas unfiltered.
        var storedProcedures = await dbContext.StoredProcedureListAsync(string.Empty, cancellationToken) ?? new List<StoredProcedure>();
        var schemaListString = string.Join(',', activeSchemas.Select(i => $"'{i.Name}'"));

        var jsonTypeLogLevel = context.JsonTypeLogLevel;

        var buildProcedures = Environment.GetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES");
        var procedureFilter = ProcedureFilter.Create(buildProcedures, consoleService);
        if (procedureFilter.HasFilter)
        {
            var beforeCount = storedProcedures.Count;
            storedProcedures = procedureFilter
                .Apply(storedProcedures, static sp => $"{sp.SchemaName}.{sp.Name}")
                .ToList();
            var kept = storedProcedures.Count;
            if (kept != beforeCount)
            {
                consoleService.Verbose($"[procedure-filter] Filtered to {kept} procedure(s) via --procedure flag (was {beforeCount})");
            }
        }

        // Build a simple fingerprint (avoid secrets): use output namespace or role kind + schemas + SP count
        var projectId = string.IsNullOrWhiteSpace(context.ProjectNamespace) ? "UnknownProject" : context.ProjectNamespace;
        var fingerprintRaw = $"{projectId}|{schemaListString}|{storedProcedures.Count}";
        var fingerprint = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(fingerprintRaw))).Substring(0, 16);

        var loadStart = DateTime.UtcNow;
        var disableCache = noCache; // Global cache toggle; per-procedure forcing handled below

        ProcedureCacheSnapshot? cache = null;
        if (!disableCache && localCacheService != null)
        {
            cache = localCacheService.Load(fingerprint);
            if (cache != null)
            {
                consoleService.Verbose($"[cache] Loaded snapshot {fingerprint} with {cache.Procedures.Count} entries in {(DateTime.UtcNow - loadStart).TotalMilliseconds:F1} ms");
            }
            else
            {
                consoleService.Verbose($"[cache] No existing snapshot for {fingerprint}");
            }
        }
        else if (disableCache)
        {
            consoleService.Verbose("[cache] Disabled (--no-cache)");
        }

        // Handle schema changes and cache invalidation
        if (!disableCache && cache != null)
        {
            HandleSchemaChanges(storedProcedures, cache);
        }

        var updatedSnapshot = new ProcedureCacheSnapshot { Fingerprint = fingerprint };
        var tableTypes = await dbContext.TableTypeListAsync(schemaListString, cancellationToken);
        tableTypes ??= new List<TableType>();

        // Handle TableType changes for cache invalidation
        if (!disableCache && localCacheService != null && tableTypes?.Count > 0)
        {
            HandleTableTypeChanges(tableTypes, fingerprint);
        }
        var procedureOutputs = new Dictionary<string, List<StoredProcedureOutputModel>>(StringComparer.OrdinalIgnoreCase);

        var totalSpCount = storedProcedures.Count;
        var processed = 0;
        var lastPercentage = -1;
        if (totalSpCount > 0)
        {
            consoleService.StartProgress($"Loading Stored Procedures ({totalSpCount})");
            consoleService.DrawProgressBar(0);
        }
        // Change detection now exclusively uses local cache snapshot (previous config ignore)

        // NOTE: Current modification ticks are derived from sys.objects.modify_date (see StoredProcedure.Modified)

        // Build snapshot procedure lookup (prefer expanded layout) for hydration of skipped procedures
        Dictionary<string, Dictionary<string, SnapshotProcedure>>? snapshotProcMap = null;
        try
        {
            if (!disableCache)
            {
                var working = DirectoryUtils.GetWorkingDirectory();
                var schemaDir = System.IO.Path.Combine(working, ".xtraq", "snapshots");
                if (System.IO.Directory.Exists(schemaDir))
                {
                    SchemaSnapshot? expanded = null;
                    try
                    {
                        expanded = expandedSnapshotService.LoadExpanded();
                        if (expanded?.Procedures?.Any() == true)
                        {
                            snapshotProcMap = expanded.Procedures
                                .GroupBy(p => p.Schema, StringComparer.OrdinalIgnoreCase)
                                .ToDictionary(g => g.Key, g => g.ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
                            consoleService.Verbose($"[snapshot-hydrate] Expanded snapshot geladen (fingerprint={expanded.Fingerprint}) procs={expanded.Procedures.Count}");
                        }
                    }
                    catch { /* best effort */ }
                }
            }
        }
        catch { /* best effort */ }

        // --- Stored Procedure Enumeration & Hydration (reconstructed cleanly) ---
        foreach (var schema in schemas)
        {
            schema.StoredProcedures = storedProcedures
                .Where(sp => sp.SchemaName.Equals(schema.Name, StringComparison.OrdinalIgnoreCase))
                .Select(sp => new StoredProcedureModel(sp))
                .ToList();
            if (schema.StoredProcedures == null) continue;

            foreach (var storedProcedure in schema.StoredProcedures)
            {
                processed++;
                if (totalSpCount > 0)
                {
                    var percentage = (processed * 100) / totalSpCount;
                    if (percentage != lastPercentage)
                    {
                        consoleService.DrawProgressBar(percentage);
                        lastPercentage = percentage;
                    }
                }

                var currentModifiedTicks = storedProcedure.Modified.Ticks;
                var cacheEntry = cache?.Procedures.FirstOrDefault(p => p.Schema == storedProcedure.SchemaName && p.Name == storedProcedure.Name);
                var previousModifiedTicks = cacheEntry?.ModifiedTicks;
                var canSkipDetails = !disableCache && previousModifiedTicks.HasValue && previousModifiedTicks.Value == currentModifiedTicks;
                // Force re-parse for procedures selected via --procedure (per-proc, not global), wildcard-aware
                if (procedureFilter.HasFilter)
                {
                    var fq = $"{storedProcedure.SchemaName}.{storedProcedure.Name}";
                    if (procedureFilter.Matches(fq))
                    {
                        if (canSkipDetails && jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                        {
                            consoleService.Verbose($"[cache] Re-parse forced for {fq} due to --procedure flag");
                        }
                        canSkipDetails = false;
                    }
                }
                // Skip decision: previously required snapshot hydration presence; caching test expects skip purely on modify_date stability.
                // We retain hydration usage when available but do not downgrade skip if absent.
                if (canSkipDetails && snapshotProcMap != null)
                {
                    bool hasHydration = snapshotProcMap.TryGetValue(storedProcedure.SchemaName, out var spMap) && spMap.ContainsKey(storedProcedure.Name);
                    // If hydration exists we may populate inputs/resultsets later; absence no longer forces parse.
                    if (!hasHydration && jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                    {
                        consoleService.Verbose($"[proc-skip-no-hydration] {storedProcedure.SchemaName}.{storedProcedure.Name} skipping without snapshot hydration");
                    }
                }

                if (canSkipDetails)
                {
                    if (jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                        consoleService.Verbose($"[proc-skip] {storedProcedure.SchemaName}.{storedProcedure.Name} unchanged (ticks={currentModifiedTicks})");
                    if (snapshotProcMap != null && snapshotProcMap.TryGetValue(storedProcedure.SchemaName, out var spMap) && spMap.TryGetValue(storedProcedure.Name, out var snapProc))
                    {
                        // Inputs hydration
                        if (snapProc.Inputs?.Any() == true && (storedProcedure.Input == null || !storedProcedure.Input.Any()))
                        {
                            storedProcedure.Input = snapProc.Inputs
                                .Select(MapSnapshotInputToModel)
                                .OfType<StoredProcedureInput>()
                                .ToList();
                        }
                        // ResultSets hydration
                        if (snapProc.ResultSets?.Any() == true && (storedProcedure.Content?.ResultSets == null || !storedProcedure.Content.ResultSets.Any()))
                        {
                            StoredProcedureContentModel.ResultColumn MapSnapshotColToRuntime(SnapshotResultColumn c)
                            {
                                if (c == null)
                                {
                                    return new StoredProcedureContentModel.ResultColumn();
                                }

                                var resolvedUserTypeRef = NormalizeTypeRef(c.UserTypeRef);
                                var fallbackTypeRef = NormalizeTypeRef(c.TypeRef);
                                var typeRefToUse = !string.IsNullOrWhiteSpace(resolvedUserTypeRef)
                                    ? resolvedUserTypeRef
                                    : fallbackTypeRef;

                                var (_, schema, name) = TypeRefUtilities.Split(typeRefToUse);
                                var isSystemType = IsSystemSchema(schema);

                                var column = new StoredProcedureContentModel.ResultColumn
                                {
                                    Name = c.Name,
                                    Alias = string.IsNullOrWhiteSpace(c.Alias) ? null : c.Alias,
                                    SourceColumn = string.IsNullOrWhiteSpace(c.SourceColumn) ? null : c.SourceColumn,
                                    SqlTypeName = string.IsNullOrWhiteSpace(c.SqlTypeName) ? null : c.SqlTypeName,
                                    IsNullable = c.IsNullable,
                                    MaxLength = c.MaxLength,
                                    IsNestedJson = c.IsNestedJson,
                                    ReturnsJson = c.ReturnsJson,
                                    ReturnsJsonArray = c.ReturnsJsonArray,
                                    ReturnsUnknownJson = c.ReturnsUnknownJson,
                                    JsonRootProperty = c.JsonRootProperty,
                                    JsonIncludeNullValues = c.JsonIncludeNullValues,
                                    JsonElementClrType = c.JsonElementClrType,
                                    JsonElementSqlType = c.JsonElementSqlType,
                                    DeferredJsonExpansion = c.DeferredJsonExpansion
                                };

                                if (!isSystemType)
                                {
                                    column.UserTypeRef = typeRefToUse;
                                    ApplyUserTypeMetadata(column, typeRefToUse, userDefinedTypeLookup);
                                }

                                column.SqlTypeName ??= BuildSqlTypeName(schema, name);

                                if (c.Columns != null && c.Columns.Count > 0)
                                {
                                    column.Columns = c.Columns.Select(MapSnapshotColToRuntime).ToArray();
                                }

                                if (c.Reference != null)
                                {
                                    column.Reference = new StoredProcedureContentModel.ColumnReferenceInfo
                                    {
                                        Kind = c.Reference.Kind,
                                        Schema = c.Reference.Schema,
                                        Name = c.Reference.Name
                                    };
                                }
                                else if (!string.IsNullOrWhiteSpace(c.FunctionRef))
                                {
                                    var (refSchema, refName) = SplitTypeRef(c.FunctionRef);
                                    if (!string.IsNullOrWhiteSpace(refName))
                                    {
                                        column.Reference = new StoredProcedureContentModel.ColumnReferenceInfo
                                        {
                                            Kind = "Function",
                                            Schema = refSchema ?? string.Empty,
                                            Name = refName
                                        };
                                    }
                                }

                                return column;
                            }
                            var rsModels = snapProc.ResultSets.Select(rs => new StoredProcedureContentModel.ResultSet
                            {
                                ReturnsJson = rs.ReturnsJson,
                                ReturnsJsonArray = rs.ReturnsJsonArray,
                                // removed flag
                                JsonRootProperty = rs.JsonRootProperty,
                                ExecSourceSchemaName = rs.ExecSourceSchemaName,
                                ExecSourceProcedureName = rs.ExecSourceProcedureName,
                                HasSelectStar = rs.HasSelectStar == true,
                                Columns = rs.Columns.Select(MapSnapshotColToRuntime).ToArray()
                            }).ToArray();
                            storedProcedure.Content = new StoredProcedureContentModel
                            {
                                Definition = storedProcedure.Content?.Definition,
                                Statements = storedProcedure.Content?.Statements ?? Array.Empty<string>(),
                                ContainsSelect = storedProcedure.Content?.ContainsSelect ?? true,
                                ContainsInsert = storedProcedure.Content?.ContainsInsert ?? false,
                                ContainsUpdate = storedProcedure.Content?.ContainsUpdate ?? false,
                                ContainsDelete = storedProcedure.Content?.ContainsDelete ?? false,
                                ContainsMerge = storedProcedure.Content?.ContainsMerge ?? false,
                                ContainsOpenJson = storedProcedure.Content?.ContainsOpenJson ?? false,
                                ResultSets = rsModels,
                                UsedFallbackParser = storedProcedure.Content?.UsedFallbackParser ?? false,
                                ParseErrorCount = storedProcedure.Content?.ParseErrorCount ?? 0,
                                FirstParseError = storedProcedure.Content?.FirstParseError,
                                ExecutedProcedures = storedProcedure.Content?.ExecutedProcedures
                                    ?? Array.Empty<StoredProcedureContentModel.ExecutedProcedureCall>()
                            };
                        }
                        // Minimal input hydration if not present even after skip
                        if (storedProcedure.Input == null)
                        {
                            var inputs = await dbContext.StoredProcedureInputListAsync(storedProcedure.SchemaName, storedProcedure.Name, cancellationToken);
                            storedProcedure.Input = inputs?.ToList();
                        }
                    }
                }
                else
                {
                    // Full load & parse
                    var def = await dbContext.StoredProcedureDefinitionAsync(storedProcedure.SchemaName, storedProcedure.Name, cancellationToken);
                    var definition = def?.Definition;
                    storedProcedure.Content = StoredProcedureContentModel.Parse(definition ?? string.Empty, storedProcedure.SchemaName);
                    if (jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                    {
                        if (storedProcedure.Content?.UsedFallbackParser == true)
                        {
                            consoleService.Verbose($"[proc-parse-fallback] {storedProcedure.SchemaName}.{storedProcedure.Name} parse errors={storedProcedure.Content.ParseErrorCount} first='{storedProcedure.Content.FirstParseError}'");
                        }
                        else if (storedProcedure.Content?.ResultSets?.Count > 1)
                        {
                            consoleService.Verbose($"[proc-json-multi] {storedProcedure.SchemaName}.{storedProcedure.Name} sets={storedProcedure.Content.ResultSets.Count}");
                        }
                        else if (previousModifiedTicks.HasValue)
                        {
                            consoleService.Verbose($"[proc-loaded] {storedProcedure.SchemaName}.{storedProcedure.Name} updated {previousModifiedTicks.Value} -> {currentModifiedTicks}");
                        }
                        else
                        {
                            consoleService.Verbose($"[proc-loaded] {storedProcedure.SchemaName}.{storedProcedure.Name} initial load (ticks={currentModifiedTicks})");
                        }
                    }

                    // Inputs & Outputs
                    var inputsFull = await dbContext.StoredProcedureInputListAsync(storedProcedure.SchemaName, storedProcedure.Name, cancellationToken);
                    storedProcedure.Input = inputsFull?.ToList();

                    var outputsFull = await dbContext.StoredProcedureOutputListAsync(storedProcedure.SchemaName, storedProcedure.Name, cancellationToken);
                    var outputModels = outputsFull?.Select(i => new StoredProcedureOutputModel(i)).ToList() ?? new List<StoredProcedureOutputModel>();
                    procedureOutputs[$"{storedProcedure.SchemaName}.{storedProcedure.Name}"] = outputModels;

                    // Synthesize ResultSet if no JSON sets and none parsed
                    var anyJson = storedProcedure.Content?.ResultSets?.Any(r => r.ReturnsJson) == true;
                    if (!anyJson && (storedProcedure.Content?.ResultSets == null || !storedProcedure.Content.ResultSets.Any()))
                    {
                        var syntheticColumns = outputModels.Select(o => new StoredProcedureContentModel.ResultColumn
                        {
                            Name = o.Name,
                            SqlTypeName = o.SqlTypeName,
                            IsNullable = o.IsNullable,
                            MaxLength = o.MaxLength
                        }).ToArray();
                        var syntheticSet = new StoredProcedureContentModel.ResultSet
                        {
                            ReturnsJson = false,
                            ReturnsJsonArray = false,
                            // removed flag
                            JsonRootProperty = null,
                            Columns = syntheticColumns
                        };
                        // Legacy FOR JSON single-column upgrade
                        if (syntheticSet.Columns.Count == 1 && string.Equals(syntheticSet.Columns[0].Name, "JSON_F52E2B61-18A1-11d1-B105-00805F49916B", StringComparison.OrdinalIgnoreCase) && (syntheticSet.Columns[0].SqlTypeName?.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase) ?? false))
                        {
                            if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug))
                            {
                                syntheticSet = new StoredProcedureContentModel.ResultSet
                                {
                                    ReturnsJson = true,
                                    ReturnsJsonArray = true,
                                    JsonRootProperty = null,
                                    Columns = Array.Empty<StoredProcedureContentModel.ResultColumn>()
                                };
                                if (jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                                    consoleService.Verbose($"[proc-json-legacy-upgrade] {storedProcedure.SchemaName}.{storedProcedure.Name} single synthetic FOR JSON column upgraded (flag)");
                            }
                            else if (ShouldDiagJsonMissAst())
                            {
                                consoleService.Output($"[proc-json-legacy-skip] {storedProcedure.SchemaName}.{storedProcedure.Name} legacy single-column FOR JSON sentinel ignored (flag disabled)");
                            }
                        }
                        storedProcedure.Content = new StoredProcedureContentModel
                        {
                            Definition = storedProcedure.Content?.Definition ?? definition,
                            Statements = storedProcedure.Content?.Statements ?? Array.Empty<string>(),
                            ContainsSelect = storedProcedure.Content?.ContainsSelect ?? false,
                            ContainsInsert = storedProcedure.Content?.ContainsInsert ?? false,
                            ContainsUpdate = storedProcedure.Content?.ContainsUpdate ?? false,
                            ContainsDelete = storedProcedure.Content?.ContainsDelete ?? false,
                            ContainsMerge = storedProcedure.Content?.ContainsMerge ?? false,
                            ContainsOpenJson = storedProcedure.Content?.ContainsOpenJson ?? false,
                            ResultSets = new[] { syntheticSet },
                            UsedFallbackParser = storedProcedure.Content?.UsedFallbackParser ?? false,
                            ParseErrorCount = storedProcedure.Content?.ParseErrorCount ?? 0,
                            FirstParseError = storedProcedure.Content?.FirstParseError,
                            ExecutedProcedures = storedProcedure.Content?.ExecutedProcedures
                                ?? Array.Empty<StoredProcedureContentModel.ExecutedProcedureCall>()
                        };
                    }
                }

                // Removed regex-based prune/recovery logic; JSON detection now purely AST-driven (StoredProcedureContentModel.Parse).

                storedProcedure.ModifiedTicks = currentModifiedTicks;
                updatedSnapshot.Procedures.Add(new ProcedureCacheEntry
                {
                    Schema = storedProcedure.SchemaName,
                    Name = storedProcedure.Name,
                    ModifiedTicks = currentModifiedTicks
                });
            }

            // TableTypes per schema
            var tableTypeModels = new List<TableTypeModel>();
            var tableTypesForSchema = tableTypes?.Where(tt => tt.SchemaName.Equals(schema.Name, StringComparison.OrdinalIgnoreCase))
                ?? Enumerable.Empty<TableType>();
            foreach (var tableType in tableTypesForSchema)
            {
                var columns = await dbContext.TableTypeColumnListAsync(tableType.UserTypeId ?? -1, cancellationToken);
                tableTypeModels.Add(new TableTypeModel(tableType, columns));
            }
            schema.TableTypes = tableTypeModels;
        }

        // Simplified forwarding normalization: create placeholders for each EXEC only.
        // No cloning or appending of target result sets; recursive expansion happens later during generation.
        try
        {
            var allProcedures = schemas.SelectMany(s => s.StoredProcedures ?? Enumerable.Empty<StoredProcedureModel>()).ToList();
            foreach (var proc in allProcedures)
            {
                var c = proc.Content;
                if (c?.ExecutedProcedures == null || c.ExecutedProcedures.Count == 0) continue;
                var localSets = c.ResultSets?.Where(rs => string.IsNullOrEmpty(rs.ExecSourceProcedureName)).ToList() ?? new List<StoredProcedureContentModel.ResultSet>();
                var placeholders = c.ExecutedProcedures
                    .Where(e => e != null)
                    .Select(e =>
                    {
                        var target = allProcedures.FirstOrDefault(p =>
                            p.SchemaName.Equals(e.Schema, StringComparison.OrdinalIgnoreCase) &&
                            p.Name.Equals(e.Name, StringComparison.OrdinalIgnoreCase));
                        var forwardsJson = target?.Content?.ResultSets?.Any(rs => rs.ReturnsJson) == true;
                        var forwardsJsonArray = target?.Content?.ResultSets?.Any(rs => rs.ReturnsJsonArray) == true;
                        return new StoredProcedureContentModel.ResultSet
                        {
                            ExecSourceSchemaName = e.Schema,
                            ExecSourceProcedureName = e.Name,
                            ReturnsJson = forwardsJson,
                            ReturnsJsonArray = forwardsJsonArray,
                            Columns = Array.Empty<StoredProcedureContentModel.ResultColumn>()
                        };
                    }).ToList();
                // Current order: placeholders precede local sets (simplified assumption) and can be refined later with position data.
                var augmentedLocalSets = localSets;
                if (localSets.Count > 0 && procedureOutputs.TryGetValue($"{proc.SchemaName}.{proc.Name}", out var outputs) && outputs.Count > 0)
                {
                    augmentedLocalSets = localSets.Select(rs =>
                    {
                        if (rs.Columns != null && rs.Columns.Count > 0) return rs;
                        var cols = outputs.Select(o => new StoredProcedureContentModel.ResultColumn
                        {
                            Name = o.Name,
                            SqlTypeName = o.SqlTypeName,
                            IsNullable = o.IsNullable,
                            MaxLength = o.MaxLength
                        }).ToArray();
                        return new StoredProcedureContentModel.ResultSet
                        {
                            ReturnsJson = rs.ReturnsJson,
                            ReturnsJsonArray = rs.ReturnsJsonArray,
                            JsonRootProperty = rs.JsonRootProperty,
                            ExecSourceSchemaName = rs.ExecSourceSchemaName,
                            ExecSourceProcedureName = rs.ExecSourceProcedureName,
                            HasSelectStar = rs.HasSelectStar,
                            Reference = rs.Reference,
                            Columns = cols
                        };
                    }).ToList();
                }

                var combined = new List<StoredProcedureContentModel.ResultSet>();
                combined.AddRange(placeholders);
                combined.AddRange(augmentedLocalSets);
                proc.Content = new StoredProcedureContentModel
                {
                    Definition = c.Definition,
                    Statements = c.Statements,
                    ContainsSelect = c.ContainsSelect,
                    ContainsInsert = c.ContainsInsert,
                    ContainsUpdate = c.ContainsUpdate,
                    ContainsDelete = c.ContainsDelete,
                    ContainsMerge = c.ContainsMerge,
                    ContainsOpenJson = c.ContainsOpenJson,
                    ResultSets = combined,
                    UsedFallbackParser = c.UsedFallbackParser,
                    ParseErrorCount = c.ParseErrorCount,
                    FirstParseError = c.FirstParseError,
                    ExecutedProcedures = c.ExecutedProcedures
                };
                consoleService.Verbose($"[proc-forward-placeholders] {proc.SchemaName}.{proc.Name} => placeholders={placeholders.Count} localSets={localSets.Count}");
            }
        }
        catch { /* best effort */ }

        if (totalSpCount > 0)
        {
            // Final completion already visually implied by 100% updates; CompleteProgress will emit separator + status.
            // Removed redundant DrawProgressBar(100) to avoid double rendering.
            consoleService.CompleteProgress(true, $"Loaded {totalSpCount} stored procedures");
        }

        // Persist updated cache (best effort)
        var saveStart = DateTime.UtcNow;
        // Reverted: no additional stub creation for ExecSource targets in the cache; original logic restored.
        if (!disableCache)
        {
            // Sort cache entries for deterministic ordering (schema, procedure name)
            if (updatedSnapshot.Procedures.Count > 1)
            {
                updatedSnapshot.Procedures = updatedSnapshot.Procedures
                    .OrderBy(p => p.Schema, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
            localCacheService?.Save(fingerprint, updatedSnapshot);
            consoleService.Verbose($"[cache] Saved snapshot {fingerprint} with {updatedSnapshot.Procedures.Count} entries in {(DateTime.UtcNow - saveStart).TotalMilliseconds:F1} ms");
        }
        else
        {
            consoleService.Verbose("[cache] Not saved (--no-cache)");
        }

        consoleService.Verbose($"[timing] Total schema load duration {(DateTime.UtcNow - loadStart).TotalMilliseconds:F1} ms");

        return schemas;
    }

    private static StoredProcedureInput? MapSnapshotInputToModel(SnapshotInput snapshotInput)
    {
        if (snapshotInput == null)
        {
            return null;
        }

        var stored = new StoredProcedureInput
        {
            Name = snapshotInput.Name ?? string.Empty,
            IsNullable = snapshotInput.IsNullable ?? false,
            MaxLength = snapshotInput.MaxLength ?? 0,
            IsOutput = snapshotInput.IsOutput ?? false,
            HasDefaultValue = snapshotInput.HasDefaultValue ?? false,
            Precision = snapshotInput.Precision,
            Scale = snapshotInput.Scale
        };

        var normalizedTableTypeRef = TableTypeRefFormatter.Normalize(snapshotInput.TableTypeRef);
        var (_, tableTypeSchemaFromRef, tableTypeNameFromRef) = TableTypeRefFormatter.Split(normalizedTableTypeRef);
        var (schemaFromRef, nameFromRef) = SplitTypeRef(snapshotInput.TypeRef);

        var tableTypeSchema = snapshotInput.TableTypeSchema ?? tableTypeSchemaFromRef;
        var tableTypeName = snapshotInput.TableTypeName ?? tableTypeNameFromRef;
        var scalarSchema = snapshotInput.TypeSchema ?? schemaFromRef;
        var scalarName = snapshotInput.TypeName ?? nameFromRef;

        if ((string.IsNullOrWhiteSpace(tableTypeSchema) || string.IsNullOrWhiteSpace(tableTypeName))
            && !string.IsNullOrWhiteSpace(schemaFromRef)
            && !string.IsNullOrWhiteSpace(nameFromRef)
            && !IsSystemSchema(schemaFromRef))
        {
            tableTypeSchema ??= schemaFromRef;
            tableTypeName ??= nameFromRef;
        }

        var isTableType = !string.IsNullOrWhiteSpace(tableTypeSchema) && !string.IsNullOrWhiteSpace(tableTypeName);

        if (isTableType)
        {
            stored.IsTableType = true;
            stored.UserTypeSchemaName = tableTypeSchema;
            stored.UserTypeName = tableTypeName;
            stored.SqlTypeName = normalizedTableTypeRef
                ?? BuildSqlTypeName(tableTypeSchema, tableTypeName)
                ?? snapshotInput.TypeRef
                ?? string.Empty;
        }
        else
        {
            stored.IsTableType = false;

            if (!IsSystemSchema(scalarSchema) && !string.IsNullOrWhiteSpace(scalarName))
            {
                stored.UserTypeSchemaName = scalarSchema;
                stored.UserTypeName = scalarName;
            }

            stored.SqlTypeName = BuildSqlTypeName(scalarSchema, scalarName) ?? snapshotInput.TypeRef ?? string.Empty;
        }

        return stored;
    }

    private static (string? Schema, string? Name) SplitTypeRef(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return (null, null);
        }

        var parts = typeRef.Trim().Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return (null, null);
        }

        var name = string.IsNullOrWhiteSpace(parts[^1]) ? null : parts[^1];
        var schema = parts.Length >= 2 ? (string.IsNullOrWhiteSpace(parts[^2]) ? null : parts[^2]) : null;
        return (schema, name);
    }

    private static string? NormalizeTypeRef(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return null;
        }

        var trimmed = typeRef.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string? BuildSqlTypeName(string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(schema) && !IsSystemSchema(schema))
        {
            return string.Concat(schema, ".", name);
        }

        return name;
    }

    private static bool IsSystemSchema(string? schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
        {
            return true;
        }

        return string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase);
    }

    private static Dictionary<string, SnapshotUserDefinedType> BuildUserDefinedTypeLookup(SchemaSnapshot? snapshot)
    {
        var map = new Dictionary<string, SnapshotUserDefinedType>(StringComparer.OrdinalIgnoreCase);
        if (snapshot?.UserDefinedTypes == null)
        {
            return map;
        }

        foreach (var udt in snapshot.UserDefinedTypes)
        {
            if (udt == null)
            {
                continue;
            }

            var schema = NormalizeTypeSegment(udt.Schema);
            var name = NormalizeTypeSegment(udt.Name);
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            AddUserTypeKey(map, BuildUserTypeKey(null, schema, name), udt, overwrite: true);
            AddUserTypeKey(map, BuildUserTypeKey(udt.Catalog, schema, name), udt, overwrite: true);
            AddUserTypeKey(map, BuildUserTypeKey(null, null, name), udt, overwrite: false);
        }

        return map;
    }

    private static void AddUserTypeKey(Dictionary<string, SnapshotUserDefinedType> lookup, string? key, SnapshotUserDefinedType udt, bool overwrite)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        if (!overwrite && lookup.ContainsKey(key))
        {
            return;
        }

        lookup[key] = udt;
    }

    private static string? BuildUserTypeKey(string? catalog, string? schema, string? name)
    {
        var normalizedName = NormalizeTypeSegment(name);
        if (string.IsNullOrWhiteSpace(normalizedName))
        {
            return null;
        }

        var segments = new List<string>(3);
        var normalizedCatalog = NormalizeTypeSegment(catalog);
        var normalizedSchema = NormalizeTypeSegment(schema);

        if (!string.IsNullOrWhiteSpace(normalizedCatalog))
        {
            segments.Add(normalizedCatalog);
        }

        if (!string.IsNullOrWhiteSpace(normalizedSchema))
        {
            segments.Add(normalizedSchema);
        }

        segments.Add(normalizedName);
        return string.Join('.', segments);
    }

    private static string? NormalizeTypeSegment(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static SnapshotUserDefinedType? FindUserDefinedType(IReadOnlyDictionary<string, SnapshotUserDefinedType> lookup, string? schema, string? name)
    {
        if (lookup == null || lookup.Count == 0 || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var schemaKey = BuildUserTypeKey(null, schema, name);
        if (!string.IsNullOrWhiteSpace(schemaKey) && lookup.TryGetValue(schemaKey, out var schemaMatch))
        {
            return schemaMatch;
        }

        var nameKey = BuildUserTypeKey(null, null, name);
        if (!string.IsNullOrWhiteSpace(nameKey) && lookup.TryGetValue(nameKey, out var nameMatch))
        {
            return nameMatch;
        }

        return null;
    }

    private static SnapshotUserDefinedType? FindUserDefinedType(IReadOnlyDictionary<string, SnapshotUserDefinedType> lookup, string? typeRef)
    {
        if (lookup == null || lookup.Count == 0 || string.IsNullOrWhiteSpace(typeRef))
        {
            return null;
        }

        var (catalog, schema, name) = TypeRefUtilities.Split(typeRef);
        var directKey = BuildUserTypeKey(catalog, schema, name);
        if (!string.IsNullOrWhiteSpace(directKey) && lookup.TryGetValue(directKey, out var direct))
        {
            return direct;
        }

        return FindUserDefinedType(lookup, schema, name);
    }

    private static void ApplyUserTypeMetadata(StoredProcedureContentModel.ResultColumn column, string? typeRef, IReadOnlyDictionary<string, SnapshotUserDefinedType> lookup)
    {
        if (column == null || string.IsNullOrWhiteSpace(typeRef) || lookup == null || lookup.Count == 0)
        {
            return;
        }

        var userType = FindUserDefinedType(lookup, typeRef);
        if (userType == null)
        {
            return;
        }

        var (_, schema, name) = TypeRefUtilities.Split(typeRef);
        var normalizedBaseType = NormalizeSystemTypeName(userType.BaseSqlTypeName);
        if (!string.IsNullOrWhiteSpace(normalizedBaseType) && (string.IsNullOrWhiteSpace(column.SqlTypeName) || (!string.IsNullOrWhiteSpace(schema) && !IsSystemSchema(schema))))
        {
            column.SqlTypeName = normalizedBaseType;
        }

        if (!column.MaxLength.HasValue && userType.MaxLength.HasValue)
        {
            column.MaxLength = userType.MaxLength;
        }

        if (!column.IsNullable.HasValue && userType.IsNullable.HasValue)
        {
            column.IsNullable = userType.IsNullable;
        }

        if (!string.IsNullOrWhiteSpace(name) && name.StartsWith("_", StringComparison.Ordinal))
        {
            column.IsNullable = false;
        }
    }

    private static string? NormalizeSystemTypeName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? null : trimmed.ToLowerInvariant();
    }

    /// <summary>
    /// Detects schema changes and triggers cache invalidation for affected objects.
    /// </summary>
    private void HandleSchemaChanges(List<Data.Models.StoredProcedure> currentProcedures, ProcedureCacheSnapshot previousCache)
    {
        if (localCacheService == null || previousCache == null) return;

        var hasChanges = false;
        var changedSchemas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Check for new or modified procedures
        foreach (var proc in currentProcedures)
        {
            var previousEntry = previousCache.Procedures
                .FirstOrDefault(p => p.Schema == proc.SchemaName && p.Name == proc.Name);

            if (previousEntry == null)
            {
                // New procedure - mark schema as changed
                changedSchemas.Add(proc.SchemaName);
                hasChanges = true;
                consoleService.Verbose($"[cache-invalidation] New procedure detected: {proc.SchemaName}.{proc.Name}");
            }
            else if (previousEntry.ModifiedTicks != proc.Modified.Ticks)
            {
                // Modified procedure - mark schema as changed
                changedSchemas.Add(proc.SchemaName);
                hasChanges = true;
                consoleService.Verbose($"[cache-invalidation] Modified procedure detected: {proc.SchemaName}.{proc.Name}");
            }
        }

        // Check for deleted procedures
        foreach (var previousEntry in previousCache.Procedures)
        {
            var stillExists = currentProcedures
                .Any(p => p.SchemaName == previousEntry.Schema && p.Name == previousEntry.Name);

            if (!stillExists)
            {
                changedSchemas.Add(previousEntry.Schema);
                hasChanges = true;
                consoleService.Verbose($"[cache-invalidation] Deleted procedure detected: {previousEntry.Schema}.{previousEntry.Name}");
            }
        }

        // Invalidate cache entries for changed schemas
        if (hasChanges && changedSchemas.Count > 0)
        {
            foreach (var schema in changedSchemas)
            {
                try
                {
                    localCacheService.InvalidateByPattern($"*{schema}*");
                    consoleService.Verbose($"[cache-invalidation] Invalidated cache for schema: {schema}");
                }
                catch (Exception ex)
                {
                    consoleService.Verbose($"[cache-invalidation] Failed to invalidate cache for schema {schema}: {ex.Message}");
                }
            }

            // Also invalidate table metadata cache for changed schemas
            try
            {
                Metadata.TableMetadataCacheRegistry.Invalidate(DirectoryUtils.GetWorkingDirectory());
                consoleService.Verbose("[cache-invalidation] Invalidated table metadata cache");
            }
            catch (Exception ex)
            {
                consoleService.Verbose($"[cache-invalidation] Failed to invalidate table metadata cache: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Handles TableType changes by tracking their names and invalidating cache when necessary.
    /// </summary>
    private void HandleTableTypeChanges(List<Data.Models.TableType> currentTableTypes, string fingerprint)
    {
        if (localCacheService == null || currentTableTypes == null) return;

        try
        {
            // For TableTypes we use a simpler approach: if any TableType has changed since last build,
            // we invalidate all caches since TableTypes can affect multiple procedures
            var tableTypeChanged = false;
            var lastBuildCacheKey = $"tabletypes_{fingerprint}";
            var lastBuildCache = localCacheService.Load(lastBuildCacheKey);

            if (lastBuildCache == null)
            {
                // First run - consider all TableTypes as new
                tableTypeChanged = currentTableTypes.Count > 0;
                if (tableTypeChanged)
                {
                    consoleService.Verbose($"[cache-invalidation] First TableType scan - found {currentTableTypes.Count} types");
                }
            }
            else
            {
                // Compare with previous run
                var previousTableTypeCount = lastBuildCache.Procedures?.Count ?? 0;
                if (currentTableTypes.Count != previousTableTypeCount)
                {
                    tableTypeChanged = true;
                    consoleService.Verbose($"[cache-invalidation] TableType count changed: {previousTableTypeCount} -> {currentTableTypes.Count}");
                }
                else
                {
                    // Check for additions/deletions by name (since we don't have modification timestamps)
                    var currentNames = currentTableTypes.Select(tt => $"{tt.SchemaName}.{tt.Name}").ToHashSet(StringComparer.OrdinalIgnoreCase);
                    var previousNames = lastBuildCache.Procedures?
                        .Select(p => $"{p.Schema}.{p.Name}")
                        .ToHashSet(StringComparer.OrdinalIgnoreCase) ?? new HashSet<string>();

                    if (!currentNames.SetEquals(previousNames))
                    {
                        tableTypeChanged = true;
                        var added = currentNames.Except(previousNames).ToList();
                        var removed = previousNames.Except(currentNames).ToList();

                        if (added.Count > 0)
                            consoleService.Verbose($"[cache-invalidation] TableTypes added: {string.Join(", ", added)}");
                        if (removed.Count > 0)
                            consoleService.Verbose($"[cache-invalidation] TableTypes removed: {string.Join(", ", removed)}");
                    }
                }
            }

            if (tableTypeChanged)
            {
                // TableType changes affect potentially all procedures, so we do a broader invalidation
                localCacheService.InvalidateAll();
                consoleService.Verbose("[cache-invalidation] TableType changes detected - invalidated all procedure caches");

                // Update the TableType tracking cache
                var tableTypeSnapshot = new ProcedureCacheSnapshot
                {
                    Fingerprint = lastBuildCacheKey,
                    Procedures = currentTableTypes.Select(tt => new ProcedureCacheEntry
                    {
                        Schema = tt.SchemaName,
                        Name = tt.Name,
                        ModifiedTicks = DateTime.UtcNow.Ticks // Use current time as placeholder
                    }).ToList()
                };
                localCacheService.Save(lastBuildCacheKey, tableTypeSnapshot);
            }
        }
        catch (Exception ex)
        {
            consoleService.Verbose($"[cache-invalidation] Error handling TableType changes: {ex.Message}");
        }
    }
}

