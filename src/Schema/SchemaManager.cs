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
        if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Trace)) return true;
        if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug)) return true;
        // Separate explicit gate if someone wants the message without full debug
        if (EnvironmentHelper.IsTrue("XTRAQ_JSON_MISS_AST_DIAG")) return true;
        return false;
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
    public async Task<List<SchemaModel>> ListAsync(ConfigurationModel config, bool noCache = false, CancellationToken cancellationToken = default)
    {
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
        if (StoredProcedureContentModel.ResolveUserDefinedType == null)
        {
            try
            {
                var expanded = expandedSnapshotService.LoadExpanded();
                var udtMap = new Dictionary<string, Services.SnapshotUserDefinedType>(StringComparer.OrdinalIgnoreCase);
                foreach (var u in expanded?.UserDefinedTypes ?? new List<Services.SnapshotUserDefinedType>())
                {
                    if (!string.IsNullOrWhiteSpace(u?.Schema) && !string.IsNullOrWhiteSpace(u?.Name))
                    {
                        udtMap[$"{u.Schema}.{u.Name}"] = u;
                        udtMap[u.Name] = u; // allow name-only match for common UDT schemas
                        if (!string.IsNullOrWhiteSpace(u.Catalog))
                        {
                            var catalogKey = string.Concat(u.Catalog, ".", u.Schema, ".", u.Name);
                            udtMap[catalogKey] = u;
                        }
                    }
                }
                StoredProcedureContentModel.ResolveUserDefinedType = (schema, name) =>
                {
                    try
                    {
                        if (string.IsNullOrWhiteSpace(name)) return (string.Empty, null, null, null, null);
                        var key1 = schema + "." + name;
                        if (udtMap.TryGetValue(key1, out var udt) || udtMap.TryGetValue(name, out udt))
                        {
                            var baseType = udt.BaseSqlTypeName;
                            int? maxLen = udt.MaxLength;
                            int? prec = udt.Precision;
                            int? scale = udt.Scale;
                            bool? isNull = udt.IsNullable;
                            // Honor underscore variant semantics: NOT NULL
                            if (!string.IsNullOrWhiteSpace(name) && name.StartsWith("_")) isNull = false;
                            return (baseType, maxLen, prec, scale, isNull);
                        }
                        // Fallback: strip leading underscore from UDT name if present (e.g., core._id -> core.id)
                        if (name.StartsWith("_"))
                        {
                            var trimmed = name.TrimStart('_');
                            var key2 = schema + "." + trimmed;
                            if (udtMap.TryGetValue(key2, out var udt2) || udtMap.TryGetValue(trimmed, out udt2))
                            {
                                var baseType = udt2.BaseSqlTypeName;
                                int? maxLen = udt2.MaxLength;
                                int? prec = udt2.Precision;
                                int? scale = udt2.Scale;
                                bool? isNull = false; // underscore -> NOT NULL
                                return (baseType, maxLen, prec, scale, isNull);
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
        if (StoredProcedureContentModel.ResolveScalarFunctionReturnType == null)
        {
            try
            {
                var expanded = expandedSnapshotService.LoadExpanded();
                var fnMap = new Dictionary<string, (string SqlType, int? MaxLen, bool? IsNull)>(StringComparer.OrdinalIgnoreCase);
                foreach (var fn in expanded?.Functions ?? new List<Services.SnapshotFunction>())
                {
                    if (fn?.IsTableValued == true) continue; // only scalar
                    if (string.IsNullOrWhiteSpace(fn?.Schema) || string.IsNullOrWhiteSpace(fn?.Name)) continue;
                    if (string.IsNullOrWhiteSpace(fn.ReturnSqlType)) continue;
                    var key = $"{fn.Schema}.{fn.Name}";
                    // Use ReturnSqlType as-is (collector may already include precision/scale or length)
                    var resolvedType = fn.ReturnSqlType;
                    fnMap[key] = (resolvedType, fn.ReturnMaxLength, fn.ReturnIsNullable);
                    // also allow name-only lookup for common schemas
                    if (!fnMap.ContainsKey(fn.Name)) fnMap[fn.Name] = (resolvedType, fn.ReturnMaxLength, fn.ReturnIsNullable);
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

        // Legacy schema list (config.Schema) still present -> use its statuses first
        if (config?.Schema != null)
        {
            foreach (var schema in schemas)
            {
                var currentSchema = config.Schema.SingleOrDefault(i => i.Name == schema.Name);
                schema.Status = (currentSchema != null)
                    ? currentSchema.Status
                    : config.Project.DefaultSchemaStatus;
            }
        }
        else if (config?.Project != null)
        {
            // Snapshot-only mode (legacy schema node removed).
            // Revised semantics for DefaultSchemaStatus=Ignore:
            //   - ONLY brand new schemas (not present in the latest snapshot) are auto-ignored and added to IgnoredSchemas.
            //   - Previously known schemas default to Build unless explicitly ignored.
            // For any other default value the prior fallback behavior applies.

            var ignored = config.Project.IgnoredSchemas ?? new List<string>();
            var defaultStatus = config.Project.DefaultSchemaStatus;

            // Determine known schemas from latest snapshot (if present)
            var knownSchemas = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var working = DirectoryUtils.GetWorkingDirectory();
                var schemaDir = System.IO.Path.Combine(working, ".xtraq", "snapshots");
                // Fixup phase: repair procedures that have exactly one EXEC placeholder but are missing a local JSON result set.
                // Scenario: parser did not capture the FOR JSON SELECT (e.g. complex construction) and after forwarding only a placeholder remains.
                // Heuristic: If definition contains "FOR JSON" and ResultSets has exactly one placeholder (empty columns, ExecSource set, ReturnsJson=false) -> attempt reparse.
                // If reparse yields no JSON sets: add a minimal synthetic empty JSON set to preserve structure for downstream generation.
                // Removed: automatic placeholder reparse and synthetic JSON set generation.
                // AST-only mode: produce optional logging when potential missed JSON sets are detected.
                try
                {
                    bool enableLegacyPlaceholderReparse = EnvironmentHelper.IsTrue("XTRAQ_JSON_PLACEHOLDER_REPARSE");
                    foreach (var schema in schemas)
                    {
                        foreach (var proc in schema.StoredProcedures ?? Enumerable.Empty<StoredProcedureModel>())
                        {
                            var content = proc.Content;
                            if (content?.ResultSets == null) continue;
                            if (content.ResultSets.Count != 1) continue;
                            var rs0 = content.ResultSets[0];
                            bool isExecPlaceholderOnly = !rs0.ReturnsJson && !rs0.ReturnsJsonArray && !string.IsNullOrEmpty(rs0.ExecSourceProcedureName) && (rs0.Columns == null || rs0.Columns.Count == 0);
                            if (!isExecPlaceholderOnly) continue;
                            var def = content.Definition;
                            if (string.IsNullOrWhiteSpace(def)) continue;
                            if (def.IndexOf("FOR JSON", StringComparison.OrdinalIgnoreCase) < 0) continue;
                            if (enableLegacyPlaceholderReparse)
                            {
                                StoredProcedureContentModel? reparsed = null;
                                try { reparsed = StoredProcedureContentModel.Parse(def, proc.SchemaName); } catch { }
                                var jsonSets = reparsed?.ResultSets?.Where(r => r.ReturnsJson)?.ToList();
                                if (jsonSets != null && jsonSets.Count > 0)
                                {
                                    var newSets = new List<StoredProcedureContentModel.ResultSet> { rs0 };
                                    newSets.AddRange(jsonSets);
                                    proc.Content = new StoredProcedureContentModel
                                    {
                                        Definition = content.Definition,
                                        Statements = content.Statements,
                                        ContainsSelect = content.ContainsSelect,
                                        ContainsInsert = content.ContainsInsert,
                                        ContainsUpdate = content.ContainsUpdate,
                                        ContainsDelete = content.ContainsDelete,
                                        ContainsMerge = content.ContainsMerge,
                                        ContainsOpenJson = content.ContainsOpenJson,
                                        ResultSets = newSets,
                                        UsedFallbackParser = content.UsedFallbackParser,
                                        ParseErrorCount = content.ParseErrorCount,
                                        FirstParseError = content.FirstParseError,
                                        ExecutedProcedures = content.ExecutedProcedures,
                                        ContainsExecKeyword = content.ContainsExecKeyword,
                                        RawExecCandidates = content.RawExecCandidates,
                                        RawExecCandidateKinds = content.RawExecCandidateKinds
                                    };
                                    consoleService.Verbose($"[proc-fixup-json-reparse] {proc.SchemaName}.{proc.Name} added {jsonSets.Count} JSON set(s) (legacy mode)");
                                    continue;
                                }
                            }
                            // Emit diagnostics only (no synthetic reconstruction anymore)
                            if (ShouldDiagJsonMissAst())
                            {
                                consoleService.Output($"[proc-json-miss-ast] {proc.SchemaName}.{proc.Name} placeholder-only EXEC with FOR JSON detected but no AST JSON set (legacy reparse disabled)");
                            }
                        }
                    }
                }
                catch (Exception jsonMissEx)
                {
                    consoleService.Verbose($"[proc-json-miss-ast-warn] {jsonMissEx.Message}");
                }

                var expandedSnapshot = expandedSnapshotService.LoadExpanded();
                if (expandedSnapshot?.Schemas != null)
                {
                    foreach (var s in expandedSnapshot.Schemas)
                    {
                        if (!string.IsNullOrWhiteSpace(s.Name))
                        {
                            knownSchemas.Add(s.Name);
                        }
                    }
                }
            }
            catch { /* best effort */ }

            bool addedNewIgnored = false;
            var initialIgnoredSet = new HashSet<string>(ignored, StringComparer.OrdinalIgnoreCase); // track originally ignored for delta detection
            var autoAddedIgnored = new List<string>();

            foreach (var schema in schemas)
            {
                var isExplicitlyIgnored = ignored.Contains(schema.Name, StringComparer.OrdinalIgnoreCase);
                var isKnown = knownSchemas.Contains(schema.Name);

                if (defaultStatus == SchemaStatusEnum.Ignore)
                {
                    // FIRST RUN (no snapshot): do NOT auto-extend IgnoredSchemas.
                    if (knownSchemas.Count == 0)
                    {
                        schema.Status = isExplicitlyIgnored ? SchemaStatusEnum.Ignore : SchemaStatusEnum.Build;
                        continue;
                    }

                    // Subsequent runs: only truly new (unknown) schemas become auto-ignored.
                    if (isExplicitlyIgnored)
                    {
                        schema.Status = SchemaStatusEnum.Ignore;
                    }
                    else if (!isKnown)
                    {
                        schema.Status = SchemaStatusEnum.Ignore;
                        if (!ignored.Contains(schema.Name, StringComparer.OrdinalIgnoreCase))
                        {
                            ignored.Add(schema.Name);
                            autoAddedIgnored.Add(schema.Name);
                            addedNewIgnored = true;
                        }
                    }
                    else
                    {
                        schema.Status = SchemaStatusEnum.Build;
                    }
                }
                else
                {
                    schema.Status = defaultStatus;
                    if (isExplicitlyIgnored)
                    {
                        schema.Status = SchemaStatusEnum.Ignore;
                    }
                }
            }

            // Update IgnoredSchemas in config (in-memory only here; persistence handled by caller)
            if (addedNewIgnored)
            {
                // Ensure list stays de-duplicated and sorted
                config.Project.IgnoredSchemas = ignored.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToList();
                consoleService.Verbose($"[ignore] Auto-added {autoAddedIgnored.Count} new schema(s) to IgnoredSchemas (default=Ignore)");
            }

            // Bootstrap heuristic removed: on first run all non-explicitly ignored schemas are built.

            if (ignored.Count > 0)
            {
                consoleService.Verbose($"[ignore] Applied IgnoredSchemas list ({ignored.Count}) (default={defaultStatus})");
            }
        }

        // If both legacy and IgnoredSchemas exist (edge case during migration), let IgnoredSchemas override
        if (config?.Schema != null && config.Project?.IgnoredSchemas?.Any() == true)
        {
            foreach (var schema in schemas)
            {
                if (config.Project.IgnoredSchemas.Contains(schema.Name, StringComparer.OrdinalIgnoreCase))
                {
                    schema.Status = SchemaStatusEnum.Ignore;
                }
            }
            consoleService.Verbose($"[ignore] IgnoredSchemas override applied ({config.Project.IgnoredSchemas.Count})");
        }

        // Reorder: ignored first (kept for legacy ordering expectations)
        schemas = schemas.OrderByDescending(static schema => schema.Status).ToList();

        var activeSchemas = schemas.Where(i => i.Status != SchemaStatusEnum.Ignore).ToList();
        // Build list only for later filtering/persistence logic; we enumerate procedures for all schemas unfiltered.
        var storedProcedures = await dbContext.StoredProcedureListAsync(string.Empty, cancellationToken) ?? new List<StoredProcedure>();
        var schemaListString = string.Join(',', activeSchemas.Select(i => $"'{i.Name}'"));

        // Apply IgnoredProcedures filter (schema.name) early
        var ignoredProcedures = config?.Project?.IgnoredProcedures ?? new List<string>();
        var jsonTypeLogLevel = config?.Project?.JsonTypeLogLevel ?? JsonTypeLogLevel.Detailed;
        if (ignoredProcedures.Count > 0)
        {
            var ignoredSet = new HashSet<string>(ignoredProcedures, StringComparer.OrdinalIgnoreCase);
            var beforeCount = storedProcedures.Count;
            storedProcedures = storedProcedures.Where(sp => !ignoredSet.Contains($"{sp.SchemaName}.{sp.Name}")).ToList();
            var removed = beforeCount - storedProcedures.Count;
            if (removed > 0)
            {
                consoleService.Verbose($"[ignore-proc] Filtered {removed} procedure(s) via IgnoredProcedures list");
            }
        }

        // Apply --procedure flag filtering for schema snapshots (snapshot command only)
        var buildProcedures = Environment.GetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES");
        bool hasProcedureFilter = false;
        var procedureFilterExact = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var procedureFilterWildcard = new List<Regex>();
        if (!string.IsNullOrWhiteSpace(buildProcedures))
        {
            var tokens = buildProcedures.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                                        .Select(p => p.Trim())
                                        .Where(p => !string.IsNullOrEmpty(p))
                                        .ToList();

            foreach (var t in tokens)
            {
                if (t.Contains('*') || t.Contains('?'))
                {
                    // Convert wildcard to Regex: escape, then replace \* -> .*, \? -> .
                    var escaped = Regex.Escape(t);
                    var pattern = "^" + escaped.Replace("\\*", ".*").Replace("\\?", ".") + "$";
                    try { procedureFilterWildcard.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)); } catch { }
                }
                else
                {
                    procedureFilterExact.Add(t);
                }
            }

            hasProcedureFilter = (procedureFilterExact.Count + procedureFilterWildcard.Count) > 0;
            if (hasProcedureFilter)
            {
                var beforeCount = storedProcedures.Count;
                bool Matches(string fq)
                {
                    if (procedureFilterExact.Contains(fq)) return true;
                    if (procedureFilterWildcard.Count == 0) return false;
                    foreach (var rx in procedureFilterWildcard) { if (rx.IsMatch(fq)) return true; }
                    return false;
                }
                storedProcedures = storedProcedures.Where(sp => Matches($"{sp.SchemaName}.{sp.Name}")).ToList();
                var kept = storedProcedures.Count;
                if (kept != beforeCount)
                {
                    consoleService.Verbose($"[procedure-filter] Filtered to {kept} procedure(s) via --procedure flag (was {beforeCount})");
                }
            }
        }

        // Build a simple fingerprint (avoid secrets): use output namespace or role kind + schemas + SP count
        var projectId = config?.Project?.Output?.Namespace ?? "UnknownProject";
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
                if (hasProcedureFilter)
                {
                    var fq = $"{storedProcedure.SchemaName}.{storedProcedure.Name}";
                    bool Matches(string name)
                    {
                        if (procedureFilterExact != null && procedureFilterExact.Contains(name)) return true;
                        if (procedureFilterWildcard != null)
                        {
                            foreach (var rx in procedureFilterWildcard) { if (rx.IsMatch(name)) return true; }
                        }
                        return false;
                    }
                    if (Matches(fq))
                    {
                        if (canSkipDetails && jsonTypeLogLevel == JsonTypeLogLevel.Detailed)
                            consoleService.Verbose($"[cache] Re-parse forced for {fq} due to --procedure flag");
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

                                var (schema, name) = SplitTypeRef(c.TypeRef);
                                var isSystemType = IsSystemSchema(schema);

                                var column = new StoredProcedureContentModel.ResultColumn
                                {
                                    Name = c.Name,
                                    SqlTypeName = BuildSqlTypeName(schema, name),
                                    IsNullable = c.IsNullable,
                                    MaxLength = c.MaxLength,
                                    IsNestedJson = c.IsNestedJson,
                                    ReturnsJson = c.ReturnsJson,
                                    ReturnsJsonArray = c.ReturnsJsonArray,
                                    JsonRootProperty = c.JsonRootProperty,
                                    DeferredJsonExpansion = c.DeferredJsonExpansion
                                };

                                if (!isSystemType)
                                {
                                    column.UserTypeSchemaName = schema;
                                    column.UserTypeName = name;
                                }

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
                            if (EnvironmentHelper.IsTrue("XTRAQ_JSON_LEGACY_SINGLE"))
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

