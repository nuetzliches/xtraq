using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.Data;
using Xtraq.Data.Queries;

namespace Xtraq.Services;

/// <summary>
/// Collects function metadata (scalar + TVF) and populates the SchemaSnapshot.Functions list.
/// </summary>
internal sealed class FunctionSnapshotCollector
{
    private readonly DbContext _db;
    private readonly SchemaSnapshotFileLayoutService _layoutService;
    private readonly IConsoleService _console;

    internal FunctionSnapshotCollector(DbContext db, SchemaSnapshotFileLayoutService layoutService, IConsoleService console)
    {
        _db = db;
        _layoutService = layoutService;
        _console = console;
    }

    internal async Task CollectAsync(SchemaSnapshot snapshot, CancellationToken ct = default)
    {
        if (snapshot == null) return;
        try
        {
            var fnRows = await _db.FunctionListAsync(ct).ConfigureAwait(false);
            var paramRows = await _db.FunctionParametersAsync(ct).ConfigureAwait(false);
            var colRows = await _db.FunctionTvfColumnsAsync(ct).ConfigureAwait(false);
            var depRows = await _db.FunctionDependenciesAsync(ct).ConfigureAwait(false);
            if (fnRows == null || fnRows.Count == 0)
            {
                _console.Verbose($"[fn-empty] functions=0 params={(paramRows?.Count ?? 0)} tvfcols={(colRows?.Count ?? 0)}");
                return;
            }
            _console.Verbose($"[fn-raw] functions={fnRows.Count} params={(paramRows?.Count ?? 0)} tvfcols={(colRows?.Count ?? 0)} deps={(depRows?.Count ?? 0)}");

            // Row models dynamic: we rely on dynamic object with properties matching aliases
            var functions = new List<SnapshotFunction>();
            var userTypeNullability = BuildUserTypeNullabilityMap(snapshot);
            foreach (var r in fnRows)
            {
                if (r == null)
                {
                    continue;
                }

                string schema = r.schema_name ?? string.Empty;
                string name = r.function_name ?? string.Empty;
                string typeCode = r.type_code ?? string.Empty; // FN / IF / TF
                string definition = r.definition ?? string.Empty;
                int objectId = r.object_id;
                bool isTableValued = string.Equals(typeCode, "IF", StringComparison.OrdinalIgnoreCase) || string.Equals(typeCode, "TF", StringComparison.OrdinalIgnoreCase);

                bool encrypted = string.IsNullOrWhiteSpace(definition);
                if (encrypted)
                {
                    if (!string.IsNullOrWhiteSpace(schema) && !string.IsNullOrWhiteSpace(name))
                    {
                        _console.Verbose($"[fn-encrypted] {schema}.{name}");
                    }
                }

                // JSON flags now determined exclusively via AST (regex fallback removed).

                // ReturnSqlType initially empty; will be reconstructed later from first parameter (return parameter) or via DECLARE/RETURNS parsing
                var fnModel = new SnapshotFunction
                {
                    Schema = schema,
                    Name = name,
                    IsTableValued = isTableValued ? true : null, // false will be pruned later
                    ReturnSqlType = string.Empty,
                    ReturnsJson = null,
                    ReturnsJsonArray = null,
                    JsonRootProperty = null,
                    IsEncrypted = encrypted ? true : null
                };

                // JSON columns for scalar functions: exclusively AST (JsonFunctionAstExtractor)
                if (!isTableValued && !encrypted && !string.IsNullOrWhiteSpace(definition))
                {
                    List<SnapshotFunctionColumn>? cols = null;
                    try
                    {
                        var ast = new JsonFunctionAstExtractor().Parse(definition);
                        if (ast.ReturnsJson)
                        {
                            // Override Array/Root based on AST if better determined
                            fnModel.ReturnsJson = true;
                            fnModel.ReturnsJsonArray = ast.ReturnsJsonArray;
                            if (!string.IsNullOrWhiteSpace(ast.JsonRoot)) fnModel.JsonRootProperty = ast.JsonRoot;
                            if (ast.IncludeNullValues) fnModel.JsonIncludeNullValues = true;
                            if (ast.Columns.Count > 0)
                            {
                                SnapshotFunctionColumn Map(JsonFunctionAstColumn c)
                                {
                                    // No heuristics: only take structure, leave type open (or 'json' for nested structures).
                                    var mapped = new SnapshotFunctionColumn
                                    {
                                        Name = c.Name,
                                        IsNullable = null,
                                        MaxLength = null,
                                        IsNestedJson = c.IsNestedJson ? true : null,
                                        ReturnsJson = c.ReturnsJson ? true : null,
                                        ReturnsJsonArray = c.ReturnsJsonArray,
                                        JsonIncludeNullValues = c.JsonIncludeNullValues == true ? true : null,
                                        Columns = new List<SnapshotFunctionColumn>()
                                    };
                                    if (c.Children != null && c.Children.Count > 0)
                                    {
                                        foreach (var child in c.Children)
                                        {
                                            mapped.Columns.Add(Map(child));
                                        }
                                    }
                                    return mapped;
                                }
                                cols = ast.Columns.Select(Map).ToList();
                            }
                        }
                    }
                    catch (Exception astEx)
                    {
                        _console.Verbose($"[fn-ast-error] {schema}.{name} {astEx.Message}");
                    }
                    // No regex fallback: leave it null when the AST does not describe a JSON shape
                    if (cols != null && cols.Count > 0)
                    {
                        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        var finalList = new List<SnapshotFunctionColumn>();
                        foreach (var col in cols)
                        {
                            var baseName = col.Name;
                            var attempt = baseName;
                            int suffix = 1;
                            while (seen.Contains(attempt))
                            {
                                attempt = baseName + suffix.ToString();
                                suffix++;
                            }
                            col.Name = attempt;
                            seen.Add(attempt);
                            finalList.Add(col);
                        }
                        fnModel.Columns = finalList;
                        _console.Verbose($"[fn-json-cols] {schema}.{name} count={finalList.Count} source=ast");
                    }
                    else
                    {
                        _console.Verbose($"[fn-json-cols-empty] {schema}.{name}");
                    }
                }

                // AST-based enhancement of precise return type from RETURNS (purely from definition, no DB)
                if (!isTableValued && !encrypted && !string.IsNullOrWhiteSpace(definition))
                {
                    try
                    {
                        var parsedReturn = TryParseScalarFunctionReturn(definition);
                        if (!string.IsNullOrWhiteSpace(parsedReturn))
                        {
                            if (string.IsNullOrWhiteSpace(fnModel.ReturnSqlType)
                                || fnModel.ReturnSqlType.Equals("decimal", StringComparison.OrdinalIgnoreCase)
                                || fnModel.ReturnSqlType.Equals("numeric", StringComparison.OrdinalIgnoreCase))
                            {
                                fnModel.ReturnSqlType = parsedReturn;
                            }
                        }
                    }
                    catch { }
                }

                functions.Add(fnModel);
            }

            // Build objectId map for efficient attach

            var idMap = new Dictionary<int, SnapshotFunction>();
            foreach (var fnr in fnRows)
            {
                if (fnr == null)
                {
                    continue;
                }

                int id = fnr.object_id;
                string schema = fnr.schema_name;
                string name = fnr.function_name;
                var fn = functions.FirstOrDefault(f => f.Name.Equals(name, StringComparison.OrdinalIgnoreCase) && f.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase));
                if (fn != null)
                {
                    idMap[id] = fn;
                }
            }
            foreach (var p in paramRows ?? Enumerable.Empty<FunctionParamRow>())
            {
                if (p == null)
                {
                    continue;
                }

                int objectId = p.object_id;
                if (!idMap.TryGetValue(objectId, out var fn))
                {
                    continue;
                }

                int ordinal = p.ordinal;
                var rawNameValue = p.param_name ?? string.Empty;
                string rawName = rawNameValue;
                string name = rawName.TrimStart('@');
                string sqlType = p.system_type_name ?? string.Empty;
                int maxLength = p.normalized_length;
                bool isOutput = p.is_output == 1;
                bool isNullable = p.is_nullable == 1;
                bool? userTypeNullable = p.user_type_is_nullable.HasValue
                    ? p.user_type_is_nullable.Value == 1 ? true : false
                    : null;
                var parameterTypeRef = BuildTypeRef(p.user_type_schema_name, p.user_type_name, p.system_type_name);
                bool? snapshotNullable = null;
                if (!string.IsNullOrWhiteSpace(parameterTypeRef) && userTypeNullability.TryGetValue(parameterTypeRef, out var mappedNullable))
                {
                    snapshotNullable = mappedNullable;
                }
                bool? effectiveNullable;
                if (!isNullable)
                {
                    effectiveNullable = false;
                }
                else if (userTypeNullable.HasValue)
                {
                    effectiveNullable = userTypeNullable.Value;
                }
                else if (snapshotNullable.HasValue)
                {
                    effectiveNullable = snapshotNullable.Value;
                }
                else
                {
                    effectiveNullable = true;
                }
                bool hasDefault = p.has_default_value == 1;

                var rawTableTypeRef = TableTypeRefFormatter.Combine(p.user_type_schema_name, p.user_type_name);
                var normalizedTableTypeRef = TableTypeRefFormatter.Normalize(rawTableTypeRef);
                var (tableTypeCatalog, tableTypeSchema, tableTypeName) = TableTypeRefFormatter.Split(normalizedTableTypeRef);

                // Recognize return parameter: For scalar functions, SQL Server provides a parameter without name ("" or null) as return pseudo-parameter
                if (!(fn.IsTableValued ?? false) && (string.IsNullOrWhiteSpace(name)))
                {
                    fn.ReturnSqlType = sqlType;
                    fn.ReturnMaxLength = maxLength > 0 ? maxLength : null;
                    if (effectiveNullable.HasValue)
                    {
                        fn.ReturnIsNullable = effectiveNullable.Value;
                    }
                    continue; // don't include as regular parameter
                }

                bool? finalNullable = effectiveNullable;

                fn.Parameters.Add(new SnapshotFunctionParameter
                {
                    Name = name,
                    TableTypeRef = normalizedTableTypeRef,
                    TableTypeCatalog = tableTypeCatalog,
                    TableTypeSchema = tableTypeSchema,
                    TableTypeName = tableTypeName,
                    TypeRef = parameterTypeRef,
                    IsOutput = null, // pruned (immer false)
                    IsNullable = finalNullable,
                    MaxLength = maxLength > 0 ? maxLength : null,
                    HasDefaultValue = hasDefault ? true : null
                });
            }

            // TVF columns
            foreach (var c in colRows ?? Enumerable.Empty<FunctionColumnRow>())
            {
                if (c == null)
                {
                    continue;
                }

                int objectId = c.object_id;
                if (!idMap.TryGetValue(objectId, out var fn)) continue;
                if (!(fn.IsTableValued ?? false)) continue;
                string name = c.column_name ?? string.Empty;
                string sqlType = c.system_type_name ?? string.Empty;
                bool isNullable = c.is_nullable == 1;
                int maxLength = c.normalized_length;
                // Collision handling (Name, Name1, Name2 ...)
                fn.Columns ??= new List<SnapshotFunctionColumn>();
                var existingNames = new HashSet<string>(fn.Columns.Select(x => x.Name), StringComparer.OrdinalIgnoreCase);
                string finalName = name;
                if (existingNames.Contains(finalName))
                {
                    int suffix = 1;
                    while (existingNames.Contains(finalName))
                    {
                        finalName = name + suffix.ToString();
                        suffix++;
                    }
                }
                var columnTypeRef = BuildTypeRef(c.user_type_schema_name, c.user_type_name, c.base_type_name);

                fn.Columns.Add(new SnapshotFunctionColumn
                {
                    Name = finalName,
                    TypeRef = columnTypeRef,
                    IsNullable = isNullable ? true : null,
                    MaxLength = maxLength > 0 ? maxLength : null
                });
            }

            // Dependencies (direct) mapping
            if (depRows != null && depRows.Count > 0)
            {
                // Build quick lookup objectId -> (schema,name)
                var metaById = fnRows
                    .Where(r => r != null)
                    .ToDictionary(r => r.object_id, r => (schema: (string)r.schema_name, name: (string)r.function_name));
                var depsGrouped = depRows.GroupBy(d => d.referencing_id);
                foreach (var grp in depsGrouped)
                {
                    if (!idMap.TryGetValue(grp.Key, out var fn)) continue;
                    var list = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var dr in grp)
                    {
                        if (dr.referenced_id == grp.Key) continue; // self-reference skip
                        if (!metaById.TryGetValue(dr.referenced_id, out var target)) continue;
                        list.Add(target.schema + "." + target.name);
                    }
                    if (list.Count > 0)
                    {
                        fn.Dependencies = list.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
                    }
                    else
                    {
                        fn.Dependencies = new List<string>(); // will serialize as [] (decide if prune)
                    }
                }
                _console.Verbose($"[fn-deps] functions_with_deps={snapshot.Functions.Count(f => f.Dependencies != null && f.Dependencies.Count > 0)}");
            }

            // Sorting and version tag
            snapshot.FunctionsVersion = 2; // Nested JSON / extended model
            // Enrichment: Type inference for JSON columns from parameter and table metadata (only when no TypeRef present)
            try
            {
                // Build fast lookup maps for parameters per function (use local 'functions' list, not already existing snapshot.Functions)
                var paramLookup = functions.ToDictionary(f => f.Schema + "." + f.Name, f => f.Parameters.ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
                // Table Lookup: Schema.Table -> Columns (Name->(TypeRef, IsNullable, MaxLength))
                var tableLookup = new Dictionary<string, Dictionary<string, (string TypeRef, bool? IsNullable, int? MaxLength)>>(StringComparer.OrdinalIgnoreCase);
                if (snapshot.Tables != null)
                {
                    foreach (var t in snapshot.Tables)
                    {
                        var key = (t.Schema + "." + t.Name);
                        var colMap = new Dictionary<string, (string, bool?, int?)>(StringComparer.OrdinalIgnoreCase);
                        foreach (var c in t.Columns ?? new List<SnapshotTableColumn>())
                        {
                            if (!string.IsNullOrWhiteSpace(c.Name) && !string.IsNullOrWhiteSpace(c.TypeRef))
                            {
                                colMap[c.Name] = (c.TypeRef!, c.IsNullable, c.MaxLength);
                            }
                        }
                        tableLookup[key] = colMap;
                    }
                }
                foreach (var f in functions.Where(fn => fn.ReturnsJson == true && fn.Columns != null && fn.Columns.Count > 0))
                {
                    var fnKey = f.Schema + "." + f.Name;
                    paramLookup.TryGetValue(fnKey, out var fnParams);
                    foreach (var col in f.Columns ?? Enumerable.Empty<SnapshotFunctionColumn>())
                    {
                        EnrichColumnRecursive(f, col, fnParams, tableLookup);
                    }
                }
            }
            catch (Exception enrichEx)
            {
                _console.Verbose($"[fn-json-enrich-error] {enrichEx.Message}");
            }
            // Prune empty dependency lists for cleaner JSON (set null so JsonIgnoreCondition prunes)
            foreach (var f in functions)
            {
                if (f.Dependencies != null && f.Dependencies.Count == 0) f.Dependencies = null;
            }

            snapshot.Functions = functions
                .OrderBy(f => f.Schema, StringComparer.OrdinalIgnoreCase)
                .ThenBy(f => f.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            _console.Verbose($"[fn-collect] loaded={snapshot.Functions.Count}");
        }
        catch (Exception ex)
        {
            _console.Warn($"[fn-collect-error] {ex.Message}");
        }
    }

    // Note: All regex-based JSON fallback heuristics removed (InferJsonColumns & related methods).

    private void EnrichColumnRecursive(SnapshotFunction fn, SnapshotFunctionColumn col, Dictionary<string, SnapshotFunctionParameter>? paramMap,
        Dictionary<string, Dictionary<string, (string TypeRef, bool? IsNullable, int? MaxLength)>> tableLookup)
    {
        if (col == null)
        {
            return;
        }

        // Skip when a concrete type already exists and is not the JSON container marker
        if (!string.IsNullOrWhiteSpace(col.TypeRef))
        {
            if (col.Columns != null && col.Columns.Count > 0)
            {
                foreach (var child in col.Columns)
                    EnrichColumnRecursive(fn, child, paramMap, tableLookup);
            }
            return;
        }
        var columnName = col.Name;
        if (string.IsNullOrWhiteSpace(columnName))
        {
            if (col.Columns != null && col.Columns.Count > 0)
            {
                foreach (var child in col.Columns)
                {
                    EnrichColumnRecursive(fn, child, paramMap, tableLookup);
                }
            }

            return;
        }

        var segments = columnName.Split('.', StringSplitOptions.RemoveEmptyEntries);
        var leaf = segments.LastOrDefault() ?? columnName;
        SnapshotFunctionParameter? matchedParam = null;
        if (paramMap != null)
        {
            if (paramMap.TryGetValue(leaf, out var direct))
            {
                matchedParam = direct;
            }
            else
            {
                var candidates = paramMap.Values
                    .Where(p => p.Name.EndsWith(leaf, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (candidates.Count == 1)
                {
                    matchedParam = candidates[0];
                }
                else if (candidates.Count > 1)
                {
                    var prefixSegments = segments.Length > 1
                        ? segments.Take(segments.Length - 1).ToArray()
                        : Array.Empty<string>();
                    SnapshotFunctionParameter? best = null;
                    var bestScore = int.MinValue;
                    foreach (var candidate in candidates)
                    {
                        var score = 0;
                        if (prefixSegments.Length > 0)
                        {
                            for (int i = 0; i < prefixSegments.Length; i++)
                            {
                                var segment = prefixSegments[i];
                                if (string.IsNullOrWhiteSpace(segment)) continue;
                                if (candidate.Name.IndexOf(segment, StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    score += (prefixSegments.Length - i) * 10;
                                }
                            }
                        }
                        if (!string.IsNullOrWhiteSpace(candidate.TypeRef)) score++;
                        if (best == null || score > bestScore)
                        {
                            best = candidate;
                            bestScore = score;
                        }
                    }
                    matchedParam = best ?? candidates[0];
                }
            }
        }
        if (matchedParam != null && !string.IsNullOrWhiteSpace(matchedParam.TypeRef))
        {
            col.TypeRef = matchedParam.TypeRef;
            col.IsNullable = matchedParam.IsNullable;
            col.MaxLength = matchedParam.MaxLength;
        }
        else
        {
            // Additional mapping: route the dateTime alias to CreatedDt or UpdatedDt based on the first segment
            if (leaf.Equals("dateTime", StringComparison.OrdinalIgnoreCase) && paramMap != null)
            {
                string paramAlias = segments.Length > 0 && segments[0].Equals("updated", StringComparison.OrdinalIgnoreCase) ? "UpdatedDt" : "CreatedDt";
                if (paramMap.TryGetValue(paramAlias, out var dtParam) && !string.IsNullOrWhiteSpace(dtParam.TypeRef))
                {
                    col.TypeRef = dtParam.TypeRef;
                    col.IsNullable = dtParam.IsNullable;
                    col.MaxLength = dtParam.MaxLength;
                }
            }
            // DisplayName / Initials / userId / rowVersion mapping
            if (string.IsNullOrWhiteSpace(col.TypeRef))
            {
                if (leaf.Equals("displayName", StringComparison.OrdinalIgnoreCase))
                {
                    TryMapTableColumn("identity.User", "DisplayName", col, tableLookup); // direct column name when present
                    if (string.IsNullOrWhiteSpace(col.TypeRef))
                    {
                        TryMapTableColumn("identity.User", "UserName", col, tableLookup); // fallback column
                    }
                }
                else if (leaf.Equals("initials", StringComparison.OrdinalIgnoreCase))
                {
                    TryMapTableColumn("identity.User", "Initials", col, tableLookup);
                }
                else if (leaf.Equals("userId", StringComparison.OrdinalIgnoreCase))
                {
                    TryMapTableColumn("identity.User", "UserId", col, tableLookup);
                }
                else if (leaf.Equals("rowVersion", StringComparison.OrdinalIgnoreCase))
                {
                    // Try binding to the actual table column (identity.User.RowVersion) and reuse its type metadata
                    TryMapTableColumn("identity.User", "RowVersion", col, tableLookup);
                    // Fallback when no mapping is found: use the canonical SQL type name 'rowversion'
                    if (string.IsNullOrWhiteSpace(col.TypeRef)) col.TypeRef = CombineTypeRef("sys", "rowversion");
                }
            }
            // Generic fallback: identity.User.<leaf>
            if (string.IsNullOrWhiteSpace(col.TypeRef))
            {
                TryMapTableColumn("identity.User", leaf, col, tableLookup);
            }
        }
        if (col.Columns != null && col.Columns.Count > 0)
        {
            foreach (var child in col.Columns)
                EnrichColumnRecursive(fn, child, paramMap, tableLookup);
        }
    }

    private void TryMapTableColumn(string tableKey, string columnName, SnapshotFunctionColumn target,
        Dictionary<string, Dictionary<string, (string TypeRef, bool? IsNullable, int? MaxLength)>> tableLookup)
    {
        if (string.IsNullOrWhiteSpace(columnName) || target == null)
        {
            return;
        }

        if (tableLookup.TryGetValue(tableKey, out var cols) && cols.TryGetValue(columnName, out var meta))
        {
            if (string.IsNullOrWhiteSpace(target.TypeRef)) target.TypeRef = meta.TypeRef;
            if (!target.IsNullable.HasValue) target.IsNullable = meta.IsNullable;
            if (!target.MaxLength.HasValue) target.MaxLength = meta.MaxLength;
        }
    }

    private static Dictionary<string, bool?> BuildUserTypeNullabilityMap(SchemaSnapshot snapshot)
    {
        var map = new Dictionary<string, bool?>(StringComparer.OrdinalIgnoreCase);
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

            var schema = udt.Schema?.Trim();
            var name = udt.Name?.Trim();
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            var key = string.Concat(schema, ".", name);
            map[key] = udt.IsNullable;

            if (!string.IsNullOrWhiteSpace(udt.Catalog))
            {
                var catalogKey = string.Concat(udt.Catalog.Trim(), ".", key);
                map[catalogKey] = udt.IsNullable;
            }
        }

        return map;
    }

    private static string TryParseScalarFunctionReturn(string definition)
    {
        try
        {
            var parser = new TSql160Parser(false);
            using var reader = new System.IO.StringReader(definition);
            var fragment = parser.Parse(reader, out var errors);
            if (fragment == null) return string.Empty;
            ScalarFunctionReturnType? ret = null;
            fragment.Accept(new ReturnTypeVisitor(rt => ret = rt));
            if (ret?.DataType is DataTypeReference dt)
            {
                string baseName = string.Join('.', dt.Name?.Identifiers?.Select(i => i.Value) ?? Array.Empty<string>()).ToLowerInvariant();
                if (string.IsNullOrWhiteSpace(baseName)) return string.Empty;
                if (dt is SqlDataTypeReference sqlRef)
                {
                    // decimal/numeric(precision, scale)
                    if (sqlRef.SqlDataTypeOption is SqlDataTypeOption.Decimal or SqlDataTypeOption.Numeric)
                    {
                        int? p = null; int? s = null;
                        if (sqlRef.Parameters?.Count >= 1 && sqlRef.Parameters[0] is Literal l0 && int.TryParse(l0.Value, out var p0)) p = p0;
                        if (sqlRef.Parameters?.Count >= 2 && sqlRef.Parameters[1] is Literal l1 && int.TryParse(l1.Value, out var s0)) s = s0;
                        if (p.HasValue && s.HasValue) return $"{baseName}({p.Value},{s.Value})";
                    }
                    // (n)varchar / varbinary(length)
                    if (sqlRef.SqlDataTypeOption is SqlDataTypeOption.VarChar or SqlDataTypeOption.NVarChar or SqlDataTypeOption.VarBinary)
                    {
                        if (sqlRef.Parameters?.Count >= 1 && sqlRef.Parameters[0] is Literal ll && int.TryParse(ll.Value, out var len))
                            return $"{baseName}({len})";
                    }
                }
                else if (dt is ParameterizedDataTypeReference pr)
                {
                    if (pr.Parameters?.Count == 1 && pr.Parameters[0] is Literal lz && int.TryParse(lz.Value, out var lnum))
                        return $"{baseName}({lnum})";
                    if (pr.Parameters?.Count >= 2 && pr.Parameters[0] is Literal lp && pr.Parameters[1] is Literal ls
                        && int.TryParse(lp.Value, out var pp) && int.TryParse(ls.Value, out var ss))
                        return $"{baseName}({pp},{ss})";
                }
                return baseName;
            }
        }
        catch { }
        return string.Empty;
    }

    private static string? BuildTypeRef(string? userTypeSchema, string? userTypeName, string? baseTypeName)
    {
        if (!string.IsNullOrWhiteSpace(userTypeSchema) && !string.IsNullOrWhiteSpace(userTypeName))
        {
            return CombineTypeRef(userTypeSchema, userTypeName);
        }

        var normalizedBase = NormalizeSqlTypeName(baseTypeName);
        if (!string.IsNullOrWhiteSpace(normalizedBase))
        {
            return CombineTypeRef("sys", normalizedBase);
        }

        return null;
    }

    private static string? CombineTypeRef(string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return string.Concat(schema.Trim(), ".", name.Trim());
    }

    private static string? NormalizeSqlTypeName(string? sqlTypeName)
    {
        if (string.IsNullOrWhiteSpace(sqlTypeName))
        {
            return null;
        }

        return sqlTypeName.Trim().ToLowerInvariant();
    }

    private sealed class ReturnTypeVisitor : TSqlFragmentVisitor
    {
        private readonly Action<ScalarFunctionReturnType> _on;
        public ReturnTypeVisitor(Action<ScalarFunctionReturnType> on) { _on = on; }
        public override void Visit(TSqlFragment node)
        {
            if (node is CreateFunctionStatement cfs && cfs.ReturnType is ScalarFunctionReturnType srt) _on(srt);
            base.Visit(node);
        }
    }
}

