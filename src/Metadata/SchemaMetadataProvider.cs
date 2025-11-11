using Xtraq.Models;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Metadata
{

    /// <summary>
    /// Aggregates metadata from the latest snapshot under .xtraq/snapshots for procedures, their inputs, outputs, and result sets.
    /// Provides strongly typed descriptor lists used by generators.
    /// </summary>
    public interface ISchemaMetadataProvider
    {
        /// <summary>
        /// Gets the procedures discovered in the snapshot metadata.
        /// </summary>
        /// <returns>A read-only list of procedure descriptors.</returns>
        IReadOnlyList<ProcedureDescriptor> GetProcedures();

        /// <summary>
        /// Gets the stored procedure input descriptors.
        /// </summary>
        /// <returns>A read-only list of input descriptors.</returns>
        IReadOnlyList<InputDescriptor> GetInputs();

        /// <summary>
        /// Gets the stored procedure output descriptors.
        /// </summary>
        /// <returns>A read-only list of output descriptors.</returns>
        IReadOnlyList<OutputDescriptor> GetOutputs();

        /// <summary>
        /// Gets the stored procedure result set descriptors.</summary>
        /// <returns>A read-only list of result set descriptors.</returns>
        IReadOnlyList<ResultSetDescriptor> GetResultSets();

        /// <summary>
        /// Gets the aggregate result descriptors used by generators.</summary>
        /// <returns>A read-only list of result descriptors.</returns>
        IReadOnlyList<ResultDescriptor> GetResults();

        /// <summary>
        /// Gets function descriptors produced by snapshot analysis.</summary>
        /// <returns>A read-only list of function descriptors.</returns>
        IReadOnlyList<FunctionDescriptor> GetFunctions();

        /// <summary>
        /// Gets JSON function descriptors produced by snapshot analysis.</summary>
        /// <returns>A read-only list of JSON function descriptors.</returns>
        IReadOnlyList<FunctionJsonDescriptor> GetFunctionJsonDescriptors();

        /// <summary>
        /// Attempts to resolve JSON metadata for the specified function.</summary>
        /// <param name="schemaName">The schema that owns the function.</param>
        /// <param name="functionName">The function name.</param>
        /// <returns>The JSON descriptor when available; otherwise <c>null</c>.</returns>
        FunctionJsonDescriptor? TryGetFunctionJsonDescriptor(string schemaName, string functionName);
    }

    /// <summary>
    /// Legacy snapshot metadata provider kept for backward compatibility.
    /// </summary>
    [Obsolete("Use Xtraq.Services.SnapshotSchemaMetadataProvider", false)]
    public class SchemaMetadataProvider : ISchemaMetadataProvider
    {
        private readonly string _projectRoot;
        private readonly object _loadSync = new();
        private volatile bool _loaded;
        private List<ProcedureDescriptor> _procedures = new();
        private List<InputDescriptor> _inputs = new();
        private List<OutputDescriptor> _outputs = new();
        private List<ResultSetDescriptor> _resultSets = new();
        private List<ResultDescriptor> _results = new();
        private List<FunctionDescriptor> _functions = new();
        private ConcurrentDictionary<string, (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames)> _functionJsonSets
            = new(StringComparer.OrdinalIgnoreCase);
        private ConcurrentDictionary<string, FunctionJsonDescriptor> _functionJsonDescriptors
            = new(StringComparer.OrdinalIgnoreCase);
        private static readonly (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames) EmptyFunctionJsonMetadata
            = (false, false, string.Empty, Array.Empty<string>());

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaMetadataProvider"/> class.
        /// </summary>
        /// <param name="projectRoot">Optional project root used to locate snapshot metadata.</param>
        public SchemaMetadataProvider(string? projectRoot = null)
        {
            _projectRoot = string.IsNullOrWhiteSpace(projectRoot) ? Directory.GetCurrentDirectory() : Path.GetFullPath(projectRoot!);
        }

        /// <inheritdoc />
        public IReadOnlyList<ProcedureDescriptor> GetProcedures() { EnsureLoaded(); return _procedures; }

        /// <inheritdoc />
        public IReadOnlyList<InputDescriptor> GetInputs() { EnsureLoaded(); return _inputs; }

        /// <inheritdoc />
        public IReadOnlyList<OutputDescriptor> GetOutputs() { EnsureLoaded(); return _outputs; }

        /// <inheritdoc />
        public IReadOnlyList<ResultSetDescriptor> GetResultSets() { EnsureLoaded(); return _resultSets; }

        /// <inheritdoc />
        public IReadOnlyList<ResultDescriptor> GetResults() { EnsureLoaded(); return _results; }

        /// <inheritdoc />
        public IReadOnlyList<FunctionDescriptor> GetFunctions() { EnsureLoaded(); return _functions; }

        /// <inheritdoc />
        public IReadOnlyList<FunctionJsonDescriptor> GetFunctionJsonDescriptors()
        {
            EnsureLoaded();
            return _functionJsonDescriptors.Values
                .OrderBy(d => d.SchemaName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(d => d.FunctionName, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        /// <inheritdoc />
        public FunctionJsonDescriptor? TryGetFunctionJsonDescriptor(string schemaName, string functionName)
        {
            EnsureLoaded();
            if (string.IsNullOrWhiteSpace(functionName))
            {
                return null;
            }

            var normalizedSchema = string.IsNullOrWhiteSpace(schemaName) ? "dbo" : schemaName.Trim();
            var normalizedName = functionName.Trim();
            if (normalizedName.Length == 0)
            {
                return null;
            }

            var key = string.Concat(normalizedSchema, ".", normalizedName);
            if (_functionJsonDescriptors.TryGetValue(key, out var descriptor))
            {
                return descriptor;
            }

            if (_functionJsonDescriptors.TryGetValue(normalizedName, out descriptor))
            {
                return descriptor;
            }

            return null;
        }

        private void EnsureLoaded()
        {
            if (_loaded)
            {
                return;
            }

            lock (_loadSync)
            {
                if (_loaded)
                {
                    return;
                }

                Load();
                _loaded = true;
            }
        }

        private void Load()
        {
            var schemaDir = Path.Combine(_projectRoot, ".xtraq", "snapshots");
            if (!Directory.Exists(schemaDir)) { SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Info: schema directory not found: {schemaDir}"); return; }
            var indexPath = Path.Combine(schemaDir, "index.json");
            JsonDocument? doc = null;
            bool expanded = false;
            if (File.Exists(indexPath))
            {
                try
                {
                    using var fs = File.OpenRead(indexPath);
                    doc = JsonDocument.Parse(fs);
                    expanded = true;
                    SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Using expanded snapshot index: {indexPath}");
                }
                catch (Exception ex)
                {
                    SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Warning: failed to parse expanded index.json: {ex.Message}");
                    doc = null; // fallback legacy
                }
            }
            if (doc == null)
            {
                // Legacy fallback: pick latest non-index *.json (monolith)
                var files = Directory.GetFiles(schemaDir, "*.json")
                    .Where(f => !string.Equals(Path.GetFileName(f), "index.json", StringComparison.OrdinalIgnoreCase))
                    .ToArray();
                if (files.Length == 0) { SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Info: no legacy snapshot files in {schemaDir}"); return; }
                var ordered = files.Select(f => new FileInfo(f)).OrderByDescending(fi => fi.LastWriteTimeUtc).ToList();
                foreach (var fi in ordered.Take(5))
                    SchemaMetadataProviderLogHelper.TryLog($"[xtraq] legacy snapshot candidate: {fi.FullName} (utc={fi.LastWriteTimeUtc:O} size={fi.Length})");
                var latest = ordered.First();
                SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Using legacy snapshot: {latest.FullName}");
                try
                {
                    using var fs = File.OpenRead(latest.FullName);
                    doc = JsonDocument.Parse(fs);
                }
                catch (Exception ex)
                {
                    SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Warning: failed to parse legacy snapshot {latest.FullName}: {ex.Message}");
                    return;
                }
            }
            if (doc == null) return;
            JsonElement procsEl;
            if (expanded)
            {
                // Expanded index: load procedure files individually - index only contains file hash entries
                if (doc.RootElement.TryGetProperty("Procedures", out var procIndexEl) && procIndexEl.ValueKind == JsonValueKind.Array)
                {
                    var procEntries = procIndexEl.EnumerateArray().Select(e => new
                    {
                        File = e.GetPropertyOrDefault("File"),
                        Name = e.GetPropertyOrDefault("Name"),
                        Schema = e.GetPropertyOrDefault("Schema")
                    }).Where(x => !string.IsNullOrWhiteSpace(x.File)).ToList();
                    var procArray = new List<JsonElement>();
                    foreach (var entry in procEntries)
                    {
                        var path = Path.Combine(schemaDir, "procedures", entry.File!);
                        if (!File.Exists(path)) continue;
                        try
                        {
                            using var pfs = File.OpenRead(path);
                            using var pdoc = JsonDocument.Parse(pfs);
                            procArray.Add(pdoc.RootElement.Clone());
                        }
                        catch (Exception ex)
                        {
                            SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Warning: failed to parse procedure file {path}: {ex.Message}");
                        }
                    }
                    // Build an array JsonElement manually
                    using var tmpDoc = JsonDocument.Parse("[]"); // placeholder
                    // Cannot create a new JsonElement dynamically without Utf8JsonWriter -> convert via re-serialize
                    var procJson = System.Text.Json.JsonSerializer.Serialize(procArray);
                    doc = JsonDocument.Parse(procJson);
                    procsEl = doc.RootElement; // doc.root ist jetzt das Array der Procs
                }
                else
                {
                    SchemaMetadataProviderLogHelper.TryLog("[xtraq] Warning: expanded index.json missing Procedures array");
                    return;
                }
            }
            else
            {
                if (!doc.RootElement.TryGetProperty("Procedures", out procsEl) || procsEl.ValueKind != JsonValueKind.Array)
                {
                    if (!doc.RootElement.TryGetProperty("StoredProcedures", out procsEl) || procsEl.ValueKind != JsonValueKind.Array)
                    {
                        SchemaMetadataProviderLogHelper.TryLog("[xtraq] Warning: snapshot has no 'Procedures' or 'StoredProcedures' array");
                        return;
                    }
                    else
                    {
                        SchemaMetadataProviderLogHelper.TryLog("[xtraq] Info: using legacy 'StoredProcedures' key");
                    }
                }
            }

            var procList = new List<ProcedureDescriptor>();
            var inputList = new List<InputDescriptor>();
            var outputList = new List<OutputDescriptor>();
            var rsList = new List<ResultSetDescriptor>();
            var resultDescriptors = new List<ResultDescriptor>();
            var functionDescriptors = new List<FunctionDescriptor>();
            var functionJsonSets = new ConcurrentDictionary<string, (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames)>(StringComparer.OrdinalIgnoreCase);

            var tableTypeProvider = new TableTypeMetadataProvider(_projectRoot);
            var tableTypeInfos = tableTypeProvider.GetAll();
            var tableTypeRefs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tt in tableTypeInfos)
            {
                if (tt == null || string.IsNullOrWhiteSpace(tt.Schema) || string.IsNullOrWhiteSpace(tt.Name)) continue;
                tableTypeRefs.Add(tt.Schema + "." + tt.Name);
            }
            var typeResolver = new TypeMetadataResolver(_projectRoot);

            foreach (var p in procsEl.EnumerateArray())
            {
                var schema = p.GetPropertyOrDefault("Schema") ?? "dbo";
                var name = p.GetPropertyOrDefault("Name") ?? string.Empty;
                if (string.IsNullOrWhiteSpace(name)) continue;
                var sanitized = NamePolicy.Sanitize(name);
                var operationName = $"{schema}.{sanitized}";
                string? rawSql = p.GetPropertyOrDefault("Sql")
                               ?? p.GetPropertyOrDefault("Definition")
                               ?? p.GetPropertyOrDefault("Body")
                               ?? p.GetPropertyOrDefault("Tsql");

                // Inputs & outputs (output params marked IsOutput or separate array)
                var inputParams = new List<FieldDescriptor>();
                var outputParams = new List<FieldDescriptor>();
                if ((p.TryGetProperty("Parameters", out var inputsEl) && inputsEl.ValueKind == JsonValueKind.Array) ||
                    (p.TryGetProperty("Inputs", out inputsEl) && inputsEl.ValueKind == JsonValueKind.Array))
                {
                    foreach (var ip in inputsEl.EnumerateArray())
                    {
                        var raw = ip.GetPropertyOrDefault("Name") ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(raw)) continue;
                        var clean = raw.TrimStart('@');
                        var typeRef = ip.GetPropertyOrDefault("TypeRef");
                        var maxLen = ip.GetPropertyOrDefaultInt("MaxLength");
                        var precision = ip.GetPropertyOrDefaultInt("Precision");
                        var scale = ip.GetPropertyOrDefaultInt("Scale");
                        var isNullable = ip.GetPropertyOrDefaultBoolStrict("IsNullable");
                        var isOutput = ip.GetPropertyOrDefaultBool("IsOutput");
                        var explicitTableType = ip.GetPropertyOrDefaultBool("IsTableType");
                        var tableTypeRefRaw = ip.GetPropertyOrDefault("TableTypeRef");
                        var normalizedTableTypeRef = TableTypeRefFormatter.Normalize(tableTypeRefRaw);
                        var (_, tableTypeSchemaFromRef, tableTypeNameFromRef) = TableTypeRefFormatter.Split(normalizedTableTypeRef);
                        var legacyTtSchema = ip.GetPropertyOrDefault("TableTypeSchema");
                        var legacyTtName = ip.GetPropertyOrDefault("TableTypeName");

                        var tableTypeSchema = legacyTtSchema ?? tableTypeSchemaFromRef;
                        var tableTypeName = legacyTtName ?? tableTypeNameFromRef;

                        if (string.IsNullOrWhiteSpace(typeRef))
                        {
                            if (!string.IsNullOrWhiteSpace(normalizedTableTypeRef))
                            {
                                typeRef = normalizedTableTypeRef;
                            }
                            else if (!string.IsNullOrWhiteSpace(tableTypeSchema) && !string.IsNullOrWhiteSpace(tableTypeName))
                            {
                                typeRef = tableTypeSchema + "." + tableTypeName;
                            }
                        }

                        var resolved = typeResolver.Resolve(typeRef, maxLen, precision, scale);
                        var sqlType = resolved?.SqlType ?? ip.GetPropertyOrDefault("SqlTypeName") ?? string.Empty;
                        var effectiveMaxLen = resolved?.MaxLength ?? maxLen;
                        bool isTableType = explicitTableType
                            || !string.IsNullOrWhiteSpace(normalizedTableTypeRef)
                            || (!string.IsNullOrWhiteSpace(tableTypeSchema) && !string.IsNullOrWhiteSpace(tableTypeName));

                        if (!isTableType && !string.IsNullOrWhiteSpace(tableTypeSchema) && !string.IsNullOrWhiteSpace(tableTypeName))
                        {
                            isTableType = true;
                        }

                        if (!isTableType && !string.IsNullOrWhiteSpace(typeRef))
                        {
                            if (tableTypeRefs.Contains(typeRef))
                            {
                                isTableType = true;
                            }
                            else
                            {
                                var (schemaFromRef, nameFromRef) = TypeMetadataResolver.SplitTypeRef(typeRef);
                                if (!string.IsNullOrWhiteSpace(schemaFromRef)
                                    && !string.IsNullOrWhiteSpace(nameFromRef)
                                    && !string.Equals(schemaFromRef, "sys", StringComparison.OrdinalIgnoreCase)
                                    && resolved == null)
                                {
                                    isTableType = true;
                                    legacyTtSchema ??= schemaFromRef;
                                    legacyTtName ??= nameFromRef;
                                    tableTypeSchema ??= schemaFromRef;
                                    tableTypeName ??= nameFromRef;
                                }
                            }
                        }

                        string? ttSchema = tableTypeSchema;
                        string? ttName = tableTypeName;
                        if (isTableType)
                        {
                            var split = TypeMetadataResolver.SplitTypeRef(typeRef);
                            if (string.IsNullOrWhiteSpace(ttSchema)) ttSchema = split.Schema ?? schema;
                            if (string.IsNullOrWhiteSpace(ttName)) ttName = split.Name ?? clean;
                        }
                        else if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef))
                        {
                            sqlType = typeRef;
                        }

                        FieldDescriptor fd;
                        if (isTableType && !string.IsNullOrWhiteSpace(ttName))
                        {
                            var pascal = NamePolicy.Sanitize(ttName!);
                            var attrs = new List<string> { "[TableType]" };
                            if (!string.IsNullOrWhiteSpace(ttSchema)) attrs.Add($"[TableTypeSchema({ttSchema})]");
                            var sqlIdentifier = string.IsNullOrWhiteSpace(typeRef) ? ttName! : typeRef!;
                            var clrType = $"IReadOnlyList<{pascal}>?";
                            fd = new FieldDescriptor(clean, NamePolicy.Sanitize(clean), clrType, true, sqlIdentifier, null, Documentation: null, Attributes: attrs);
                        }
                        else
                        {
                            var clr = MapSqlToClr(sqlType, isNullable);
                            fd = new FieldDescriptor(clean, NamePolicy.Sanitize(clean), clr, isNullable, sqlType, effectiveMaxLen);
                        }

                        if (isOutput) outputParams.Add(fd); else inputParams.Add(fd);
                    }
                }
                if (p.TryGetProperty("OutputParameters", out var outsEl) && outsEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var opEl in outsEl.EnumerateArray())
                    {
                        var raw = opEl.GetPropertyOrDefault("Name") ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(raw)) continue;
                        var clean = raw.TrimStart('@');
                        if (outputParams.Any(o => o.Name.Equals(clean, StringComparison.OrdinalIgnoreCase))) continue;
                        var typeRef = opEl.GetPropertyOrDefault("TypeRef");
                        var maxLen = opEl.GetPropertyOrDefaultInt("MaxLength");
                        var precision = opEl.GetPropertyOrDefaultInt("Precision");
                        var scale = opEl.GetPropertyOrDefaultInt("Scale");
                        var resolved = typeResolver.Resolve(typeRef, maxLen, precision, scale);
                        var sqlType = resolved?.SqlType ?? opEl.GetPropertyOrDefault("SqlTypeName") ?? string.Empty;
                        var effectiveMaxLen = resolved?.MaxLength ?? maxLen;
                        var isNullable = opEl.GetPropertyOrDefaultBoolStrict("IsNullable");
                        if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef)) sqlType = typeRef;
                        var clr = MapSqlToClr(sqlType, isNullable);
                        var fd = new FieldDescriptor(clean, NamePolicy.Sanitize(clean), clr, isNullable, sqlType, effectiveMaxLen);
                        outputParams.Add(fd);
                    }
                }

                // Result sets with always-on resolver
                var resultSetDescriptors = new List<ResultSetDescriptor>();
                if (p.TryGetProperty("ResultSets", out var rsEl) && rsEl.ValueKind == JsonValueKind.Array)
                {
                    int idx = 0;
                    var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var rse in rsEl.EnumerateArray())
                    {
                        var columns = new List<FieldDescriptor>();
                        var returnsJsonFlag = rse.GetPropertyOrDefaultBool("ReturnsJson");
                        var returnsJsonArrayExplicit = TryGetOptionalBoolean(rse, "ReturnsJsonArray");
                        var returnsJsonArrayFlag = returnsJsonArrayExplicit ?? (returnsJsonFlag ? true : false);
                        JsonStructureBuilder? jsonStructureBuilder = (returnsJsonFlag || returnsJsonArrayExplicit.HasValue) ? new JsonStructureBuilder() : null;
                        if (rse.TryGetProperty("Columns", out var colsEl) && colsEl.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var c in colsEl.EnumerateArray())
                            {
                                AppendResultColumn(columns, c, null, jsonStructureBuilder);
                            }
                        }
                        var rsName = ResultSetNaming.DeriveName(idx, columns, usedNames);
                        if (!string.IsNullOrWhiteSpace(rawSql))
                        {
                            try
                            {
                                var suggested = ResultSetNameResolver.TryResolve(idx, rawSql!);
                                if (!string.IsNullOrWhiteSpace(suggested) && rsName.StartsWith("ResultSet", StringComparison.OrdinalIgnoreCase))
                                {
                                    var baseNameUnique = NamePolicy.Sanitize(suggested!);
                                    var final = baseNameUnique;
                                    if (usedNames.Contains(final))
                                    {
                                        // Duplicate base table: append incremental numeric suffix (Users, Users1, Users2 ...)
                                        int suffix = 1;
                                        while (usedNames.Contains(final))
                                        {
                                            final = baseNameUnique + suffix.ToString();
                                            suffix++;
                                        }
                                    }
                                    rsName = final;
                                }
                            }
                            catch { /* silent fallback */ }
                        }
                        usedNames.Add(rsName);
                        // Forwarding + diagnostic metadata (optional in snapshot)
                        string? execSourceSchema = null;
                        string? execSourceProc = null;
                        bool hasSelectStar = false;
                        try
                        {
                            execSourceSchema = rse.GetPropertyOrDefault("ExecSourceSchemaName");
                            execSourceProc = rse.GetPropertyOrDefault("ExecSourceProcedureName");
                            var hasStarRaw = rse.GetPropertyOrDefaultBool("HasSelectStar");
                            hasSelectStar = hasStarRaw;
                        }
                        catch { /* best effort; leave null/defaults */ }
                        JsonPayloadDescriptor? jsonPayload = null;
                        var includeNullValuesFlag = TryGetOptionalBoolean(rse, "JsonIncludeNullValues");
                        try
                        {
                            var root = rse.GetPropertyOrDefault("JsonRootProperty");
                            if (returnsJsonFlag || returnsJsonArrayExplicit.HasValue || !string.IsNullOrWhiteSpace(root))
                            {
                                jsonPayload = new JsonPayloadDescriptor(returnsJsonArrayFlag, string.IsNullOrWhiteSpace(root) ? null : root, includeNullValuesFlag == true);
                            }
                        }
                        catch { /* best effort */ }
                        string? procedureRef = null;
                        try
                        {
                            if (rse.TryGetProperty("ProcedureRef", out var procRefEl) && procRefEl.ValueKind == JsonValueKind.String)
                            {
                                procedureRef = procRefEl.GetString();
                            }
                            else if (rse.TryGetProperty("Reference", out var legacyRefEl) && legacyRefEl.ValueKind == JsonValueKind.Object)
                            {
                                var kindLegacy = legacyRefEl.GetPropertyOrDefault("Kind");
                                var schemaLegacy = legacyRefEl.GetPropertyOrDefault("Schema");
                                var nameLegacy = legacyRefEl.GetPropertyOrDefault("Name");
                                if (string.Equals(kindLegacy, "Procedure", StringComparison.OrdinalIgnoreCase))
                                {
                                    procedureRef = ComposeSchemaObjectRef(schemaLegacy, nameLegacy);
                                }
                            }
                        }
                        catch { }
                        if (string.IsNullOrWhiteSpace(procedureRef) && !string.IsNullOrWhiteSpace(execSourceProc))
                        {
                            procedureRef = ComposeSchemaObjectRef(execSourceSchema, execSourceProc);
                        }
                        resultSetDescriptors.Add(new ResultSetDescriptor(
                            Index: idx,
                            Name: rsName,
                            Fields: columns,
                            IsScalar: false,
                            Optional: true,
                            HasSelectStar: hasSelectStar,
                            ExecSourceSchemaName: execSourceSchema,
                            ExecSourceProcedureName: execSourceProc,
                            ProcedureRef: NormalizeSchemaObjectRef(procedureRef),
                            JsonPayload: jsonPayload,
                            JsonStructure: jsonStructureBuilder?.Build()
                        ));
                        idx++;
                    }

                    void AppendResultColumn(List<FieldDescriptor> collector, JsonElement columnElement, string? prefix, JsonStructureBuilder? jsonBuilder)
                    {
                        var rawName = columnElement.GetPropertyOrDefault("Name") ?? string.Empty;
                        var fullName = CombineColumnNames(prefix, rawName);

                        var hasNested = columnElement.TryGetProperty("Columns", out var nestedEl) && nestedEl.ValueKind == JsonValueKind.Array && nestedEl.GetArrayLength() > 0;
                        var returnsJson = TryGetOptionalBoolean(columnElement, "ReturnsJson");
                        var returnsJsonArray = TryGetOptionalBoolean(columnElement, "ReturnsJsonArray");
                        var includeNullValues = TryGetOptionalBoolean(columnElement, "JsonIncludeNullValues");
                        var jsonRootProperty = columnElement.GetPropertyOrDefault("JsonRootProperty");
                        var jsonElementClrType = columnElement.GetPropertyOrDefault("JsonElementClrType");
                        var jsonElementSqlType = columnElement.GetPropertyOrDefault("JsonElementSqlType");

                        var typeRef = columnElement.GetPropertyOrDefault("TypeRef");
                        var maxLen = columnElement.GetPropertyOrDefaultInt("MaxLength");
                        var precision = columnElement.GetPropertyOrDefaultInt("Precision");
                        var scale = columnElement.GetPropertyOrDefaultInt("Scale");
                        var resolved = typeResolver.Resolve(typeRef, maxLen, precision, scale);
                        var sqlType = resolved?.SqlType ?? columnElement.GetPropertyOrDefault("SqlTypeName") ?? string.Empty;
                        var effectiveMaxLen = resolved?.MaxLength ?? maxLen;
                        var inferred = resolved?.IsNullable ?? InferNullabilityFromTypeRef(typeRef);
                        bool isNullable;
                        if (columnElement.TryGetProperty("IsNullable", out var nullableToken))
                        {
                            isNullable = nullableToken.ValueKind == JsonValueKind.True;
                            if (inferred.HasValue)
                            {
                                isNullable = inferred.Value;
                            }
                        }
                        else
                        {
                            isNullable = inferred ?? (returnsJsonArray == true || returnsJson == true);
                        }
                        if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef)) sqlType = typeRef;
                        var clr = MapSqlToClr(sqlType, isNullable);
                        string? functionRef = null;
                        bool? deferred = null;
                        bool? returnsUnknownJson = null;
                        try
                        {
                            if (columnElement.TryGetProperty("FunctionRef", out var fnEl) && fnEl.ValueKind == JsonValueKind.String)
                            {
                                functionRef = fnEl.GetString();
                            }
                            if (string.IsNullOrWhiteSpace(functionRef) && columnElement.TryGetProperty("Reference", out var legacyRefEl) && legacyRefEl.ValueKind == JsonValueKind.Object)
                            {
                                var kindLegacy = legacyRefEl.GetPropertyOrDefault("Kind");
                                var schemaLegacy = legacyRefEl.GetPropertyOrDefault("Schema");
                                var nameLegacy = legacyRefEl.GetPropertyOrDefault("Name");
                                if (string.Equals(kindLegacy, "Function", StringComparison.OrdinalIgnoreCase))
                                {
                                    functionRef = ComposeSchemaObjectRef(schemaLegacy, nameLegacy);
                                }
                            }
                            if (columnElement.TryGetProperty("DeferredJsonExpansion", out var defEl))
                            {
                                if (defEl.ValueKind == JsonValueKind.True) deferred = true; else if (defEl.ValueKind == JsonValueKind.False) deferred = null;
                            }
                            if (columnElement.TryGetProperty("ReturnsUnknownJson", out var unknownEl) && unknownEl.ValueKind == JsonValueKind.True)
                            {
                                returnsUnknownJson = true;
                            }
                        }
                        catch { }

                        if (deferred is null && !string.IsNullOrWhiteSpace(functionRef) && hasNested == false && (returnsJson == true || returnsJson is null))
                        {
                            deferred = true;
                        }

                        if (returnsUnknownJson == true)
                        {
                            // Surface unknown JSON payloads as JsonNode for generator consumption.
                            clr = "System.Text.Json.Nodes.JsonNode?";
                            if (!isNullable)
                            {
                                isNullable = true;
                            }

                            if (string.IsNullOrWhiteSpace(sqlType))
                            {
                                sqlType = "nvarchar(max)";
                            }
                        }

                        bool TryEmitScalarJsonArray()
                        {
                            if (returnsUnknownJson == true)
                            {
                                return false;
                            }

                            if (returnsJsonArray != true)
                            {
                                return false;
                            }

                            if (string.IsNullOrWhiteSpace(fullName))
                            {
                                return false;
                            }

                            if (nestedEl.ValueKind != JsonValueKind.Array || nestedEl.GetArrayLength() == 0)
                            {
                                return false;
                            }

                            JsonElement? scalarNode = null;
                            foreach (var child in nestedEl.EnumerateArray())
                            {
                                if (child.ValueKind == JsonValueKind.Object)
                                {
                                    if (scalarNode != null)
                                    {
                                        return false; // multiple children => object array
                                    }
                                    scalarNode = child;
                                }
                            }

                            if (scalarNode == null)
                            {
                                return false;
                            }

                            if (scalarNode.Value.TryGetProperty("Columns", out var nestedChild) && nestedChild.ValueKind == JsonValueKind.Array && nestedChild.GetArrayLength() > 0)
                            {
                                return false;
                            }

                            string? elementClrType = string.IsNullOrWhiteSpace(jsonElementClrType) ? null : jsonElementClrType;
                            string? elementSqlType = string.IsNullOrWhiteSpace(jsonElementSqlType) ? null : jsonElementSqlType;
                            var elementNullable = scalarNode.Value.TryGetProperty("IsNullable", out var elementNullableToken)
                                ? elementNullableToken.ValueKind == JsonValueKind.True
                                : true;

                            if (string.IsNullOrWhiteSpace(elementSqlType))
                            {
                                var elementTypeRef = scalarNode.Value.GetPropertyOrDefault("TypeRef");
                                var elementMaxLen = scalarNode.Value.GetPropertyOrDefaultInt("MaxLength");
                                var elementPrecision = scalarNode.Value.GetPropertyOrDefaultInt("Precision");
                                var elementScale = scalarNode.Value.GetPropertyOrDefaultInt("Scale");
                                var resolvedElement = typeResolver.Resolve(elementTypeRef, elementMaxLen, elementPrecision, elementScale);
                                elementSqlType = resolvedElement?.SqlType ?? scalarNode.Value.GetPropertyOrDefault("SqlTypeName");
                                if (string.IsNullOrWhiteSpace(elementSqlType) && !string.IsNullOrWhiteSpace(elementTypeRef))
                                {
                                    elementSqlType = elementTypeRef;
                                }
                            }

                            if (string.IsNullOrWhiteSpace(elementClrType) && !string.IsNullOrWhiteSpace(elementSqlType))
                            {
                                elementClrType = MapSqlToClr(elementSqlType!, elementNullable);
                            }

                            if (string.IsNullOrWhiteSpace(elementClrType))
                            {
                                return false;
                            }

                            static string ComposeArrayClrType(string elementType, bool nullable)
                            {
                                var core = string.IsNullOrWhiteSpace(elementType) ? "string" : elementType.Trim();
                                if (!core.EndsWith("[]", StringComparison.Ordinal))
                                {
                                    core += "[]";
                                }

                                if (nullable && !core.EndsWith("?", StringComparison.Ordinal))
                                {
                                    core += "?";
                                }

                                return core;
                            }

                            var arrayClr = ComposeArrayClrType(elementClrType!, isNullable);
                            collector.Add(new FieldDescriptor(fullName, NamePolicy.Sanitize(fullName), arrayClr, isNullable, sqlType, effectiveMaxLen, FunctionRef: NormalizeSchemaObjectRef(functionRef), DeferredJsonExpansion: deferred, ReturnsJson: returnsJson, ReturnsJsonArray: returnsJsonArray, JsonRootProperty: string.IsNullOrWhiteSpace(jsonRootProperty) ? null : jsonRootProperty, ReturnsUnknownJson: returnsUnknownJson, JsonElementClrType: elementClrType, JsonElementSqlType: string.IsNullOrWhiteSpace(elementSqlType) ? null : elementSqlType, JsonIncludeNullValues: includeNullValues));
                            jsonBuilder?.RegisterLeaf(fullName, true);
                            return true;
                        }

                        if (hasNested)
                        {
                            if (TryEmitScalarJsonArray())
                            {
                                return;
                            }

                            if (!string.IsNullOrWhiteSpace(fullName))
                            {
                                jsonBuilder?.RegisterContainer(fullName, returnsJsonArray == true);
                            }

                            foreach (var child in nestedEl.EnumerateArray())
                            {
                                AppendResultColumn(collector, child, fullName, jsonBuilder);
                            }
                            return;
                        }

                        if (string.IsNullOrWhiteSpace(fullName))
                        {
                            return;
                        }

                        jsonBuilder?.RegisterLeaf(fullName, returnsJsonArray == true);

                        collector.Add(new FieldDescriptor(fullName, NamePolicy.Sanitize(fullName), clr, isNullable, sqlType, effectiveMaxLen, FunctionRef: NormalizeSchemaObjectRef(functionRef), DeferredJsonExpansion: deferred, ReturnsJson: returnsJson, ReturnsJsonArray: returnsJsonArray, JsonRootProperty: string.IsNullOrWhiteSpace(jsonRootProperty) ? null : jsonRootProperty, ReturnsUnknownJson: returnsUnknownJson, JsonElementClrType: string.IsNullOrWhiteSpace(jsonElementClrType) ? null : jsonElementClrType, JsonElementSqlType: string.IsNullOrWhiteSpace(jsonElementSqlType) ? null : jsonElementSqlType, JsonIncludeNullValues: includeNullValues));
                    }

                    static string CombineColumnNames(string? prefix, string? name)
                    {
                        if (string.IsNullOrWhiteSpace(prefix))
                        {
                            return name ?? string.Empty;
                        }

                        if (string.IsNullOrWhiteSpace(name))
                        {
                            return prefix;
                        }

                        return prefix + "." + name;
                    }
                }

                var procDescriptor = new ProcedureDescriptor(
                    ProcedureName: name,
                    Schema: schema,
                    OperationName: operationName,
                    InputParameters: inputParams,
                    OutputFields: outputParams,
                    ResultSets: resultSetDescriptors
                );
                procList.Add(procDescriptor);
                if (resultSetDescriptors.Count > 0)
                {
                    var primary = resultSetDescriptors[0];
                    var payloadType = NamePolicy.Sanitize(operationName) + NamePolicy.Sanitize(primary.Name) + "Row";
                    resultDescriptors.Add(new ResultDescriptor(operationName, payloadType));
                }

                if (inputParams.Count > 0) inputList.Add(new InputDescriptor(operationName, inputParams));
                if (outputParams.Count > 0) outputList.Add(new OutputDescriptor(operationName, outputParams));
                if (resultSetDescriptors.Count > 0) rsList.AddRange(resultSetDescriptors);
            }

            _procedures = procList.OrderBy(p => p.Schema).ThenBy(p => p.ProcedureName).ToList();
            _inputs = inputList.OrderBy(i => i.OperationName).ToList();
            // UDTT recovery block removed—direct snapshot evaluation replaces the old heuristic
            _outputs = outputList.OrderBy(o => o.OperationName).ToList();
            _resultSets = rsList.OrderBy(r => r.Name).ToList();
            _results = resultDescriptors.OrderBy(r => r.OperationName).ToList();

            // Functions (expanded snapshot only)
            try
            {
                if (expanded)
                {
                    // Load functions directory entries from index.json? index.json holds FunctionsVersion + Function file hashes
                    var fnSchemaDir = Path.Combine(_projectRoot, ".xtraq", "snapshots");
                    var fnIndexPath = Path.Combine(fnSchemaDir, "index.json");
                    if (File.Exists(fnIndexPath))
                    {
                        using var idxFs = File.OpenRead(fnIndexPath);
                        using var idxDoc = JsonDocument.Parse(idxFs);
                        if (idxDoc.RootElement.TryGetProperty("FunctionsVersion", out var fv) && fv.ValueKind != JsonValueKind.Null)
                        {
                            // Enumerate function file entries
                            if (idxDoc.RootElement.TryGetProperty("Functions", out var fnsEl) && fnsEl.ValueKind == JsonValueKind.Array)
                            {
                                var fnEntries = fnsEl.EnumerateArray().Select(e => new
                                {
                                    File = e.GetPropertyOrDefault("File"),
                                    Name = e.GetPropertyOrDefault("Name"),
                                    Schema = e.GetPropertyOrDefault("Schema")
                                }).Where(x => !string.IsNullOrWhiteSpace(x.File)).ToList();
                                foreach (var entry in fnEntries)
                                {
                                    var path = Path.Combine(fnSchemaDir, "functions", entry.File!);
                                    if (!File.Exists(path)) continue;
                                    try
                                    {
                                        using var ffs = File.OpenRead(path);
                                        using var fdoc = JsonDocument.Parse(ffs);
                                        var root = fdoc.RootElement;
                                        var schema = root.GetPropertyOrDefault("Schema") ?? entry.Schema ?? "dbo";
                                        var name = root.GetPropertyOrDefault("Name") ?? entry.Name ?? string.Empty;
                                        if (string.IsNullOrWhiteSpace(name)) continue;
                                        bool isTableValued = root.GetPropertyOrDefaultBool("IsTableValued");
                                        var returnSql = root.GetPropertyOrDefault("ReturnSqlType");
                                        var returnMaxLenVal = root.GetPropertyOrDefaultInt("ReturnMaxLength");
                                        int? returnMaxLen = returnMaxLenVal > 0 ? returnMaxLenVal : null;
                                        bool? returnIsNullable = null;
                                        if (root.TryGetProperty("ReturnIsNullable", out var rin) && rin.ValueKind != JsonValueKind.Null)
                                        {
                                            if (rin.ValueKind == JsonValueKind.True) returnIsNullable = true; else if (rin.ValueKind == JsonValueKind.False) returnIsNullable = false; // record only when metadata is present
                                        }
                                        JsonPayloadDescriptor? jsonPayload = null;
                                        var returnsJson = root.GetPropertyOrDefaultBool("ReturnsJson");
                                        var returnsJsonArrayExplicit = TryGetOptionalBoolean(root, "ReturnsJsonArray");
                                        var returnsJsonArray = returnsJsonArrayExplicit ?? (returnsJson ? true : false);
                                        var jsonRootProp = root.GetPropertyOrDefault("JsonRootProperty");
                                        var includeNullValues = TryGetOptionalBoolean(root, "JsonIncludeNullValues");
                                        if (returnsJson || returnsJsonArray || !string.IsNullOrWhiteSpace(jsonRootProp))
                                        {
                                            jsonPayload = new JsonPayloadDescriptor(returnsJsonArray, string.IsNullOrWhiteSpace(jsonRootProp) ? null : jsonRootProp, includeNullValues == true);
                                        }
                                        bool encrypted = root.GetPropertyOrDefaultBool("IsEncrypted");
                                        // Dependencies
                                        var dependencies = new List<string>();
                                        if (root.TryGetProperty("Dependencies", out var depsEl) && depsEl.ValueKind == JsonValueKind.Array)
                                        {
                                            foreach (var de in depsEl.EnumerateArray())
                                            {
                                                if (de.ValueKind == JsonValueKind.String)
                                                {
                                                    var dep = de.GetString();
                                                    if (!string.IsNullOrWhiteSpace(dep)) dependencies.Add(dep!);
                                                }
                                            }
                                        }
                                        // Parameters
                                        var paramDescriptors = new List<FunctionParameterDescriptor>();
                                        if (root.TryGetProperty("Parameters", out var paramsEl) && paramsEl.ValueKind == JsonValueKind.Array)
                                        {
                                            foreach (var pe in paramsEl.EnumerateArray())
                                            {
                                                var raw = pe.GetPropertyOrDefault("Name") ?? string.Empty;
                                                if (string.IsNullOrWhiteSpace(raw)) continue;
                                                var clean = raw.TrimStart('@');
                                                var typeRef = pe.GetPropertyOrDefault("TypeRef");
                                                var maxLenVal = pe.GetPropertyOrDefaultInt("MaxLength");
                                                var precisionVal = pe.GetPropertyOrDefaultInt("Precision");
                                                var scaleVal = pe.GetPropertyOrDefaultInt("Scale");
                                                var resolved = typeResolver.Resolve(typeRef, maxLenVal, precisionVal, scaleVal);
                                                var sqlType = resolved?.SqlType ?? pe.GetPropertyOrDefault("SqlTypeName") ?? pe.GetPropertyOrDefault("SqlType") ?? string.Empty;
                                                int maxLen = (resolved?.MaxLength ?? maxLenVal) ?? 0;
                                                bool isNullable = pe.GetPropertyOrDefaultBoolStrict("IsNullable");
                                                bool isOutput = pe.GetPropertyOrDefaultBool("IsOutput");
                                                if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef)) sqlType = typeRef;
                                                var clr = SqlClrTypeMapper.Map(sqlType, isNullable);
                                                paramDescriptors.Add(new FunctionParameterDescriptor(clean, sqlType, clr, isNullable, maxLen <= 0 ? null : maxLen, isOutput));
                                            }
                                        }
                                        // Columns (TVF)
                                        var colDescriptors = new List<TableValuedFunctionColumnDescriptor>();
                                        if (isTableValued && root.TryGetProperty("Columns", out var colsEl) && colsEl.ValueKind == JsonValueKind.Array)
                                        {
                                            foreach (var ce in colsEl.EnumerateArray())
                                            {
                                                var colName = ce.GetPropertyOrDefault("Name") ?? string.Empty;
                                                if (string.IsNullOrWhiteSpace(colName)) continue;
                                                var typeRef = ce.GetPropertyOrDefault("TypeRef");
                                                bool isNullable = ce.GetPropertyOrDefaultBoolStrict("IsNullable");
                                                var maxLenVal = ce.GetPropertyOrDefaultInt("MaxLength");
                                                var precisionVal = ce.GetPropertyOrDefaultInt("Precision");
                                                var scaleVal = ce.GetPropertyOrDefaultInt("Scale");
                                                var resolved = typeResolver.Resolve(typeRef, maxLenVal, precisionVal, scaleVal);
                                                var sqlType = resolved?.SqlType ?? ce.GetPropertyOrDefault("SqlTypeName") ?? ce.GetPropertyOrDefault("SqlType") ?? string.Empty;
                                                int maxLen = (resolved?.MaxLength ?? maxLenVal) ?? 0;
                                                if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef)) sqlType = typeRef;
                                                var clr = SqlClrTypeMapper.Map(sqlType, isNullable);
                                                colDescriptors.Add(new TableValuedFunctionColumnDescriptor(colName, sqlType, clr, isNullable, maxLen <= 0 ? null : maxLen));
                                            }
                                        }
                                        var functionDescriptor = new FunctionDescriptor(
                                            SchemaName: schema,
                                            FunctionName: name,
                                            IsTableValued: isTableValued,
                                            ReturnSqlType: string.IsNullOrWhiteSpace(returnSql) ? null : returnSql,
                                            ReturnMaxLength: returnMaxLen,
                                            ReturnIsNullable: returnIsNullable,
                                            JsonPayload: jsonPayload,
                                            IsEncrypted: encrypted,
                                            Dependencies: dependencies,
                                            Parameters: paramDescriptors,
                                            Columns: colDescriptors
                                        );
                                        functionDescriptors.Add(functionDescriptor);

                                        if (!isTableValued)
                                        {
                                            var descriptor = TryBuildFunctionJsonDescriptorFromSnapshot(schema, name, jsonPayload, root, typeResolver);
                                            if (descriptor != null)
                                            {
                                                var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
                                                var normalizedName = name.Trim();
                                                var descriptorKey = string.Concat(normalizedSchema, ".", normalizedName);
                                                _functionJsonDescriptors[descriptorKey] = descriptor;
                                                if (normalizedName.Length > 0)
                                                {
                                                    _functionJsonDescriptors.TryAdd(normalizedName, descriptor);
                                                }

                                                var flattened = FlattenFunctionJsonDescriptor(descriptor);
                                                (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames) metadata = (
                                                    ReturnsJson: true,
                                                    ReturnsJsonArray: descriptor.ReturnsJsonArray,
                                                    RootProperty: jsonPayload?.RootProperty ?? string.Empty,
                                                    ColumnNames: flattened
                                                );
                                                functionJsonSets[descriptorKey] = metadata;
                                                if (normalizedName.Length > 0)
                                                {
                                                    functionJsonSets.TryAdd(normalizedName, metadata);
                                                }
                                            }
                                            else
                                            {
                                                var jsonColumns = ExtractFunctionJsonColumns(root);
                                                if (jsonColumns.Count > 0)
                                                {
                                                    var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
                                                    var normalizedName = name.Trim();
                                                    var key = string.Concat(normalizedSchema, ".", normalizedName);
                                                    (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames) metadata = (
                                                        ReturnsJson: jsonPayload != null || returnsJson,
                                                        ReturnsJsonArray: returnsJsonArray,
                                                        RootProperty: string.IsNullOrWhiteSpace(jsonRootProp) ? string.Empty : jsonRootProp.Trim(),
                                                        ColumnNames: jsonColumns
                                                    );
                                                    functionJsonSets[key] = metadata;
                                                    if (normalizedName.Length > 0)
                                                    {
                                                        functionJsonSets.TryAdd(normalizedName, metadata);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception fx) { SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Warning: failed to parse function file {path}: {fx.Message}"); }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SchemaMetadataProviderLogHelper.TryLog($"[xtraq] Warning: function descriptors load failed: {ex.Message}");
            }
            _functions = functionDescriptors.OrderBy(f => f.SchemaName).ThenBy(f => f.FunctionName).ToList();
            _functionJsonSets = functionJsonSets;
            UpdateFunctionJsonResolver();
            if (_procedures.Count == 0)
            {
                SchemaMetadataProviderLogHelper.TryLog("[xtraq] Warning: 0 procedures parsed from snapshot (expanded/legacy)");
            }
        }

        private static IReadOnlyList<string> ExtractFunctionJsonColumns(JsonElement functionElement)
        {
            if (!functionElement.TryGetProperty("Columns", out var colsEl) || colsEl.ValueKind != JsonValueKind.Array || colsEl.GetArrayLength() == 0)
            {
                return Array.Empty<string>();
            }

            var collector = new List<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in colsEl.EnumerateArray())
            {
                CollectFunctionJsonColumns(column, null, collector, seen);
            }
            return collector;
        }

        private static void CollectFunctionJsonColumns(JsonElement columnElement, string? prefix, List<string> collector, HashSet<string> seen)
        {
            var name = columnElement.GetPropertyOrDefault("Name");
            if (string.IsNullOrWhiteSpace(name))
            {
                return;
            }

            var current = string.IsNullOrWhiteSpace(prefix) ? name : string.Concat(prefix, ".", name);
            if (columnElement.TryGetProperty("Columns", out var children) && children.ValueKind == JsonValueKind.Array && children.GetArrayLength() > 0)
            {
                foreach (var child in children.EnumerateArray())
                {
                    CollectFunctionJsonColumns(child, current, collector, seen);
                }
                return;
            }

            var normalized = current.Trim();
            if (normalized.Length == 0)
            {
                return;
            }

            if (seen.Add(normalized))
            {
                collector.Add(normalized);
            }
        }

        private static IReadOnlyList<string> FlattenFunctionJsonDescriptor(FunctionJsonDescriptor descriptor)
        {
            if (descriptor?.Fields == null || descriptor.Fields.Count == 0)
            {
                return Array.Empty<string>();
            }

            var collector = new List<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            void Traverse(FunctionJsonFieldDescriptor field, string? prefix, List<string> target)
            {
                if (field == null || string.IsNullOrWhiteSpace(field.Name))
                {
                    return;
                }

                var current = string.IsNullOrWhiteSpace(prefix) ? field.Name : string.Concat(prefix, ".", field.Name);
                if (field.IsContainer && field.Children.Count > 0)
                {
                    foreach (var child in field.Children)
                    {
                        Traverse(child, current, target);
                    }
                    return;
                }

                if (seen.Add(current))
                {
                    target.Add(current);
                }
            }

            foreach (var field in descriptor.Fields)
            {
                Traverse(field, null, collector);
            }

            return collector;
        }

        private FunctionJsonDescriptor? TryBuildFunctionJsonDescriptorFromSnapshot(
            string schema,
            string name,
            JsonPayloadDescriptor? payload,
            JsonElement functionElement,
            TypeMetadataResolver typeResolver)
        {
            var normalizedName = name?.Trim() ?? string.Empty;
            if (normalizedName.Length == 0)
            {
                return null;
            }

            var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
            if (!functionElement.TryGetProperty("Columns", out var columnsElement)
                || columnsElement.ValueKind != JsonValueKind.Array
                || columnsElement.GetArrayLength() == 0)
            {
                return null;
            }

            var root = new FunctionJsonFieldNode(string.Empty);
            foreach (var columnElement in columnsElement.EnumerateArray())
            {
                var nameToken = columnElement.GetPropertyOrDefault("Name");
                if (string.IsNullOrWhiteSpace(nameToken))
                {
                    continue;
                }

                var segments = nameToken.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (segments.Length == 0)
                {
                    continue;
                }

                var current = root;
                for (var i = 0; i < segments.Length; i++)
                {
                    var segment = segments[i];
                    if (!current.Children.TryGetValue(segment, out var child))
                    {
                        child = new FunctionJsonFieldNode(segment);
                        current.Children[segment] = child;
                    }

                    current = child;
                }

                var isNullableToken = TryGetOptionalBoolean(columnElement, "IsNullable");
                var includeNullToken = TryGetOptionalBoolean(columnElement, "JsonIncludeNullValues");
                var isArrayToken = TryGetOptionalBoolean(columnElement, "ReturnsJsonArray");
                var clrType = ResolveClrTypeFromSnapshot(columnElement, typeResolver, isNullableToken == true);

                current.Metadata = new FunctionJsonFieldMetadata(
                    ClrType: clrType,
                    IsNullable: isNullableToken,
                    IncludeNullValues: includeNullToken == true,
                    IsArray: isArrayToken == true);
            }

            var fields = root.Children.Values
                .OrderBy(node => node.Name, StringComparer.OrdinalIgnoreCase)
                .Select(BuildDescriptor)
                .Where(static descriptor => descriptor != null)
                .Select(static descriptor => descriptor!)
                .ToList();

            if (fields.Count == 0)
            {
                return null;
            }

            var rootTypeName = NamePolicy.Result(normalizedName);
            var returnsArray = payload?.IsArray ?? functionElement.GetPropertyOrDefaultBool("ReturnsJsonArray");
            var includeNullValues = payload?.IncludeNullValues ?? functionElement.GetPropertyOrDefaultBool("JsonIncludeNullValues");

            return new FunctionJsonDescriptor(
                SchemaName: normalizedSchema,
                FunctionName: normalizedName,
                RootTypeName: rootTypeName,
                ReturnsJsonArray: returnsArray,
                IncludeNullValues: includeNullValues,
                Fields: fields);
        }

        private FunctionJsonFieldDescriptor? BuildDescriptor(FunctionJsonFieldNode node)
        {
            if (node == null)
            {
                return null;
            }

            if (node.Children.Count == 0)
            {
                var metadata = node.Metadata;
                if (metadata == null)
                {
                    return new FunctionJsonFieldDescriptor(
                        node.Name,
                        "string",
                        true,
                        false,
                        false,
                        Array.Empty<FunctionJsonFieldDescriptor>());
                }

                var clrType = string.IsNullOrWhiteSpace(metadata.ClrType) ? "string" : metadata.ClrType;
                var isNullable = metadata.IsNullable ?? false;

                return new FunctionJsonFieldDescriptor(
                    node.Name,
                    clrType!,
                    isNullable,
                    metadata.IsArray,
                    metadata.IncludeNullValues,
                    Array.Empty<FunctionJsonFieldDescriptor>());
            }

            var childDescriptors = node.Children.Values
                .OrderBy(child => child.Name, StringComparer.OrdinalIgnoreCase)
                .Select(BuildDescriptor)
                .Where(static descriptor => descriptor != null)
                .Select(static descriptor => descriptor!)
                .ToList();

            if (childDescriptors.Count == 0)
            {
                return null;
            }

            var nodeMetadata = node.Metadata;
            var effectiveNullable = nodeMetadata?.IsNullable ?? childDescriptors.All(child => child.IsNullable);
            var includeNullValues = nodeMetadata?.IncludeNullValues ?? false;
            var isArray = nodeMetadata?.IsArray ?? false;

            return new FunctionJsonFieldDescriptor(
                node.Name,
                null,
                effectiveNullable,
                isArray,
                includeNullValues,
                childDescriptors);
        }

        private string ResolveClrTypeFromSnapshot(JsonElement columnElement, TypeMetadataResolver typeResolver, bool isNullable)
        {
            var typeRef = columnElement.GetPropertyOrDefault("TypeRef");
            var maxLength = columnElement.GetPropertyOrDefaultInt("MaxLength");
            var precision = columnElement.GetPropertyOrDefaultInt("Precision");
            var scale = columnElement.GetPropertyOrDefaultInt("Scale");

            if (!string.IsNullOrWhiteSpace(typeRef))
            {
                var resolved = typeResolver.Resolve(typeRef, maxLength, precision, scale);
                if (resolved.HasValue)
                {
                    var sqlType = string.IsNullOrWhiteSpace(resolved.Value.SqlType) ? typeRef : resolved.Value.SqlType;
                    var nullable = isNullable;
                    if (!nullable && resolved.Value.IsNullable.HasValue && resolved.Value.IsNullable.Value)
                    {
                        nullable = true;
                    }

                    return SqlClrTypeMapper.Map(sqlType, nullable);
                }

                return SqlClrTypeMapper.Map(typeRef, isNullable);
            }

            return SqlClrTypeMapper.Map("nvarchar", isNullable);
        }

        private sealed class FunctionJsonFieldNode
        {
            public FunctionJsonFieldNode(string name) => Name = name;

            public string Name { get; }
            public FunctionJsonFieldMetadata? Metadata { get; set; }
            public Dictionary<string, FunctionJsonFieldNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        }

        private sealed record FunctionJsonFieldMetadata(
            string? ClrType,
            bool? IsNullable,
            bool IncludeNullValues,
            bool IsArray);

        private void UpdateFunctionJsonResolver()
        {
            var local = _functionJsonSets;
            StoredProcedureContentModel.ResolveFunctionJsonSet = (schema, name) =>
            {
                if (string.IsNullOrWhiteSpace(name))
                {
                    return EmptyFunctionJsonMetadata;
                }

                var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
                var normalizedName = name.Trim();
                if (normalizedName.Length == 0)
                {
                    return EmptyFunctionJsonMetadata;
                }

                var key = string.Concat(normalizedSchema, ".", normalizedName);
                if (local.TryGetValue(key, out var meta))
                {
                    return meta;
                }

                if (local.TryGetValue(normalizedName, out meta))
                {
                    return meta;
                }

                return TryResolveFunctionJsonFromSql(normalizedSchema, normalizedName, local);
            };
        }

        private (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames) TryResolveFunctionJsonFromSql(
            string schema,
            string name,
            ConcurrentDictionary<string, (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames)> cache)
        {
            try
            {
                var sql = TryLoadFunctionSql(schema, name);
                if (string.IsNullOrWhiteSpace(sql))
                {
                    return EmptyFunctionJsonMetadata;
                }

                var extractor = new JsonFunctionAstExtractor();
                var ast = extractor.Parse(sql);
                if (!ast.ReturnsJson || ast.Columns.Count == 0)
                {
                    return EmptyFunctionJsonMetadata;
                }

                var columns = FlattenAstColumns(ast.Columns);
                if (columns.Count == 0)
                {
                    return EmptyFunctionJsonMetadata;
                }

                var entry = (
                    ReturnsJson: true,
                    ReturnsJsonArray: ast.ReturnsJsonArray,
                    RootProperty: string.IsNullOrWhiteSpace(ast.JsonRoot) ? string.Empty : ast.JsonRoot!,
                    ColumnNames: columns
                );

                var key = string.Concat(schema, ".", name);
                cache[key] = entry;
                cache.TryAdd(name, entry);
                return entry;
            }
            catch
            {
                return EmptyFunctionJsonMetadata;
            }
        }

        private static IReadOnlyList<string> FlattenAstColumns(IReadOnlyList<JsonFunctionAstColumn> columns)
        {
            if (columns == null || columns.Count == 0)
            {
                return Array.Empty<string>();
            }

            var list = new List<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in columns)
            {
                CollectAstColumns(column, null, list, seen);
            }

            return list;
        }

        private static void CollectAstColumns(JsonFunctionAstColumn column, string? prefix, List<string> collector, HashSet<string> seen)
        {
            if (column == null)
            {
                return;
            }

            var name = column.Name?.Trim();
            if (string.IsNullOrWhiteSpace(name))
            {
                name = "col";
            }

            var current = string.IsNullOrWhiteSpace(prefix) ? name : string.Concat(prefix, ".", name);

            if (column.Children != null && column.Children.Count > 0)
            {
                foreach (var child in column.Children)
                {
                    CollectAstColumns(child, current, collector, seen);
                }

                return;
            }

            if (seen.Add(current))
            {
                collector.Add(current);
            }
        }

        private string? TryLoadFunctionSql(string schema, string name)
        {
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var fileName = name.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) ? name : string.Concat(name, ".sql");
            var schemaFileName = string.Concat(schema, ".", fileName);
            var candidates = new[]
            {
                Path.Combine(_projectRoot, "debug", "sql-sources", "Functions", schema, fileName),
                Path.Combine(_projectRoot, "debug", "sql-sources", "Functions", schema, schemaFileName),
                Path.Combine(_projectRoot, "debug", "sql-sources", "Functions", fileName),
                Path.Combine(_projectRoot, "debug", "sql-sources", "functions", schema, fileName),
                Path.Combine(_projectRoot, "debug", "sql", "functions", schema, fileName),
                Path.Combine(_projectRoot, "sql-sources", "Functions", schema, fileName),
                Path.Combine(_projectRoot, "sql", "functions", schema, fileName),
                Path.Combine(_projectRoot, "sql", "functions", schemaFileName)
            };

            foreach (var candidate in candidates)
            {
                if (string.IsNullOrWhiteSpace(candidate))
                {
                    continue;
                }

                try
                {
                    if (File.Exists(candidate))
                    {
                        return File.ReadAllText(candidate);
                    }
                }
                catch
                {
                    // ignore IO errors and try next candidate
                }
            }

            return null;
        }

        private static string MapSqlToClr(string sql, bool nullable)
        {
            sql = sql.ToLowerInvariant();
            string core = sql switch
            {
                var s when s.StartsWith("int") => "int",
                var s when s.StartsWith("bigint") => "long",
                var s when s.StartsWith("smallint") => "short",
                var s when s.StartsWith("tinyint") => "byte",
                var s when s.StartsWith("bit") => "bool",
                var s when s.StartsWith("decimal") || s.StartsWith("numeric") => "decimal",
                var s when s.StartsWith("float") => "double",
                var s when s.StartsWith("real") => "float",
                var s when s.Contains("date") || s.Contains("time") => "DateTime",
                var s when s.Contains("uniqueidentifier") => "Guid",
                var s when s.Contains("binary") || s.Contains("varbinary") => "byte[]",
                var s when s.Contains("char") || s.Contains("text") => "string",
                _ => "string"
            };
            if (core == "string")
            {
                return nullable ? "string?" : "string";
            }

            if (core == "byte[]")
            {
                return nullable ? "byte[]?" : "byte[]";
            }

            if (nullable)
            {
                core += "?";
            }

            return core;
        }

        private static string? ComposeSchemaObjectRef(string? schema, string? name)
        {
            if (string.IsNullOrWhiteSpace(name)) return null;
            var cleanName = name.Trim();
            if (cleanName.Length == 0) return null;
            var cleanSchema = string.IsNullOrWhiteSpace(schema) ? null : schema.Trim();
            return cleanSchema != null ? string.Concat(cleanSchema, ".", cleanName) : cleanName;
        }

        private static bool? InferNullabilityFromTypeRef(string? typeRef)
        {
            if (string.IsNullOrWhiteSpace(typeRef))
            {
                return null;
            }

            var (schema, name) = TypeMetadataResolver.SplitTypeRef(typeRef);
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (!string.Equals(schema, "core", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            return name.StartsWith("_", StringComparison.Ordinal) ? false : true;
        }

        private static string? NormalizeSchemaObjectRef(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;
            var trimmed = raw.Trim();
            if (trimmed.Length == 0) return null;
            var parts = trimmed.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) return null;
            if (parts.Length == 1)
            {
                return parts[0];
            }
            var schema = parts[0];
            var name = parts[1];
            if (string.IsNullOrWhiteSpace(name)) return null;
            if (string.IsNullOrWhiteSpace(schema)) return name;
            return string.Concat(schema, ".", name);
        }

        private static bool? TryGetOptionalBoolean(JsonElement element, string propertyName)
        {
            if (!element.TryGetProperty(propertyName, out var property))
            {
                return null;
            }

            return property.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => null
            };
        }

        private sealed class JsonStructureBuilder
        {
            private readonly JsonFieldNodeBuilder _root = new(string.Empty, string.Empty);
            private bool _hasNodes;

            public void RegisterContainer(string path, bool isArray)
                => RegisterPath(path, isArray);

            public void RegisterLeaf(string path, bool isArray)
                => RegisterPath(path, isArray);

            private void RegisterPath(string path, bool isArray)
            {
                if (string.IsNullOrWhiteSpace(path))
                {
                    return;
                }

                var segments = path.Split('.', StringSplitOptions.RemoveEmptyEntries);
                if (segments.Length == 0)
                {
                    return;
                }

                var current = _root;
                string currentPath = string.Empty;
                for (int i = 0; i < segments.Length; i++)
                {
                    var segment = segments[i];
                    currentPath = CombinePath(currentPath, segment);
                    current = current.GetOrAdd(segment, currentPath);
                    if (i == segments.Length - 1 && isArray)
                    {
                        current.IsArray = true;
                    }
                }

                if (isArray)
                {
                    current.IsArray = true;
                }

                current.HasValue = true;
                _hasNodes = true;
            }

            public IReadOnlyList<JsonFieldNode>? Build()
            {
                if (!_hasNodes)
                {
                    return null;
                }

                return _root.Children.Values
                    .OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(Convert)
                    .ToList();
            }

            private static JsonFieldNode Convert(JsonFieldNodeBuilder builder)
            {
                var children = builder.Children.Values
                    .OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(Convert)
                    .ToList();

                return new JsonFieldNode(builder.Name, builder.Path, builder.IsArray, children);
            }

            private sealed class JsonFieldNodeBuilder
            {
                public JsonFieldNodeBuilder(string name, string path)
                {
                    Name = name;
                    Path = path;
                }

                public string Name { get; }
                public string Path { get; }
                public bool IsArray { get; set; }
                public bool HasValue { get; set; }
                public Dictionary<string, JsonFieldNodeBuilder> Children { get; } = new(StringComparer.OrdinalIgnoreCase);

                public JsonFieldNodeBuilder GetOrAdd(string name, string path)
                {
                    if (!Children.TryGetValue(name, out var child))
                    {
                        child = new JsonFieldNodeBuilder(name, path);
                        Children[name] = child;
                    }

                    return child;
                }
            }

            private static string CombinePath(string current, string segment)
                => string.IsNullOrWhiteSpace(current) ? segment : string.Concat(current, ".", segment);
        }
    }

}

namespace Xtraq.Metadata
{
    internal static class SchemaMetadataProviderLogHelper
    {
        public static void TryLog(string message)
        {
            try { Console.Out.WriteLine(message); } catch { }
        }
    }
}

