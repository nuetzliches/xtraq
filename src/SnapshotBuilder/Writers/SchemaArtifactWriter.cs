using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Metadata;
using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Writers;

internal sealed class SchemaArtifactWriter
{
    private readonly IConsoleService _console;
    private readonly DbContext _dbContext;
    private readonly ITableMetadataProvider _tableMetadataProvider;
    private readonly ITableTypeMetadataProvider _tableTypeMetadataProvider;
    private readonly IUserDefinedTypeMetadataProvider _userDefinedTypeMetadataProvider;
    private readonly Func<string, byte[], CancellationToken, Task<ArtifactWriteOutcome>> _artifactWriter;
    private static readonly Regex AutoDefaultConstraintRegex = new("^DF__.+__[0-9A-F]{8}$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public SchemaArtifactWriter(
        IConsoleService console,
        DbContext dbContext,
        ITableMetadataProvider tableMetadataProvider,
        ITableTypeMetadataProvider tableTypeMetadataProvider,
        IUserDefinedTypeMetadataProvider userDefinedTypeMetadataProvider,
        Func<string, byte[], CancellationToken, Task<ArtifactWriteOutcome>> artifactWriter)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _tableMetadataProvider = tableMetadataProvider ?? throw new ArgumentNullException(nameof(tableMetadataProvider));
        _tableTypeMetadataProvider = tableTypeMetadataProvider ?? throw new ArgumentNullException(nameof(tableTypeMetadataProvider));
        _userDefinedTypeMetadataProvider = userDefinedTypeMetadataProvider ?? throw new ArgumentNullException(nameof(userDefinedTypeMetadataProvider));
        _artifactWriter = artifactWriter ?? throw new ArgumentNullException(nameof(artifactWriter));
    }

    public async Task<SchemaArtifactSummary> WriteAsync(
        string schemaRoot,
        SnapshotBuildOptions options,
        IReadOnlyList<ProcedureAnalysisResult> updatedProcedures,
        ISet<string> requiredTypeRefs,
        ISet<string> requiredTableRefs,
        CancellationToken cancellationToken)
    {
        var summary = new SchemaArtifactSummary();
        var schemaSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (options?.Schemas != null)
        {
            foreach (var schema in options.Schemas)
            {
                if (!string.IsNullOrWhiteSpace(schema))
                {
                    schemaSet.Add(schema);
                }
            }
        }

        if (updatedProcedures != null)
        {
            foreach (var proc in updatedProcedures)
            {
                var schema = proc?.Descriptor?.Schema;
                if (!string.IsNullOrWhiteSpace(schema))
                {
                    schemaSet.Add(schema);
                }
            }
        }

        if (schemaSet.Count == 0)
        {
            return summary;
        }

        var functionSummary = await WriteFunctionArtifactsAsync(schemaRoot, schemaSet, requiredTypeRefs, cancellationToken).ConfigureAwait(false);
        summary.FilesWritten += functionSummary.FilesWritten;
        summary.FilesUnchanged += functionSummary.FilesUnchanged;
        if (functionSummary.FunctionsVersion > 0)
        {
            summary.FunctionsVersion = functionSummary.FunctionsVersion;
        }

        if (functionSummary.Functions.Count > 0)
        {
            summary.Functions.AddRange(functionSummary.Functions);
        }

        var tableSummary = await WriteTableArtifactsAsync(schemaRoot, schemaSet, requiredTypeRefs, requiredTableRefs, cancellationToken).ConfigureAwait(false);
        summary.FilesWritten += tableSummary.FilesWritten;
        summary.FilesUnchanged += tableSummary.FilesUnchanged;
        if (tableSummary.Tables.Count > 0)
        {
            summary.Tables.AddRange(tableSummary.Tables);
        }

        IReadOnlyList<TableTypeMetadata> tableTypes = Array.Empty<TableTypeMetadata>();
        try
        {
            tableTypes = await _tableTypeMetadataProvider.GetTableTypesAsync(schemaSet, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-tabletype] metadata provider failed: {ex.Message}");
        }

        var tableTypeRoot = Path.Combine(schemaRoot, "tabletypes");
        Directory.CreateDirectory(tableTypeRoot);
        var validTableTypeFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var tableTypeMetadata in tableTypes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (tableTypeMetadata == null)
            {
                continue;
            }

            var tableType = tableTypeMetadata.TableType;
            if (tableType == null || string.IsNullOrWhiteSpace(tableType.SchemaName) || string.IsNullOrWhiteSpace(tableType.Name))
            {
                continue;
            }

            var columns = tableTypeMetadata.Columns ?? Array.Empty<Column>();
            var jsonBytes = BuildTableTypeJson(tableType, columns, requiredTypeRefs);
            var fileName = SnapshotWriterUtilities.BuildArtifactFileName(tableType.SchemaName, tableType.Name);
            var filePath = Path.Combine(tableTypeRoot, fileName);
            var outcome = await _artifactWriter(filePath, jsonBytes, cancellationToken).ConfigureAwait(false);
            if (outcome.Wrote)
            {
                summary.FilesWritten++;
            }
            else
            {
                summary.FilesUnchanged++;
            }

            validTableTypeFiles.Add(fileName);
            summary.TableTypes.Add(new IndexTableTypeEntry
            {
                Schema = tableType.SchemaName,
                Name = tableType.Name,
                File = fileName,
                Hash = outcome.Hash
            });
        }

        PruneExtraneousFiles(tableTypeRoot, validTableTypeFiles);

        var scalarTypes = new List<UserDefinedTypeRow>();
        try
        {
            var localScalarTypes = await _userDefinedTypeMetadataProvider.GetUserDefinedTypesAsync(schemaSet, cancellationToken).ConfigureAwait(false);
            if (localScalarTypes != null && localScalarTypes.Count > 0)
            {
                scalarTypes.AddRange(localScalarTypes);
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-udt] metadata provider failed: {ex.Message}");
        }

        var remoteScalarTypes = await LoadRemoteScalarTypesAsync(requiredTypeRefs, cancellationToken).ConfigureAwait(false);
        if (remoteScalarTypes.Count > 0)
        {
            scalarTypes.AddRange(remoteScalarTypes);
        }

        var scalarRoot = Path.Combine(schemaRoot, "types");
        Directory.CreateDirectory(scalarRoot);
        var validScalarFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var filterActive = requiredTypeRefs.Count > 0;

        foreach (var type in scalarTypes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (type == null || string.IsNullOrWhiteSpace(type.schema_name) || string.IsNullOrWhiteSpace(type.user_type_name))
            {
                continue;
            }

            var baseKey = SnapshotWriterUtilities.BuildKey(type.schema_name, type.user_type_name);
            var notNullKey = SnapshotWriterUtilities.BuildKey(type.schema_name, "_" + type.user_type_name);
            var catalogKey = string.IsNullOrWhiteSpace(type.catalog_name)
                ? null
                : string.Concat(type.catalog_name, ".", baseKey);

            if (filterActive && !requiredTypeRefs.Contains(baseKey) && !requiredTypeRefs.Contains(notNullKey) && (catalogKey == null || !requiredTypeRefs.Contains(catalogKey)))
            {
                continue;
            }

            var jsonBytes = BuildScalarTypeJson(type);
            var fileName = string.IsNullOrWhiteSpace(type.catalog_name)
                ? SnapshotWriterUtilities.BuildArtifactFileName(type.schema_name, type.user_type_name)
                : SnapshotWriterUtilities.BuildArtifactFileName(type.catalog_name, type.schema_name, type.user_type_name);
            var filePath = Path.Combine(scalarRoot, fileName);
            var outcome = await _artifactWriter(filePath, jsonBytes, cancellationToken).ConfigureAwait(false);
            if (outcome.Wrote)
            {
                summary.FilesWritten++;
            }
            else
            {
                summary.FilesUnchanged++;
            }

            validScalarFiles.Add(fileName);
            summary.UserDefinedTypes.Add(new IndexUserDefinedTypeEntry
            {
                Catalog = type.catalog_name,
                Schema = type.schema_name,
                Name = type.user_type_name,
                File = fileName,
                Hash = outcome.Hash
            });
        }

        PruneExtraneousFiles(scalarRoot, validScalarFiles);

        summary.TableTypes.Sort((a, b) =>
        {
            var schemaCompare = string.Compare(a.Schema, b.Schema, StringComparison.OrdinalIgnoreCase);
            return schemaCompare != 0 ? schemaCompare : string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase);
        });

        summary.UserDefinedTypes.Sort((a, b) =>
        {
            var catalogCompare = string.Compare(a.Catalog, b.Catalog, StringComparison.OrdinalIgnoreCase);
            if (catalogCompare != 0)
            {
                return catalogCompare;
            }

            var schemaCompare = string.Compare(a.Schema, b.Schema, StringComparison.OrdinalIgnoreCase);
            return schemaCompare != 0 ? schemaCompare : string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase);
        });

        return summary;
    }

    private async Task<TableArtifactSummary> WriteTableArtifactsAsync(
        string schemaRoot,
        ISet<string> schemaSet,
        ISet<string> requiredTypeRefs,
        ISet<string> requiredTableRefs,
        CancellationToken cancellationToken)
    {
        var summary = new TableArtifactSummary();

        IReadOnlyList<TableMetadata> tableMetadata = Array.Empty<TableMetadata>();
        try
        {
            tableMetadata = await _tableMetadataProvider.GetTablesAsync(schemaSet, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-table] metadata provider failed: {ex.Message}");
            return summary;
        }

        if (tableMetadata == null || tableMetadata.Count == 0)
        {
            return summary;
        }

        var tableRoot = Path.Combine(schemaRoot, "tables");
        Directory.CreateDirectory(tableRoot);
        var validTableFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var tableEntry in tableMetadata)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var table = tableEntry?.Table;
            if (table == null || string.IsNullOrWhiteSpace(table.SchemaName) || string.IsNullOrWhiteSpace(table.Name))
            {
                continue;
            }

            var columns = tableEntry?.Columns ?? Array.Empty<Column>();
            var jsonBytes = BuildTableJson(table, columns, requiredTypeRefs);
            var fileName = SnapshotWriterUtilities.BuildArtifactFileName(table.SchemaName, table.Name);
            var filePath = Path.Combine(tableRoot, fileName);
            var outcome = await _artifactWriter(filePath, jsonBytes, cancellationToken).ConfigureAwait(false);
            if (outcome.Wrote)
            {
                summary.FilesWritten++;
            }
            else
            {
                summary.FilesUnchanged++;
            }

            validTableFiles.Add(fileName);

            summary.Tables.Add(new IndexTableEntry
            {
                Schema = table.SchemaName,
                Name = table.Name,
                File = fileName,
                Hash = outcome.Hash
            });
        }

        if (requiredTableRefs != null && requiredTableRefs.Count > 0)
        {
            var remoteRequests = new Dictionary<string, HashSet<(string Schema, string Name)>>(StringComparer.OrdinalIgnoreCase);
            foreach (var tableRef in requiredTableRefs)
            {
                if (string.IsNullOrWhiteSpace(tableRef))
                {
                    continue;
                }

                var parts = SnapshotWriterUtilities.SplitTableRefParts(tableRef);
                if (string.IsNullOrWhiteSpace(parts.Catalog) || string.IsNullOrWhiteSpace(parts.Schema) || string.IsNullOrWhiteSpace(parts.Name))
                {
                    continue;
                }

                if (!remoteRequests.TryGetValue(parts.Catalog!, out var set))
                {
                    set = new HashSet<(string Schema, string Name)>(new SchemaNameComparer());
                    remoteRequests[parts.Catalog!] = set;
                }

                set.Add((parts.Schema!, parts.Name!));
            }

            foreach (var (catalog, entries) in remoteRequests)
            {
                foreach (var (schema, name) in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    Table? table = null;
                    try
                    {
                        table = await _dbContext.TableAsync(catalog, schema, name, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _console.Verbose($"[snapshot-table-remote] failed to load table {catalog}.{schema}.{name}: {ex.Message}");
                    }

                    if (table == null)
                    {
                        continue;
                    }

                    table.CatalogName = catalog;

                    List<Column> columns;
                    try
                    {
                        columns = await _dbContext.TableColumnsListAsync(catalog, schema, name, cancellationToken).ConfigureAwait(false) ?? new List<Column>();
                    }
                    catch (Exception ex)
                    {
                        _console.Verbose($"[snapshot-table-remote] failed to load columns for {catalog}.{schema}.{name}: {ex.Message}");
                        columns = new List<Column>();
                    }

                    var jsonBytes = BuildTableJson(table, columns, requiredTypeRefs);
                    var fileName = SnapshotWriterUtilities.BuildArtifactFileName(catalog, schema, name);
                    var filePath = Path.Combine(tableRoot, fileName);
                    var outcome = await _artifactWriter(filePath, jsonBytes, cancellationToken).ConfigureAwait(false);
                    if (outcome.Wrote)
                    {
                        summary.FilesWritten++;
                    }
                    else
                    {
                        summary.FilesUnchanged++;
                    }

                    validTableFiles.Add(fileName);

                    summary.Tables.Add(new IndexTableEntry
                    {
                        Catalog = catalog,
                        Schema = schema,
                        Name = name,
                        File = fileName,
                        Hash = outcome.Hash
                    });
                }
            }
        }

        PruneExtraneousFiles(tableRoot, validTableFiles);
        RemoveLegacyTableCacheArtifacts(Directory.GetParent(schemaRoot)?.FullName ?? schemaRoot);

        summary.Tables.Sort((a, b) =>
        {
            var catalogCompare = string.Compare(a.Catalog, b.Catalog, StringComparison.OrdinalIgnoreCase);
            if (catalogCompare != 0)
            {
                return catalogCompare;
            }

            var schemaCompare = string.Compare(a.Schema, b.Schema, StringComparison.OrdinalIgnoreCase);
            return schemaCompare != 0 ? schemaCompare : string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase);
        });

        return summary;
    }

    private async Task<List<UserDefinedTypeRow>> LoadRemoteScalarTypesAsync(ISet<string> requiredTypeRefs, CancellationToken cancellationToken)
    {
        var result = new List<UserDefinedTypeRow>();
        if (requiredTypeRefs == null || requiredTypeRefs.Count == 0)
        {
            return result;
        }

        var requests = new Dictionary<string, HashSet<(string Schema, string Name)>>(StringComparer.OrdinalIgnoreCase);
        foreach (var typeRef in requiredTypeRefs)
        {
            if (string.IsNullOrWhiteSpace(typeRef))
            {
                continue;
            }

            var parts = SnapshotWriterUtilities.SplitTypeRefParts(typeRef);
            if (string.IsNullOrWhiteSpace(parts.Catalog) || string.IsNullOrWhiteSpace(parts.Schema) || string.IsNullOrWhiteSpace(parts.Name))
            {
                continue;
            }

            if (!requests.TryGetValue(parts.Catalog!, out var set))
            {
                set = new HashSet<(string Schema, string Name)>(new SchemaNameComparer());
                requests[parts.Catalog!] = set;
            }

            set.Add((parts.Schema!, parts.Name!));
        }

        if (requests.Count == 0)
        {
            return result;
        }

        foreach (var (catalog, entries) in requests)
        {
            foreach (var (schema, name) in entries)
            {
                var row = await _userDefinedTypeMetadataProvider.GetUserDefinedTypeAsync(catalog, schema, name, cancellationToken).ConfigureAwait(false);
                if (row != null)
                {
                    row.catalog_name = catalog;
                    result.Add(row);
                }
            }
        }

        return result;
    }

    private sealed class SchemaNameComparer : IEqualityComparer<(string Schema, string Name)>
    {
        public bool Equals((string Schema, string Name) x, (string Schema, string Name) y)
            => string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase)
               && string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Schema, string Name) obj)
        {
            var schemaHash = obj.Schema?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            var nameHash = obj.Name?.GetHashCode(StringComparison.OrdinalIgnoreCase) ?? 0;
            return HashCode.Combine(schemaHash, nameHash);
        }
    }

    private async Task<FunctionArtifactSummary> WriteFunctionArtifactsAsync(
        string schemaRoot,
        ISet<string> schemaSet,
        ISet<string> requiredTypeRefs,
        CancellationToken cancellationToken)
    {
        var summary = new FunctionArtifactSummary();

        List<FunctionRow> functionRows = new();
        try
        {
            var list = await _dbContext.FunctionListAsync(cancellationToken).ConfigureAwait(false);
            if (list != null)
            {
                functionRows = list;
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-function] failed to enumerate functions: {ex.Message}");
            return summary;
        }

        var functionRoot = Path.Combine(schemaRoot, "functions");
        Directory.CreateDirectory(functionRoot);
        var validFunctionFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        List<FunctionParamRow> parameterRows = new();
        List<FunctionColumnRow> columnRows = new();
        List<FunctionDependencyRow> dependencyRows = new();

        if (functionRows.Count > 0)
        {
            try
            {
                var list = await _dbContext.FunctionParametersAsync(cancellationToken).ConfigureAwait(false);
                if (list != null)
                {
                    parameterRows = list;
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-function] failed to load function parameters: {ex.Message}");
            }

            try
            {
                var list = await _dbContext.FunctionTvfColumnsAsync(cancellationToken).ConfigureAwait(false);
                if (list != null)
                {
                    columnRows = list;
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-function] failed to load function columns: {ex.Message}");
            }

            try
            {
                var list = await _dbContext.FunctionDependenciesAsync(cancellationToken).ConfigureAwait(false);
                if (list != null)
                {
                    dependencyRows = list;
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-function] failed to load function dependencies: {ex.Message}");
            }
        }

        IReadOnlyDictionary<string, bool?> userTypeNullability = new Dictionary<string, bool?>(StringComparer.OrdinalIgnoreCase);
        var scalarReturnTypeRefs = new Dictionary<int, string>();
        if (functionRows.Count > 0)
        {
            var targetSchemas = new HashSet<string>(schemaSet, StringComparer.OrdinalIgnoreCase);
            foreach (var row in functionRows)
            {
                if (row == null)
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(row.schema_name))
                {
                    targetSchemas.Add(row.schema_name);
                }

                var isTableValuedFunction = string.Equals(row.type_code, "IF", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(row.type_code, "TF", StringComparison.OrdinalIgnoreCase);

                if (!isTableValuedFunction && !string.IsNullOrWhiteSpace(row.definition))
                {
                    var parsedTypeRef = TryParseScalarFunctionReturnTypeRef(row.definition);
                    if (!string.IsNullOrWhiteSpace(parsedTypeRef))
                    {
                        scalarReturnTypeRefs[row.object_id] = parsedTypeRef;

                        var typeParts = SnapshotWriterUtilities.SplitTypeRefParts(parsedTypeRef);
                        var schemaPart = typeParts.Schema;
                        if (!string.IsNullOrWhiteSpace(schemaPart))
                        {
                            targetSchemas.Add(schemaPart);
                        }
                    }
                }
            }

            foreach (var parameter in parameterRows)
            {
                if (parameter == null)
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(parameter.user_type_schema_name))
                {
                    targetSchemas.Add(parameter.user_type_schema_name);
                }
            }

            foreach (var column in columnRows)
            {
                if (column == null)
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(column.user_type_schema_name))
                {
                    targetSchemas.Add(column.user_type_schema_name);
                }
            }

            if (targetSchemas.Count > 0)
            {
                try
                {
                    var scalarTypes = await _userDefinedTypeMetadataProvider.GetUserDefinedTypesAsync(targetSchemas, cancellationToken).ConfigureAwait(false);
                    userTypeNullability = BuildUserTypeNullabilityMap(scalarTypes);
                }
                catch (Exception ex)
                {
                    _console.Verbose($"[snapshot-function] failed to load scalar type metadata: {ex.Message}");
                }
            }
        }

        var parameterLookup = parameterRows
            .GroupBy(row => row.object_id)
            .ToDictionary(group => group.Key, group => group.OrderBy(row => row.ordinal).ToList());

        var columnLookup = columnRows
            .GroupBy(row => row.object_id)
            .ToDictionary(group => group.Key, group => group.OrderBy(row => row.ordinal).ToList());

        var dependencyLookup = BuildFunctionDependencyLookup(functionRows, dependencyRows);
        var astExtractor = new JsonFunctionAstExtractor();

        foreach (var function in functionRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (function == null || string.IsNullOrWhiteSpace(function.schema_name) || string.IsNullOrWhiteSpace(function.function_name))
            {
                continue;
            }

            var schema = function.schema_name;
            var name = function.function_name;
            schemaSet.Add(schema);

            parameterLookup.TryGetValue(function.object_id, out var rawParameters);
            rawParameters ??= new List<FunctionParamRow>();

            var isTableValued = string.Equals(function.type_code, "IF", StringComparison.OrdinalIgnoreCase)
                || string.Equals(function.type_code, "TF", StringComparison.OrdinalIgnoreCase);

            scalarReturnTypeRefs.TryGetValue(function.object_id, out var returnTypeRefFromAst);

            var returnInfo = ExtractFunctionReturnInfo(rawParameters, isTableValued, userTypeNullability, returnTypeRefFromAst);
            var parameters = rawParameters
                .Where(row => !IsReturnParameter(row))
                .OrderBy(row => row.ordinal)
                .ToList();

            List<FunctionColumnRow> columns = new();
            if (isTableValued && columnLookup.TryGetValue(function.object_id, out var mappedColumns))
            {
                columns = mappedColumns;
            }

            dependencyLookup.TryGetValue(function.object_id, out var dependencies);
            dependencies ??= new List<string>();

            JsonFunctionAstResult? astResult = null;
            if (!isTableValued && !string.IsNullOrWhiteSpace(function.definition))
            {
                try
                {
                    astResult = astExtractor.Parse(function.definition);
                }
                catch (Exception ex)
                {
                    _console.Verbose($"[snapshot-function] AST parse failed for {schema}.{name}: {ex.Message}");
                }
            }

            var jsonBytes = BuildFunctionJson(
                function,
                parameters,
                columns,
                dependencies,
                returnInfo.SqlType,
                returnInfo.MaxLength,
                returnInfo.IsNullable,
                astResult,
                requiredTypeRefs,
                userTypeNullability);

            var fileName = SnapshotWriterUtilities.BuildArtifactFileName(schema, name);
            var filePath = Path.Combine(functionRoot, fileName);
            var outcome = await _artifactWriter(filePath, jsonBytes, cancellationToken).ConfigureAwait(false);
            if (outcome.Wrote)
            {
                summary.FilesWritten++;
            }
            else
            {
                summary.FilesUnchanged++;
            }

            validFunctionFiles.Add(fileName);
            summary.Functions.Add(new IndexFunctionEntry
            {
                Schema = schema,
                Name = name,
                File = fileName,
                Hash = outcome.Hash
            });
        }

        summary.FunctionsVersion = 2;
        PruneExtraneousFiles(functionRoot, validFunctionFiles);

        return summary;
    }

    private static Dictionary<int, List<string>> BuildFunctionDependencyLookup(
        IReadOnlyList<FunctionRow> functions,
        IReadOnlyList<FunctionDependencyRow> dependencies)
    {
        var result = new Dictionary<int, List<string>>();
        if (functions == null || functions.Count == 0 || dependencies == null || dependencies.Count == 0)
        {
            return result;
        }

        var map = new Dictionary<int, (string Schema, string Name)>();
        foreach (var function in functions)
        {
            if (function == null)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(function.schema_name) || string.IsNullOrWhiteSpace(function.function_name))
            {
                continue;
            }

            map[function.object_id] = (function.schema_name, function.function_name);
        }

        if (map.Count == 0)
        {
            return result;
        }

        foreach (var group in dependencies.GroupBy(d => d.referencing_id))
        {
            if (!map.ContainsKey(group.Key))
            {
                continue;
            }

            var refs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in group)
            {
                if (entry == null)
                {
                    continue;
                }

                if (entry.referenced_id == entry.referencing_id)
                {
                    continue;
                }

                if (!map.TryGetValue(entry.referenced_id, out var target))
                {
                    continue;
                }

                var key = SnapshotWriterUtilities.BuildKey(target.Schema, target.Name);
                if (!string.IsNullOrWhiteSpace(key))
                {
                    refs.Add(key);
                }
            }

            if (refs.Count > 0)
            {
                result[group.Key] = refs.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
            }
        }

        return result;
    }

    private static IReadOnlyDictionary<string, bool?> BuildUserTypeNullabilityMap(IReadOnlyList<UserDefinedTypeRow>? userDefinedTypes)
    {
        var map = new Dictionary<string, bool?>(StringComparer.OrdinalIgnoreCase);
        if (userDefinedTypes == null || userDefinedTypes.Count == 0)
        {
            return map;
        }

        foreach (var type in userDefinedTypes)
        {
            if (type == null)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(type.schema_name) || string.IsNullOrWhiteSpace(type.user_type_name))
            {
                continue;
            }

            var baseKey = SnapshotWriterUtilities.BuildTypeRef(type.schema_name, type.user_type_name);
            if (!string.IsNullOrWhiteSpace(baseKey))
            {
                map[baseKey] = type.is_nullable == 1;
            }

            if (!string.IsNullOrWhiteSpace(type.catalog_name))
            {
                var catalogKey = SnapshotWriterUtilities.BuildTypeRef(type.catalog_name, type.schema_name, type.user_type_name);
                if (!string.IsNullOrWhiteSpace(catalogKey))
                {
                    map[catalogKey] = type.is_nullable == 1;
                }
            }
        }

        return map;
    }

    private static string TryParseScalarFunctionReturnTypeRef(string? definition)
    {
        if (string.IsNullOrWhiteSpace(definition))
        {
            return string.Empty;
        }

        try
        {
            var parser = new TSql160Parser(false);
            using var reader = new StringReader(definition);
            var fragment = parser.Parse(reader, out var errors);
            if (fragment == null)
            {
                return string.Empty;
            }

            if (errors != null && errors.Count > 0)
            {
                return string.Empty;
            }

            ScalarFunctionReturnType? returnType = null;
            fragment.Accept(new ScalarReturnTypeVisitor(rt => returnType = rt));
            if (returnType?.DataType is DataTypeReference dataType && dataType.Name != null)
            {
                var identifiers = dataType.Name.Identifiers?.Select(i => i.Value) ?? Array.Empty<string>();
                var assembled = string.Join('.', identifiers);
                return assembled.Trim();
            }
        }
        catch
        {
            // Ignore parse failures and fall back to metadata.
        }

        return string.Empty;
    }

    private static FunctionReturnInfo ExtractFunctionReturnInfo(
        IReadOnlyList<FunctionParamRow> parameters,
        bool isTableValued,
        IReadOnlyDictionary<string, bool?> userTypeNullability,
        string? returnTypeRefFromAst)
    {
        if (isTableValued || parameters == null)
        {
            return new FunctionReturnInfo(null, null, null);
        }

        foreach (var parameter in parameters)
        {
            if (parameter == null)
            {
                continue;
            }

            if (!IsReturnParameter(parameter))
            {
                continue;
            }

            var sqlType = SnapshotWriterUtilities.BuildSqlTypeName(parameter);
            int? maxLength = null;
            if (parameter.normalized_length > 0)
            {
                maxLength = parameter.normalized_length;
            }
            else if (parameter.max_length > 0)
            {
                maxLength = parameter.max_length;
            }

            var typeRef = SnapshotWriterUtilities.BuildTypeRef(parameter);
            if (string.IsNullOrWhiteSpace(typeRef) && !string.IsNullOrWhiteSpace(returnTypeRefFromAst))
            {
                typeRef = returnTypeRefFromAst;
            }
            bool? snapshotNullable = null;
            if (!string.IsNullOrWhiteSpace(typeRef)
                && userTypeNullability != null
                && userTypeNullability.TryGetValue(typeRef, out var mapped))
            {
                snapshotNullable = mapped;
            }

            if (!string.IsNullOrWhiteSpace(typeRef))
            {
                sqlType = typeRef;
            }

            bool? userTypeNullable = parameter.user_type_is_nullable.HasValue
                ? parameter.user_type_is_nullable.Value == 1 ? true : false
                : null;

            bool? effectiveNullable;
            if (parameter.is_nullable == 0)
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

            return new FunctionReturnInfo(sqlType, maxLength, effectiveNullable);
        }

        return new FunctionReturnInfo(null, null, null);
    }

    private sealed class ScalarReturnTypeVisitor : TSqlFragmentVisitor
    {
        private readonly Action<ScalarFunctionReturnType> _callback;

        public ScalarReturnTypeVisitor(Action<ScalarFunctionReturnType> callback)
        {
            _callback = callback;
        }

        public override void Visit(TSqlFragment node)
        {
            if (node is CreateFunctionStatement createFunction && createFunction.ReturnType is ScalarFunctionReturnType scalarReturnType)
            {
                _callback(scalarReturnType);
            }

            base.Visit(node);
        }
    }

    private static bool IsReturnParameter(FunctionParamRow parameter)
        => parameter != null && string.IsNullOrWhiteSpace(parameter.param_name);

    private static byte[] BuildFunctionJson(
        FunctionRow function,
        IReadOnlyList<FunctionParamRow> parameters,
        IReadOnlyList<FunctionColumnRow> columns,
        IReadOnlyList<string> dependencies,
        string? returnSqlType,
        int? returnMaxLength,
        bool? returnIsNullable,
        JsonFunctionAstResult? astResult,
        ISet<string>? requiredTypeRefs,
        IReadOnlyDictionary<string, bool?> userTypeNullability)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            var schema = function?.schema_name ?? string.Empty;
            var name = function?.function_name ?? string.Empty;
            writer.WriteString("Schema", schema);
            writer.WriteString("Name", name);

            var isTableValued = string.Equals(function?.type_code, "IF", StringComparison.OrdinalIgnoreCase)
                || string.Equals(function?.type_code, "TF", StringComparison.OrdinalIgnoreCase);
            if (isTableValued)
            {
                writer.WriteBoolean("IsTableValued", true);
            }

            if (string.IsNullOrWhiteSpace(function?.definition))
            {
                writer.WriteBoolean("IsEncrypted", true);
            }

            if (!string.IsNullOrWhiteSpace(returnSqlType))
            {
                writer.WriteString("ReturnSqlType", returnSqlType);
            }

            if (returnMaxLength.HasValue)
            {
                writer.WriteNumber("ReturnMaxLength", returnMaxLength.Value);
            }

            if (returnIsNullable.HasValue)
            {
                writer.WriteBoolean("ReturnIsNullable", returnIsNullable.Value);
            }

            var parameterMetadata = BuildParameterMetadata(parameters, userTypeNullability);
            JsonSnapshotColumn[] jsonColumns = (!isTableValued && astResult?.ReturnsJson == true)
                ? BuildJsonFunctionColumns(astResult!, parameterMetadata)
                : Array.Empty<JsonSnapshotColumn>();

            if (astResult?.ReturnsJson == true)
            {
                writer.WriteBoolean("ReturnsJson", true);
                if (astResult.ReturnsJsonArray)
                {
                    writer.WriteBoolean("ReturnsJsonArray", true);
                }
                else
                {
                    writer.WriteBoolean("ReturnsJsonArray", false);
                }

                if (!string.IsNullOrWhiteSpace(astResult.JsonRoot))
                {
                    writer.WriteString("JsonRootProperty", astResult.JsonRoot);
                }

                if (astResult.IncludeNullValues)
                {
                    writer.WriteBoolean("JsonIncludeNullValues", true);
                }
            }

            if (dependencies != null && dependencies.Count > 0)
            {
                writer.WritePropertyName("Dependencies");
                writer.WriteStartArray();
                foreach (var dep in dependencies)
                {
                    if (string.IsNullOrWhiteSpace(dep))
                    {
                        continue;
                    }

                    writer.WriteStringValue(dep);
                }

                writer.WriteEndArray();
            }

            writer.WritePropertyName("Parameters");
            writer.WriteStartArray();
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    if (parameter == null)
                    {
                        continue;
                    }

                    var rawName = parameter.param_name ?? string.Empty;
                    var cleanName = rawName.TrimStart('@');
                    if (string.IsNullOrWhiteSpace(cleanName))
                    {
                        continue;
                    }

                    writer.WriteStartObject();
                    writer.WriteString("Name", cleanName);

                    var typeRef = SnapshotWriterUtilities.BuildTypeRef(parameter);
                    if (!string.IsNullOrWhiteSpace(typeRef))
                    {
                        writer.WriteString("TypeRef", typeRef);
                        SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, typeRef);
                    }
                    else
                    {
                        var sqlTypeName = SnapshotWriterUtilities.BuildSqlTypeName(parameter);
                        if (!string.IsNullOrWhiteSpace(sqlTypeName))
                        {
                            writer.WriteString("SqlTypeName", sqlTypeName);
                        }
                    }

                    if (SnapshotWriterUtilities.ShouldEmitIsNullable(parameter.is_nullable == 1, typeRef))
                    {
                        writer.WriteBoolean("IsNullable", true);
                    }

                    int? maxLength = null;
                    if (parameter.normalized_length > 0)
                    {
                        maxLength = parameter.normalized_length;
                    }
                    else if (parameter.max_length > 0)
                    {
                        maxLength = parameter.max_length;
                    }

                    if (parameter.max_length == -1)
                    {
                        maxLength = null;
                    }

                    if (maxLength.HasValue && SnapshotWriterUtilities.ShouldEmitMaxLength(maxLength.Value, typeRef))
                    {
                        writer.WriteNumber("MaxLength", maxLength.Value);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitPrecision(parameter.precision, typeRef))
                    {
                        writer.WriteNumber("Precision", parameter.precision);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitScale(parameter.scale, typeRef))
                    {
                        writer.WriteNumber("Scale", parameter.scale);
                    }

                    if (parameter.is_output == 1)
                    {
                        writer.WriteBoolean("IsOutput", true);
                    }

                    if (parameter.has_default_value == 1)
                    {
                        writer.WriteBoolean("HasDefaultValue", true);
                    }

                    writer.WriteEndObject();
                }
            }

            writer.WriteEndArray();

            if (!isTableValued && jsonColumns.Length > 0)
            {
                WriteJsonFunctionColumns(writer, jsonColumns, requiredTypeRefs);
            }

            if (isTableValued && columns != null && columns.Count > 0)
            {
                writer.WritePropertyName("Columns");
                writer.WriteStartArray();
                foreach (var column in columns)
                {
                    if (column == null)
                    {
                        continue;
                    }

                    writer.WriteStartObject();
                    if (!string.IsNullOrWhiteSpace(column.column_name))
                    {
                        writer.WriteString("Name", column.column_name);
                    }

                    var columnTypeRef = SnapshotWriterUtilities.BuildTypeRef(column);
                    if (!string.IsNullOrWhiteSpace(columnTypeRef))
                    {
                        writer.WriteString("TypeRef", columnTypeRef);
                        SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, columnTypeRef);
                    }
                    else
                    {
                        var sqlTypeName = SnapshotWriterUtilities.BuildSqlTypeName(column);
                        if (!string.IsNullOrWhiteSpace(sqlTypeName))
                        {
                            writer.WriteString("SqlTypeName", sqlTypeName);
                        }
                    }

                    if (SnapshotWriterUtilities.ShouldEmitIsNullable(column.is_nullable == 1, columnTypeRef))
                    {
                        writer.WriteBoolean("IsNullable", true);
                    }

                    int? maxLength = null;
                    if (column.normalized_length > 0)
                    {
                        maxLength = column.normalized_length;
                    }
                    else if (column.max_length > 0)
                    {
                        maxLength = column.max_length;
                    }

                    if (column.max_length == -1)
                    {
                        maxLength = null;
                    }

                    if (maxLength.HasValue && SnapshotWriterUtilities.ShouldEmitMaxLength(maxLength.Value, columnTypeRef))
                    {
                        writer.WriteNumber("MaxLength", maxLength.Value);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitPrecision(column.precision, columnTypeRef))
                    {
                        writer.WriteNumber("Precision", column.precision);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitScale(column.scale, columnTypeRef))
                    {
                        writer.WriteNumber("Scale", column.scale);
                    }

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    private static IReadOnlyDictionary<string, ParameterMeta> BuildParameterMetadata(
        IReadOnlyList<FunctionParamRow> parameters,
        IReadOnlyDictionary<string, bool?> userTypeNullability)
    {
        var map = new Dictionary<string, ParameterMeta>(StringComparer.OrdinalIgnoreCase);
        if (parameters == null)
        {
            return map;
        }

        foreach (var parameter in parameters)
        {
            if (parameter == null)
            {
                continue;
            }

            var cleanName = SnapshotWriterUtilities.NormalizeParameterName(parameter.param_name);
            if (string.IsNullOrWhiteSpace(cleanName))
            {
                continue;
            }

            var typeRef = SnapshotWriterUtilities.BuildTypeRef(parameter);
            bool? effectiveNullable;
            if (parameter.is_nullable == 1)
            {
                if (parameter.user_type_is_nullable.HasValue)
                {
                    effectiveNullable = parameter.user_type_is_nullable.Value == 1;
                }
                else if (!string.IsNullOrWhiteSpace(typeRef)
                    && userTypeNullability != null
                    && userTypeNullability.TryGetValue(typeRef, out var mappedNullable)
                    && mappedNullable.HasValue)
                {
                    effectiveNullable = mappedNullable.Value;
                }
                else
                {
                    effectiveNullable = true;
                }
            }
            else
            {
                effectiveNullable = false;
            }

            int? maxLength = null;
            if (parameter.normalized_length > 0)
            {
                maxLength = parameter.normalized_length;
            }
            else if (parameter.max_length > 0)
            {
                maxLength = parameter.max_length;
            }

            if (parameter.max_length == -1)
            {
                maxLength = null;
            }

            map[cleanName] = new ParameterMeta(cleanName, typeRef, effectiveNullable, maxLength);
        }

        return map;
    }

    private static JsonSnapshotColumn[] BuildJsonFunctionColumns(
        JsonFunctionAstResult astResult,
        IReadOnlyDictionary<string, ParameterMeta> parameterMetadata)
    {
        if (astResult == null || astResult.Columns.Count == 0)
        {
            return Array.Empty<JsonSnapshotColumn>();
        }

        var collector = new List<JsonSnapshotColumn>();
        foreach (var column in astResult.Columns)
        {
            CollectJsonColumns(column, Array.Empty<string>(), collector, parameterMetadata);
        }

        return collector
            .GroupBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
            .Select(group => MergeColumns(group))
            .OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static void CollectJsonColumns(
        JsonFunctionAstColumn source,
        IReadOnlyList<string> prefix,
        List<JsonSnapshotColumn> collector,
        IReadOnlyDictionary<string, ParameterMeta> parameterMetadata)
    {
        if (source == null)
        {
            return;
        }

        var segments = SplitAliasSegments(source.Name);
        if (segments.Count == 0)
        {
            return;
        }

        var path = new List<string>(prefix.Count + segments.Count);
        for (var i = 0; i < prefix.Count; i++)
        {
            path.Add(prefix[i]);
        }
        for (var i = 0; i < segments.Count; i++)
        {
            path.Add(segments[i]);
        }

        if (source.Children != null && source.Children.Count > 0)
        {
            foreach (var child in source.Children)
            {
                CollectJsonColumns(child, path, collector, parameterMetadata);
            }
            return;
        }

        var name = string.Join('.', path);
        var snapshotColumn = new JsonSnapshotColumn
        {
            Name = name,
            ReturnsJson = source.ReturnsJson ? true : null,
            ReturnsJsonArray = source.ReturnsJsonArray,
            JsonIncludeNullValues = source.JsonIncludeNullValues == true ? true : null
        };

        AssignParameterMetadata(snapshotColumn, path, source, parameterMetadata);
        if (!snapshotColumn.IsNullable.HasValue)
        {
            snapshotColumn.IsNullable = true;
        }

        collector.Add(snapshotColumn);
    }

    private static List<string> SplitAliasSegments(string? alias)
    {
        var segments = new List<string>();
        if (string.IsNullOrWhiteSpace(alias))
        {
            return segments;
        }

        var tokens = alias.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var token in tokens)
        {
            var value = token.Trim();
            if (value.Length > 0)
            {
                segments.Add(value);
            }
        }

        return segments;
    }

    private static void AssignParameterMetadata(
        JsonSnapshotColumn node,
        IReadOnlyList<string> path,
        JsonFunctionAstColumn source,
        IReadOnlyDictionary<string, ParameterMeta> parameterMetadata)
    {
        if (node == null)
        {
            return;
        }

        if (parameterMetadata == null || parameterMetadata.Count == 0)
        {
            node.IsNullable ??= true;
            return;
        }

        ParameterMeta? selected = null;

        if (source?.Parts != null)
        {
            foreach (var part in source.Parts)
            {
                if (string.IsNullOrWhiteSpace(part))
                {
                    continue;
                }

                var key = part.TrimStart('@');
                if (parameterMetadata.TryGetValue(key, out var meta))
                {
                    selected = meta;
                    break;
                }
            }
        }

        if (!selected.HasValue)
        {
            var leaf = path[^1];
            if (parameterMetadata.TryGetValue(leaf, out var direct))
            {
                selected = direct;
            }
        }

        if (!selected.HasValue)
        {
            var leaf = path[^1];
            var candidates = new List<ParameterMeta>();
            foreach (var meta in parameterMetadata.Values)
            {
                if (meta.Name.EndsWith(leaf, StringComparison.OrdinalIgnoreCase))
                {
                    candidates.Add(meta);
                }
            }

            if (candidates.Count == 1)
            {
                selected = candidates[0];
            }
            else if (candidates.Count > 1)
            {
                var prefixSegments = new List<string>();
                for (var i = 0; i < path.Count - 1; i++)
                {
                    prefixSegments.Add(path[i]);
                }

                ParameterMeta? best = null;
                var bestScore = int.MinValue;
                foreach (var candidate in candidates)
                {
                    var score = 0;
                    foreach (var segment in prefixSegments)
                    {
                        if (string.IsNullOrWhiteSpace(segment))
                        {
                            continue;
                        }

                        if (candidate.Name.IndexOf(segment, StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            score += 10;
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(candidate.TypeRef))
                    {
                        score++;
                    }

                    if (!best.HasValue || score > bestScore)
                    {
                        best = candidate;
                        bestScore = score;
                    }
                }

                if (best.HasValue)
                {
                    selected = best.Value;
                }
            }
        }

        if (selected.HasValue)
        {
            var meta = selected.Value;
            if (!string.IsNullOrWhiteSpace(meta.TypeRef))
            {
                node.TypeRef ??= meta.TypeRef;
            }

            if (meta.IsNullable.HasValue)
            {
                node.IsNullable = meta.IsNullable;
            }

            if (meta.MaxLength.HasValue && meta.MaxLength.Value > 0)
            {
                node.MaxLength ??= meta.MaxLength;
            }
        }
        else
        {
            node.IsNullable ??= true;
        }
    }

    private static void WriteJsonFunctionColumns(Utf8JsonWriter writer, JsonSnapshotColumn[] columns, ISet<string>? requiredTypeRefs)
    {
        if (columns == null || columns.Length == 0)
        {
            return;
        }

        writer.WritePropertyName("Columns");
        writer.WriteStartArray();
        foreach (var column in columns)
        {
            WriteJsonFunctionColumn(writer, column, requiredTypeRefs);
        }
        writer.WriteEndArray();
    }

    private static void WriteJsonFunctionColumn(Utf8JsonWriter writer, JsonSnapshotColumn column, ISet<string>? requiredTypeRefs)
    {
        if (column == null)
        {
            return;
        }

        writer.WriteStartObject();
        writer.WriteString("Name", column.Name);

        if (!string.IsNullOrWhiteSpace(column.TypeRef))
        {
            writer.WriteString("TypeRef", column.TypeRef);
            SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, column.TypeRef);
        }

        if (column.IsNullable == true)
        {
            writer.WriteBoolean("IsNullable", true);
        }

        if (column.MaxLength.HasValue && column.MaxLength.Value > 0)
        {
            writer.WriteNumber("MaxLength", column.MaxLength.Value);
        }

        if (column.ReturnsJson == true)
        {
            writer.WriteBoolean("ReturnsJson", true);
        }

        if (column.ReturnsJsonArray == true)
        {
            writer.WriteBoolean("ReturnsJsonArray", true);
        }

        if (column.JsonIncludeNullValues == true)
        {
            writer.WriteBoolean("JsonIncludeNullValues", true);
        }

        writer.WriteEndObject();
    }

    private sealed class JsonSnapshotColumn
    {
        public string Name { get; set; } = string.Empty;
        public string? TypeRef { get; set; }
        public bool? IsNullable { get; set; }
        public int? MaxLength { get; set; }
        public bool? ReturnsJson { get; set; }
        public bool? ReturnsJsonArray { get; set; }
        public bool? JsonIncludeNullValues { get; set; }
    }

    private static JsonSnapshotColumn MergeColumns(IGrouping<string, JsonSnapshotColumn> group)
    {
        var merged = new JsonSnapshotColumn { Name = group.Key };
        foreach (var column in group)
        {
            merged.TypeRef ??= column.TypeRef;
            merged.IsNullable ??= column.IsNullable;
            merged.MaxLength ??= column.MaxLength;
            merged.ReturnsJson ??= column.ReturnsJson;
            merged.ReturnsJsonArray ??= column.ReturnsJsonArray;
            merged.JsonIncludeNullValues ??= column.JsonIncludeNullValues;
        }
        return merged;
    }

    private readonly record struct ParameterMeta(string Name, string? TypeRef, bool? IsNullable, int? MaxLength);

    private static byte[] BuildTableJson(Table table, IReadOnlyList<Column> columns, ISet<string>? requiredTypeRefs)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            var catalogName = table?.CatalogName;
            if (!string.IsNullOrWhiteSpace(catalogName))
            {
                writer.WriteString("Catalog", catalogName);
            }
            writer.WriteString("Schema", table?.SchemaName ?? string.Empty);
            writer.WriteString("Name", table?.Name ?? string.Empty);
            WriteTableColumns(writer, columns, requiredTypeRefs);
            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    private static void WriteTableColumns(Utf8JsonWriter writer, IReadOnlyList<Column> columns, ISet<string>? requiredTypeRefs)
    {
        writer.WritePropertyName("Columns");
        writer.WriteStartArray();
        if (columns != null)
        {
            foreach (var column in columns)
            {
                if (column == null)
                {
                    continue;
                }

                writer.WriteStartObject();
                if (!string.IsNullOrWhiteSpace(column.Name))
                {
                    writer.WriteString("Name", column.Name);
                }

                var columnTypeRef = SnapshotWriterUtilities.BuildTypeRef(column);
                var effectiveTypeRef = columnTypeRef;
                if (string.IsNullOrWhiteSpace(effectiveTypeRef) && !string.IsNullOrWhiteSpace(column.SqlTypeName))
                {
                    var normalized = SnapshotWriterUtilities.NormalizeSqlTypeName(column.SqlTypeName);
                    if (!string.IsNullOrWhiteSpace(normalized))
                    {
                        effectiveTypeRef = SnapshotWriterUtilities.BuildTypeRef("sys", normalized);
                    }
                }

                if (!string.IsNullOrWhiteSpace(effectiveTypeRef))
                {
                    writer.WriteString("TypeRef", effectiveTypeRef);
                    SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, effectiveTypeRef);
                }
                else if (!string.IsNullOrWhiteSpace(column.SqlTypeName))
                {
                    writer.WriteString("SqlTypeName", column.SqlTypeName);
                }

                if (SnapshotWriterUtilities.ShouldEmitIsNullable(column.IsNullable, effectiveTypeRef ?? column.SqlTypeName))
                {
                    writer.WriteBoolean("IsNullable", true);
                }

                if (SnapshotWriterUtilities.ShouldEmitMaxLength(column.MaxLength, effectiveTypeRef))
                {
                    writer.WriteNumber("MaxLength", column.MaxLength);
                }

                var precision = column.Precision;
                if (SnapshotWriterUtilities.ShouldEmitPrecision(precision, effectiveTypeRef))
                {
                    writer.WriteNumber("Precision", precision.GetValueOrDefault());
                }

                var scale = column.Scale;
                if (SnapshotWriterUtilities.ShouldEmitScale(scale, effectiveTypeRef))
                {
                    writer.WriteNumber("Scale", scale.GetValueOrDefault());
                }

                if (column.IsIdentityRaw.HasValue && column.IsIdentityRaw.Value == 1)
                {
                    writer.WriteBoolean("IsIdentity", true);
                }

                if (column.HasDefaultValue)
                {
                    writer.WriteBoolean("HasDefaultValue", true);
                    if (ShouldPersistDefaultConstraintName(column.DefaultConstraintName))
                    {
                        writer.WriteString("DefaultConstraintName", column.DefaultConstraintName);
                    }

                    var defaultDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.DefaultDefinition);
                    if (!string.IsNullOrWhiteSpace(defaultDefinition))
                    {
                        writer.WriteString("DefaultDefinition", defaultDefinition);
                    }
                }

                if (column.IsComputed)
                {
                    writer.WriteBoolean("IsComputed", true);

                    var computedDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.ComputedDefinition);
                    if (!string.IsNullOrWhiteSpace(computedDefinition))
                    {
                        writer.WriteString("ComputedDefinition", computedDefinition);
                    }

                    if (column.IsComputedPersisted)
                    {
                        writer.WriteBoolean("IsComputedPersisted", true);
                    }
                }

                if (column.IsRowGuid)
                {
                    writer.WriteBoolean("IsRowGuid", true);
                }

                if (column.IsSparse)
                {
                    writer.WriteBoolean("IsSparse", true);
                }

                if (column.IsHidden)
                {
                    writer.WriteBoolean("IsHidden", true);
                }

                if (column.IsColumnSet)
                {
                    writer.WriteBoolean("IsColumnSet", true);
                }

                if (!string.IsNullOrWhiteSpace(column.GeneratedAlwaysType) &&
                    !string.Equals(column.GeneratedAlwaysType, "NOT_APPLICABLE", StringComparison.OrdinalIgnoreCase))
                {
                    writer.WriteString("GeneratedAlwaysType", column.GeneratedAlwaysType);
                }

                writer.WriteEndObject();
            }
        }

        writer.WriteEndArray();
    }

    private static bool ShouldPersistDefaultConstraintName(string? constraintName)
    {
        if (string.IsNullOrWhiteSpace(constraintName))
        {
            return false;
        }

        return !AutoDefaultConstraintRegex.IsMatch(constraintName);
    }

    private static byte[] BuildTableTypeJson(TableType tableType, IReadOnlyList<Column> columns, ISet<string>? requiredTypeRefs)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WriteString("Schema", tableType?.SchemaName ?? string.Empty);
            writer.WriteString("Name", tableType?.Name ?? string.Empty);

            writer.WritePropertyName("Columns");
            writer.WriteStartArray();
            if (columns != null)
            {
                foreach (var column in columns)
                {
                    if (column == null)
                    {
                        continue;
                    }

                    writer.WriteStartObject();
                    if (!string.IsNullOrWhiteSpace(column.Name))
                    {
                        writer.WriteString("Name", column.Name);
                    }

                    var columnTypeRef = SnapshotWriterUtilities.BuildTypeRef(column);
                    var effectiveTypeRef = columnTypeRef;
                    if (string.IsNullOrWhiteSpace(effectiveTypeRef) && !string.IsNullOrWhiteSpace(column.SqlTypeName))
                    {
                        var normalized = SnapshotWriterUtilities.NormalizeSqlTypeName(column.SqlTypeName);
                        if (!string.IsNullOrWhiteSpace(normalized))
                        {
                            effectiveTypeRef = SnapshotWriterUtilities.BuildTypeRef("sys", normalized);
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(effectiveTypeRef))
                    {
                        writer.WriteString("TypeRef", effectiveTypeRef);
                        SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, effectiveTypeRef);
                    }
                    else if (!string.IsNullOrWhiteSpace(column.SqlTypeName))
                    {
                        writer.WriteString("SqlTypeName", column.SqlTypeName);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitIsNullable(column.IsNullable, effectiveTypeRef ?? column.SqlTypeName))
                    {
                        writer.WriteBoolean("IsNullable", true);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitMaxLength(column.MaxLength, effectiveTypeRef))
                    {
                        writer.WriteNumber("MaxLength", column.MaxLength);
                    }

                    var columnPrecision = column.Precision;
                    if (SnapshotWriterUtilities.ShouldEmitPrecision(columnPrecision, effectiveTypeRef))
                    {
                        writer.WriteNumber("Precision", columnPrecision.GetValueOrDefault());
                    }

                    var columnScale = column.Scale;
                    if (SnapshotWriterUtilities.ShouldEmitScale(columnScale, effectiveTypeRef))
                    {
                        writer.WriteNumber("Scale", columnScale.GetValueOrDefault());
                    }

                    if (column.IsIdentityRaw.HasValue && column.IsIdentityRaw.Value == 1)
                    {
                        writer.WriteBoolean("IsIdentity", true);
                    }

                    writer.WriteEndObject();
                }
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    private static byte[] BuildScalarTypeJson(UserDefinedTypeRow type)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            if (!string.IsNullOrWhiteSpace(type?.catalog_name))
            {
                writer.WriteString("Catalog", type.catalog_name);
            }
            writer.WriteString("Schema", type?.schema_name ?? string.Empty);
            writer.WriteString("Name", type?.user_type_name ?? string.Empty);
            if (!string.IsNullOrWhiteSpace(type?.base_type_name))
            {
                writer.WriteString("BaseSqlTypeName", type.base_type_name);
            }

            if (type?.max_length > 0)
            {
                writer.WriteNumber("MaxLength", type.max_length);
            }

            if (type?.precision > 0)
            {
                writer.WriteNumber("Precision", type.precision);
            }

            if (type?.scale > 0)
            {
                writer.WriteNumber("Scale", type.scale);
            }

            writer.WriteBoolean("IsNullable", type?.is_nullable == 1);

            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    private static void PruneExtraneousFiles(string directory, HashSet<string> validFileNames)
    {
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
        {
            return;
        }

        try
        {
            var existingFiles = Directory.GetFiles(directory, "*.json", SearchOption.TopDirectoryOnly);
            foreach (var path in existingFiles)
            {
                var fileName = Path.GetFileName(path);
                if (validFileNames != null && validFileNames.Contains(fileName))
                {
                    continue;
                }

                try
                {
                    File.Delete(path);
                }
                catch
                {
                    // best effort cleanup
                }
            }
        }
        catch
        {
            // ignore cleanup failures
        }
    }

    private static void RemoveLegacyTableCacheArtifacts(string xtraqRoot)
    {
        if (string.IsNullOrWhiteSpace(xtraqRoot))
        {
            return;
        }

        var cacheTables = Path.Combine(xtraqRoot, "cache", "tables");
        if (!Directory.Exists(cacheTables))
        {
            return;
        }

        try
        {
            Directory.Delete(cacheTables, recursive: true);
        }
        catch
        {
            // best effort cleanup only
        }
    }
}
