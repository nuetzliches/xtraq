using Xtraq.Data.Models;
using Xtraq.Schema;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Analyzers;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Metadata;

/// <summary>
/// Default implementation of <see cref="IProcedureMetadataEnricher"/> that reuses snapshot metadata,
/// enriches AST models with SQL type information, and records unresolved columns.
/// </summary>
internal sealed class ProcedureMetadataEnricher : IProcedureMetadataEnricher
{
    private readonly IConsoleService _console;
    private readonly IFunctionJsonMetadataProvider _functionJsonMetadataProvider;
    private readonly IEnhancedSchemaMetadataProvider _schemaMetadataProvider;

    public ProcedureMetadataEnricher(
        IConsoleService console,
        IFunctionJsonMetadataProvider functionJsonMetadataProvider,
        IEnhancedSchemaMetadataProvider schemaMetadataProvider)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _functionJsonMetadataProvider = functionJsonMetadataProvider ?? throw new ArgumentNullException(nameof(functionJsonMetadataProvider));
        _schemaMetadataProvider = schemaMetadataProvider ?? throw new ArgumentNullException(nameof(schemaMetadataProvider));
    }

    public async Task EnrichAsync(ProcedureMetadataEnrichmentRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Procedure);

        var descriptorLabel = FormatProcedureLabel(request.Descriptor);

        try
        {
            ApplySnapshotColumnMetadata(request.SnapshotFile, request.Procedure);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] Snapshot metadata reuse failed for {descriptorLabel}: {ex.Message}");
        }

        try
        {
            await EnrichResultSetMetadataAsync(request.Descriptor, request.Procedure, descriptorLabel, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] Result-set enrichment failed for {descriptorLabel}: {ex.Message}");
        }
    }

    private async Task EnrichResultSetMetadataAsync(
        ProcedureDescriptor descriptor,
        ProcedureModel procedure,
        string descriptorLabel,
        CancellationToken cancellationToken)
    {
        if (procedure.ResultSets.Count == 0)
        {
            return;
        }

        var tableCache = new Dictionary<string, Dictionary<string, Column>>(StringComparer.OrdinalIgnoreCase);
        var unresolvedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var resultSet in procedure.ResultSets)
        {
            if (resultSet?.Columns == null || resultSet.Columns.Count == 0)
            {
                continue;
            }

            foreach (var column in resultSet.Columns)
            {
                var initialPath = column?.Name;
                await EnrichColumnRecursiveAsync(column, tableCache, descriptorLabel, initialPath, unresolvedColumns, cancellationToken).ConfigureAwait(false);
            }
        }

        if (unresolvedColumns.Count == 0)
        {
            return;
        }

        static string ExtractPath(string entry)
        {
            if (string.IsNullOrWhiteSpace(entry))
            {
                return string.Empty;
            }

            var separatorIndex = entry.IndexOf('|');
            if (separatorIndex >= 0 && separatorIndex < entry.Length - 1)
            {
                return entry[(separatorIndex + 1)..];
            }

            return entry;
        }

        var paths = unresolvedColumns
            .Select(ExtractPath)
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (paths.Length > 0 && _console.IsVerbose)
        {
            var summary = string.Join(", ", paths);
            _console.Verbose($"[snapshot-analyze] {descriptorLabel}: {paths.Length} column(s) remain without SQL type after full metadata enrichment ({summary}).");
        }
    }

    private async Task EnrichColumnRecursiveAsync(
        ProcedureResultColumn? column,
        Dictionary<string, Dictionary<string, Column>> tableCache,
        string descriptorLabel,
        string? path,
        ISet<string> unresolvedColumns,
        CancellationToken cancellationToken)
    {
        if (column == null)
        {
            return;
        }

        var currentPath = string.IsNullOrWhiteSpace(path) ? column.Name : path;

        if (column.Columns != null && column.Columns.Count > 0)
        {
            foreach (var child in column.Columns)
            {
                var childPath = CombinePath(currentPath, child?.Name);
                await EnrichColumnRecursiveAsync(child, tableCache, descriptorLabel, childPath, unresolvedColumns, cancellationToken).ConfigureAwait(false);
            }
        }

        if (column.Reference != null && column.Reference.Kind == ProcedureReferenceKind.Function)
        {
            await ApplyFunctionJsonMetadataAsync(column, cancellationToken).ConfigureAwait(false);
        }

        if (column.ReturnsJson == true)
        {
            EnsureJsonArrayElementMetadata(column);
            return;
        }

        var metadata = await ResolveColumnMetadataAsync(column, tableCache, cancellationToken).ConfigureAwait(false);
        var warningPath = BuildWarningPath(currentPath, column);
        if (metadata == null)
        {
            if (column.ReturnsUnknownJson == true)
            {
                return;
            }

            RegisterUnresolvedColumn(column, descriptorLabel, warningPath, unresolvedColumns);
            return;
        }

        if (!string.IsNullOrWhiteSpace(metadata.SqlType))
        {
            column.SqlTypeName = metadata.SqlType;
        }
        if (!column.MaxLength.HasValue && metadata.MaxLength.HasValue)
        {
            column.MaxLength = metadata.MaxLength;
        }
        if (!column.IsNullable.HasValue && metadata.IsNullable.HasValue)
        {
            column.IsNullable = metadata.IsNullable;
        }
        if (!string.IsNullOrWhiteSpace(metadata.UserTypeSchema) && string.IsNullOrWhiteSpace(column.UserTypeSchemaName))
        {
            column.UserTypeSchemaName = metadata.UserTypeSchema;
        }
        if (!string.IsNullOrWhiteSpace(metadata.UserTypeName) && string.IsNullOrWhiteSpace(column.UserTypeName))
        {
            column.UserTypeName = metadata.UserTypeName;
        }

        if (column.ReturnsUnknownJson == true)
        {
            return;
        }

        if (!HasEffectiveTypeMetadata(column))
        {
            RegisterUnresolvedColumn(column, descriptorLabel, warningPath, unresolvedColumns);
        }
    }

    private async Task ApplyFunctionJsonMetadataAsync(ProcedureResultColumn column, CancellationToken cancellationToken)
    {
        if (column?.Reference == null)
        {
            return;
        }

        var name = column.Reference.Name;
        if (string.IsNullOrWhiteSpace(name))
        {
            return;
        }

        var schema = string.IsNullOrWhiteSpace(column.Reference.Schema) ? null : column.Reference.Schema;
        var metadata = await _functionJsonMetadataProvider.ResolveAsync(schema, name, cancellationToken).ConfigureAwait(false);
        if (metadata == null || !metadata.ReturnsJson)
        {
            return;
        }

        column.ReturnsJson = true;
        column.IsNestedJson = column.IsNestedJson ?? true;
        column.ReturnsJsonArray = metadata.ReturnsJsonArray;

        if (string.IsNullOrWhiteSpace(column.JsonRootProperty) && !string.IsNullOrWhiteSpace(metadata.RootProperty))
        {
            column.JsonRootProperty = metadata.RootProperty;
        }
    }

    private async Task<ColumnMetadata?> ResolveColumnMetadataAsync(
        ProcedureResultColumn column,
        Dictionary<string, Dictionary<string, Column>> tableCache,
        CancellationToken cancellationToken)
    {
        if (column == null)
        {
            return null;
        }

        if (column.Reference?.Kind == ProcedureReferenceKind.Function && !string.IsNullOrWhiteSpace(column.Reference.Name))
        {
            var functionSchema = column.Reference.Schema;
            if (string.IsNullOrWhiteSpace(functionSchema))
            {
                functionSchema = column.SourceSchema;
            }

            var functionMetadata = await _schemaMetadataProvider.ResolveFunctionReturnAsync(functionSchema ?? string.Empty, column.Reference.Name, cancellationToken).ConfigureAwait(false);
            if (functionMetadata != null && !functionMetadata.IsTableValued)
            {
                if (!string.IsNullOrWhiteSpace(functionMetadata.SqlTypeName))
                {
                    column.SqlTypeName ??= NormalizeSqlType(functionMetadata.SqlTypeName);
                }

                if (!column.MaxLength.HasValue && functionMetadata.MaxLength.HasValue)
                {
                    column.MaxLength = functionMetadata.MaxLength;
                }

                if (!column.IsNullable.HasValue && functionMetadata.IsNullable.HasValue)
                {
                    column.IsNullable = functionMetadata.IsNullable;
                }

                if (!string.IsNullOrWhiteSpace(functionMetadata.UserTypeSchema) && string.IsNullOrWhiteSpace(column.UserTypeSchemaName))
                {
                    column.UserTypeSchemaName = functionMetadata.UserTypeSchema;
                }

                if (!string.IsNullOrWhiteSpace(functionMetadata.UserTypeName) && string.IsNullOrWhiteSpace(column.UserTypeName))
                {
                    column.UserTypeName = functionMetadata.UserTypeName;
                }

                var effectiveSqlType = column.SqlTypeName ?? functionMetadata.SqlTypeName;
                if (string.IsNullOrWhiteSpace(effectiveSqlType) && !string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(column.UserTypeName))
                {
                    effectiveSqlType = string.Concat(column.UserTypeSchemaName, ".", column.UserTypeName);
                }

                return new ColumnMetadata(
                    effectiveSqlType ?? string.Empty,
                    column.MaxLength ?? functionMetadata.MaxLength,
                    functionMetadata.Precision,
                    functionMetadata.Scale,
                    column.IsNullable ?? functionMetadata.IsNullable,
                    column.UserTypeSchemaName,
                    column.UserTypeName);
            }
        }

        Column? tableColumn = null;
        if (!string.IsNullOrWhiteSpace(column.SourceSchema) && !string.IsNullOrWhiteSpace(column.SourceTable) && !string.IsNullOrWhiteSpace(column.SourceColumn))
        {
            tableColumn = await GetTableColumnAsync(column.SourceCatalog, column.SourceSchema, column.SourceTable, column.SourceColumn, tableCache, cancellationToken).ConfigureAwait(false);
        }

        if (!string.IsNullOrWhiteSpace(column.CastTargetType))
        {
            var normalized = NormalizeSqlType(column.CastTargetType);
            var maxLen = NormalizeLength(column.CastTargetLength);
            var precision = column.CastTargetPrecision;
            var scale = column.CastTargetScale;
            var userSchema = !string.IsNullOrWhiteSpace(column.UserTypeSchemaName) ? column.UserTypeSchemaName : tableColumn?.UserTypeSchemaName;
            var userName = !string.IsNullOrWhiteSpace(column.UserTypeName) ? column.UserTypeName : tableColumn?.UserTypeName;
            var isNullable = column.IsNullable ?? tableColumn?.IsNullable;
            return new ColumnMetadata(normalized, maxLen, precision, scale, isNullable, userSchema, userName);
        }

        if (tableColumn != null)
        {
            var tableMaxLength = NormalizeLength(tableColumn.MaxLength);
            var tablePrecision = NormalizePrecision(tableColumn.Precision);
            var tableScale = NormalizePrecision(tableColumn.Scale);
            var baseType = !string.IsNullOrWhiteSpace(tableColumn.BaseSqlTypeName) ? tableColumn.BaseSqlTypeName : tableColumn.SqlTypeName;
            var formatted = FormatSqlType(baseType, tableMaxLength, tablePrecision, tableScale);
            if (string.IsNullOrWhiteSpace(formatted))
            {
                formatted = !string.IsNullOrWhiteSpace(column.SqlTypeName) ? NormalizeSqlType(column.SqlTypeName) : baseType?.Trim();
            }

            var effectiveSqlType = !string.IsNullOrWhiteSpace(formatted) ? formatted : (!string.IsNullOrWhiteSpace(column.SqlTypeName) ? NormalizeSqlType(column.SqlTypeName) : null);
            if (string.IsNullOrWhiteSpace(effectiveSqlType) && !string.IsNullOrWhiteSpace(baseType))
            {
                effectiveSqlType = baseType.Trim();
            }

            var effectiveMaxLength = column.MaxLength ?? tableMaxLength;
            var effectiveNullable = column.IsNullable ?? tableColumn.IsNullable;
            var effectivePrecision = column.CastTargetPrecision ?? tablePrecision;
            var effectiveScale = column.CastTargetScale ?? tableScale;
            var userSchema = !string.IsNullOrWhiteSpace(column.UserTypeSchemaName) ? column.UserTypeSchemaName : tableColumn.UserTypeSchemaName;
            var userName = !string.IsNullOrWhiteSpace(column.UserTypeName) ? column.UserTypeName : tableColumn.UserTypeName;

            return new ColumnMetadata(
                effectiveSqlType ?? string.Empty,
                effectiveMaxLength,
                effectivePrecision,
                effectiveScale,
                effectiveNullable,
                userSchema,
                userName);
        }

        if (!string.IsNullOrWhiteSpace(column.SqlTypeName))
        {
            var normalized = NormalizeSqlType(column.SqlTypeName);
            return new ColumnMetadata(normalized, column.MaxLength, null, null, column.IsNullable, column.UserTypeSchemaName, column.UserTypeName);
        }

        var fallback = InferColumnMetadataFromPatterns(column);
        if (fallback != null)
        {
            return fallback;
        }

        return null;
    }

    private async Task<Column?> GetTableColumnAsync(
        string? catalog,
        string schema,
        string table,
        string columnName,
        Dictionary<string, Dictionary<string, Column>> tableCache,
        CancellationToken cancellationToken)
    {
        var key = string.IsNullOrWhiteSpace(catalog)
            ? string.Concat(schema, '.', table)
            : string.Concat(catalog, '.', schema, '.', table);
        if (!tableCache.TryGetValue(key, out var map))
        {
            var metadataColumns = await _schemaMetadataProvider.GetTableColumnsAsync(schema, table, catalog, cancellationToken).ConfigureAwait(false);
            var list = metadataColumns.Select(ConvertToColumn).Where(c => c != null).Cast<Column>().ToList();
            map = list.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
            tableCache[key] = map;
        }

        return map.TryGetValue(columnName, out var column) ? column : null;
    }

    private static Column? ConvertToColumn(Schema.ColumnMetadata metadata)
    {
        if (metadata == null)
        {
            return null;
        }

        return new Column
        {
            Name = metadata.Name,
            CatalogName = metadata.Catalog,
            SqlTypeName = metadata.SqlTypeName,
            IsNullable = metadata.IsNullable,
            MaxLength = metadata.MaxLength ?? 0,
            Precision = metadata.Precision,
            Scale = metadata.Scale,
            UserTypeName = metadata.UserTypeName,
            UserTypeSchemaName = metadata.UserTypeSchema,
            HasDefaultValue = metadata.HasDefaultValue,
            DefaultDefinition = metadata.DefaultDefinition,
            DefaultConstraintName = metadata.DefaultConstraintName,
            IsComputed = metadata.IsComputed,
            ComputedDefinition = metadata.ComputedDefinition,
            IsComputedPersisted = metadata.IsComputedPersisted,
            IsRowGuid = metadata.IsRowGuid,
            IsSparse = metadata.IsSparse,
            IsHidden = metadata.IsHidden,
            IsColumnSet = metadata.IsColumnSet,
            GeneratedAlwaysType = metadata.GeneratedAlwaysType
        };
    }

    private static ColumnMetadata? InferColumnMetadataFromPatterns(ProcedureResultColumn column)
    {
        if (column == null)
        {
            return null;
        }

        var aggregateCandidate = column.AggregateFunction;
        if (string.IsNullOrWhiteSpace(aggregateCandidate))
        {
            aggregateCandidate = TryExtractFunctionName(column.RawExpression);
        }

        if (!string.IsNullOrWhiteSpace(aggregateCandidate))
        {
            var aggregate = aggregateCandidate.Trim();
            if (aggregate.Length > 0)
            {
                if (aggregate.Equals("count", StringComparison.OrdinalIgnoreCase))
                {
                    return new ColumnMetadata("int", null, null, null, column.IsNullable ?? false, null, null);
                }

                if (aggregate.Equals("count_big", StringComparison.OrdinalIgnoreCase))
                {
                    return new ColumnMetadata("bigint", null, null, null, column.IsNullable ?? false, null, null);
                }

                if (aggregate.Equals("sum", StringComparison.OrdinalIgnoreCase))
                {
                    var sumMetadata = ResolveAggregateColumnMetadata(column, AggregateTypeRules.InferSum, propagateUserType: false, defaultNullable: true);
                    if (sumMetadata != null)
                    {
                        return sumMetadata;
                    }

                    return new ColumnMetadata("decimal(38,10)", null, 38, 10, column.IsNullable ?? true, null, null);
                }

                if (aggregate.Equals("avg", StringComparison.OrdinalIgnoreCase))
                {
                    var avgMetadata = ResolveAggregateColumnMetadata(column, AggregateTypeRules.InferAverage, propagateUserType: false, defaultNullable: true);
                    if (avgMetadata != null)
                    {
                        return avgMetadata;
                    }

                    return new ColumnMetadata("decimal(38,6)", null, 38, 6, column.IsNullable ?? true, null, null);
                }

                if (aggregate.Equals("min", StringComparison.OrdinalIgnoreCase) || aggregate.Equals("max", StringComparison.OrdinalIgnoreCase))
                {
                    var minMaxMetadata = ResolveAggregateColumnMetadata(column, AggregateTypeRules.InferMinMax, propagateUserType: true, defaultNullable: column.IsNullable ?? true);
                    if (minMaxMetadata != null)
                    {
                        return minMaxMetadata;
                    }

                    if (column.HasIntegerLiteral)
                    {
                        return new ColumnMetadata("int", null, null, null, column.IsNullable ?? true, column.UserTypeSchemaName, column.UserTypeName);
                    }

                    if (column.HasDecimalLiteral)
                    {
                        return new ColumnMetadata("decimal(38,6)", null, 38, 6, column.IsNullable ?? true, column.UserTypeSchemaName, column.UserTypeName);
                    }
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(column.RawExpression))
        {
            var raw = column.RawExpression.Trim();
            if (raw.StartsWith("EXISTS", StringComparison.OrdinalIgnoreCase))
            {
                return new ColumnMetadata("bit", null, null, null, false, null, null);
            }

            if (LooksLikeBooleanCase(raw))
            {
                return new ColumnMetadata("bit", null, null, null, true, null, null);
            }

            if (raw.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                return new ColumnMetadata("int", null, null, null, true, null, null);
            }
        }

        return null;
    }

    private static ColumnMetadata? ResolveAggregateColumnMetadata(
        ProcedureResultColumn column,
        Func<string?, string?, int?, int?, int?, AggregateTypeRules.AggregateSqlType> infer,
        bool propagateUserType,
        bool? defaultNullable)
    {
        if (column == null)
        {
            return null;
        }

        var baseType = NormalizeSqlType(column.CastTargetType ?? column.SqlTypeName ?? string.Empty);
        if (string.IsNullOrWhiteSpace(baseType))
        {
            baseType = null;
        }

        var precision = column.CastTargetPrecision;
        var scale = column.CastTargetScale;
        var length = column.CastTargetLength ?? column.MaxLength;
        var formattedOperand = BuildAggregateOperandType(baseType, precision, scale, length);
        var inference = infer(baseType, formattedOperand, precision, scale, length);

        if (!inference.HasValue)
        {
            return null;
        }

        var formattedType = inference.FormattedType ?? inference.BaseType;
        if (string.IsNullOrWhiteSpace(formattedType))
        {
            return null;
        }

        var maxLength = inference.Length ?? length;
        var precisionResult = inference.Precision ?? precision;
        var scaleResult = inference.Scale ?? scale;

        bool? isNullable = column.IsNullable;
        if (!isNullable.HasValue && inference.ForceNullable)
        {
            isNullable = true;
        }
        else if (!isNullable.HasValue && defaultNullable.HasValue)
        {
            isNullable = defaultNullable;
        }

        var userSchema = propagateUserType ? column.UserTypeSchemaName : null;
        var userName = propagateUserType ? column.UserTypeName : null;

        return new ColumnMetadata(formattedType, maxLength, precisionResult, scaleResult, isNullable, userSchema, userName);
    }

    private static string? BuildAggregateOperandType(string? baseType, int? precision, int? scale, int? length)
    {
        if (string.IsNullOrWhiteSpace(baseType))
        {
            return null;
        }

        return baseType switch
        {
            "decimal" or "numeric" => BuildDecimalTypeName(baseType, precision, scale),
            "float" => FormatFloat(baseType, length),
            _ => length.HasValue && length.Value > 0
                ? string.Concat(baseType, "(", length.Value.ToString(CultureInfo.InvariantCulture), ")")
                : baseType
        };
    }

    private static string? BuildDecimalTypeName(string baseType, int? precision, int? scale)
    {
        if (!precision.HasValue)
        {
            return null;
        }

        var effectiveScale = Math.Max(scale ?? 0, 0);
        return string.Concat(baseType, "(", precision.Value.ToString(CultureInfo.InvariantCulture), ",", effectiveScale.ToString(CultureInfo.InvariantCulture), ")");
    }

    private static string FormatFloat(string baseType, int? length)
    {
        if (!length.HasValue || length.Value <= 0)
        {
            return baseType;
        }

        return string.Concat(baseType, "(", length.Value.ToString(CultureInfo.InvariantCulture), ")");
    }

    private static string NormalizeSqlType(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var trimmed = raw.Trim().ToLowerInvariant();
        var parenIndex = trimmed.IndexOf('(');
        return parenIndex > 0 ? trimmed[..parenIndex] : trimmed;
    }

    private static int? NormalizeLength(int? value)
    {
        if (!value.HasValue)
        {
            return null;
        }

        var val = value.Value;
        if (val <= 0)
        {
            return null;
        }

        if (val == int.MaxValue)
        {
            return -1;
        }

        return val;
    }

    private static int? NormalizePrecision(int? value)
    {
        if (!value.HasValue)
        {
            return null;
        }

        var val = value.Value;
        return val <= 0 ? null : val;
    }

    private static string FormatSqlType(string? baseType, int? maxLength, int? precision, int? scale)
    {
        if (string.IsNullOrWhiteSpace(baseType))
        {
            return string.Empty;
        }

        var normalized = baseType.Trim().ToLowerInvariant();
        return normalized switch
        {
            "decimal" or "numeric" => precision.HasValue ? $"{normalized}({precision.Value},{scale ?? 0})" : normalized,
            "varchar" or "nvarchar" or "varbinary" or "char" or "nchar" or "binary" =>
                maxLength.HasValue ? (maxLength.Value < 0 ? $"{normalized}(max)" : $"{normalized}({maxLength.Value})") : $"{normalized}(max)",
            "datetime2" or "datetimeoffset" or "time" => scale.HasValue ? $"{normalized}({scale.Value})" : normalized,
            _ => normalized
        };
    }

    private static bool LooksLikeBooleanCase(string rawExpression)
    {
        if (string.IsNullOrWhiteSpace(rawExpression))
        {
            return false;
        }

        if (!rawExpression.StartsWith("CASE", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var text = rawExpression;
        var hasThenOneElseZero = text.IndexOf(" THEN 1", StringComparison.OrdinalIgnoreCase) >= 0 && text.IndexOf(" ELSE 0", StringComparison.OrdinalIgnoreCase) >= 0;
        var hasThenZeroElseOne = text.IndexOf(" THEN 0", StringComparison.OrdinalIgnoreCase) >= 0 && text.IndexOf(" ELSE 1", StringComparison.OrdinalIgnoreCase) >= 0;
        return hasThenOneElseZero || hasThenZeroElseOne;
    }

    private static string? TryExtractFunctionName(string? rawExpression)
    {
        if (string.IsNullOrWhiteSpace(rawExpression))
        {
            return null;
        }

        var match = Regex.Match(rawExpression, "^\\s*([A-Za-z0-9_]+)\\s*\\(");
        return match.Success ? match.Groups[1].Value : null;
    }

    private static bool HasEffectiveTypeMetadata(ProcedureResultColumn column)
    {
        if (column == null)
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(column.SqlTypeName) || !string.IsNullOrWhiteSpace(column.CastTargetType))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(column.UserTypeName))
        {
            return true;
        }

        return false;
    }

    private static string? CombinePath(string? parent, string? child)
    {
        if (string.IsNullOrWhiteSpace(child))
        {
            return parent;
        }

        if (string.IsNullOrWhiteSpace(parent))
        {
            return child;
        }

        return string.Concat(parent, ".", child);
    }

    private void ApplySnapshotColumnMetadata(string? snapshotFile, ProcedureModel? procedure)
    {
        if (procedure?.ResultSets == null || procedure.ResultSets.Count == 0)
        {
            return;
        }

        var snapshotPath = ResolveSnapshotProcedurePath(snapshotFile);
        if (string.IsNullOrWhiteSpace(snapshotPath) || !File.Exists(snapshotPath))
        {
            if (_console.IsVerbose)
            {
                var missingLabel = string.IsNullOrWhiteSpace(snapshotPath) ? snapshotFile ?? "(unknown)" : snapshotPath;
                _console.Verbose($"[snapshot-analyze] Snapshot metadata not found at {missingLabel}; proceeding without reuse.");
            }

            return;
        }

        IReadOnlyDictionary<int, Dictionary<string, SnapshotColumnMetadata>> metadataByResultSet;
        try
        {
            metadataByResultSet = LoadSnapshotColumnMetadata(snapshotPath);
        }
        catch
        {
            return;
        }

        if (metadataByResultSet.Count == 0)
        {
            return;
        }

        if (_console.IsVerbose)
        {
            var totalColumns = metadataByResultSet.Sum(static pair => pair.Value.Count);
            _console.Verbose($"[snapshot-analyze] Reusing {totalColumns} cached column metadata entries from {snapshotFile}.");
        }

        for (var index = 0; index < procedure.ResultSets.Count; index++)
        {
            if (!metadataByResultSet.TryGetValue(index, out var columnMap) || columnMap.Count == 0)
            {
                continue;
            }

            var resultSet = procedure.ResultSets[index];
            ApplySnapshotColumnMetadataRecursive(resultSet?.Columns, columnMap, parentPath: null);
        }
    }

    private static string? ResolveSnapshotProcedurePath(string? snapshotFile)
    {
        if (string.IsNullOrWhiteSpace(snapshotFile))
        {
            return null;
        }

        return DirectoryUtils.GetWorkingDirectory(".xtraq", "snapshots", "procedures", snapshotFile);
    }

    private static IReadOnlyDictionary<int, Dictionary<string, SnapshotColumnMetadata>> LoadSnapshotColumnMetadata(string path)
    {
        var map = new Dictionary<int, Dictionary<string, SnapshotColumnMetadata>>();

        using var stream = File.OpenRead(path);
        using var document = JsonDocument.Parse(stream);
        if (!document.RootElement.TryGetProperty("ResultSets", out var resultSets) || resultSets.ValueKind != JsonValueKind.Array)
        {
            return map;
        }

        var index = 0;
        foreach (var resultSet in resultSets.EnumerateArray())
        {
            if (resultSet.ValueKind != JsonValueKind.Object)
            {
                index++;
                continue;
            }

            if (!resultSet.TryGetProperty("Columns", out var columnsElement) || columnsElement.ValueKind != JsonValueKind.Array)
            {
                index++;
                continue;
            }

            var columnMap = new Dictionary<string, SnapshotColumnMetadata>(StringComparer.OrdinalIgnoreCase);
            CollectSnapshotColumnMetadata(columnsElement, columnMap, parentPath: null);

            if (columnMap.Count > 0)
            {
                map[index] = columnMap;
            }

            index++;
        }

        return map;
    }

    private static void CollectSnapshotColumnMetadata(JsonElement columnsElement, Dictionary<string, SnapshotColumnMetadata> map, string? parentPath)
    {
        foreach (var columnElement in columnsElement.EnumerateArray())
        {
            if (columnElement.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var name = TryGetString(columnElement, "Name");
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            var path = CombinePath(parentPath, name);
            var typeRef = TryGetString(columnElement, "TypeRef");
            var sqlType = NormalizeOptionalSqlType(TryGetString(columnElement, "SqlTypeName"));
            var metadata = new SnapshotColumnMetadata
            {
                TypeRef = typeRef,
                SqlTypeName = sqlType,
                IsNullable = ReadNullableBool(columnElement, "IsNullable"),
                MaxLength = TryGetInt32(columnElement, "MaxLength"),
                Precision = TryGetInt32(columnElement, "Precision"),
                Scale = TryGetInt32(columnElement, "Scale"),
                ReturnsJson = ReadNullableBool(columnElement, "ReturnsJson"),
                ReturnsJsonArray = ReadNullableBool(columnElement, "ReturnsJsonArray"),
                IsNestedJson = ReadNullableBool(columnElement, "IsNestedJson"),
                JsonRootProperty = TryGetString(columnElement, "JsonRootProperty"),
                JsonElementClrType = TryGetString(columnElement, "JsonElementClrType"),
                JsonElementSqlType = TryGetString(columnElement, "JsonElementSqlType"),
                UserTypeSchema = TryGetString(columnElement, "UserTypeSchemaName") ?? TryGetString(columnElement, "UserTypeSchema"),
                UserTypeName = TryGetString(columnElement, "UserTypeName")
            };

            if (!string.IsNullOrWhiteSpace(typeRef))
            {
                var (schema, namePart) = SplitTypeRef(typeRef);
                if (!string.IsNullOrWhiteSpace(schema) && !string.IsNullOrWhiteSpace(namePart))
                {
                    if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
                    {
                        metadata.SqlTypeName ??= NormalizeOptionalSqlType(namePart);
                    }
                    else
                    {
                        metadata.UserTypeSchema ??= schema;
                        metadata.UserTypeName ??= namePart;
                    }
                }
            }

            if (!string.IsNullOrWhiteSpace(path))
            {
                map[path] = metadata;
            }

            if (columnElement.TryGetProperty("Columns", out var nested) && nested.ValueKind == JsonValueKind.Array)
            {
                var nextPath = string.IsNullOrWhiteSpace(path) ? parentPath : path;
                CollectSnapshotColumnMetadata(nested, map, nextPath);
            }
        }
    }

    private void ApplySnapshotColumnMetadataRecursive(IReadOnlyList<ProcedureResultColumn>? columns, Dictionary<string, SnapshotColumnMetadata> columnMap, string? parentPath)
    {
        if (columns == null || columns.Count == 0)
        {
            return;
        }

        foreach (var column in columns)
        {
            if (column == null)
            {
                continue;
            }

            var key = string.IsNullOrWhiteSpace(parentPath) ? (column.Name ?? string.Empty) : CombinePath(parentPath, column.Name);
            if (!string.IsNullOrWhiteSpace(key) && columnMap.TryGetValue(key, out var metadata))
            {
                MergeSnapshotMetadata(column, metadata);
            }

            if (column.Columns != null && column.Columns.Count > 0)
            {
                ApplySnapshotColumnMetadataRecursive(column.Columns, columnMap, key);
            }
        }
    }

    private void MergeSnapshotMetadata(ProcedureResultColumn column, SnapshotColumnMetadata metadata)
    {
        if (!HasEffectiveTypeMetadata(column))
        {
            if (!string.IsNullOrWhiteSpace(metadata.SqlTypeName))
            {
                column.SqlTypeName ??= NormalizeSqlType(metadata.SqlTypeName);
            }

            if (!string.IsNullOrWhiteSpace(metadata.TypeRef))
            {
                var (schema, name) = SplitTypeRef(metadata.TypeRef);
                if (!string.IsNullOrWhiteSpace(schema) && !string.IsNullOrWhiteSpace(name))
                {
                    if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
                    {
                        column.SqlTypeName ??= NormalizeSqlType(name);
                    }
                    else
                    {
                        column.UserTypeSchemaName ??= schema;
                        column.UserTypeName ??= name;
                    }
                }
            }
        }

        if (string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(metadata.UserTypeSchema))
        {
            column.UserTypeSchemaName = metadata.UserTypeSchema;
        }

        if (string.IsNullOrWhiteSpace(column.UserTypeName) && !string.IsNullOrWhiteSpace(metadata.UserTypeName))
        {
            column.UserTypeName = metadata.UserTypeName;
        }

        if (!column.MaxLength.HasValue && metadata.MaxLength.HasValue)
        {
            column.MaxLength = metadata.MaxLength;
        }

        if (!column.CastTargetPrecision.HasValue && metadata.Precision.HasValue)
        {
            column.CastTargetPrecision = metadata.Precision;
        }

        if (!column.CastTargetScale.HasValue && metadata.Scale.HasValue)
        {
            column.CastTargetScale = metadata.Scale;
        }

        if (!column.IsNullable.HasValue && metadata.IsNullable.HasValue)
        {
            column.IsNullable = metadata.IsNullable;
        }

        if (metadata.ReturnsJson.HasValue && column.ReturnsJson is null)
        {
            column.ReturnsJson = metadata.ReturnsJson;
        }

        if (metadata.ReturnsJsonArray.HasValue && column.ReturnsJsonArray is null)
        {
            column.ReturnsJsonArray = metadata.ReturnsJsonArray;
        }

        if (metadata.IsNestedJson.HasValue && column.IsNestedJson is null)
        {
            column.IsNestedJson = metadata.IsNestedJson;
        }

        if (!string.IsNullOrWhiteSpace(metadata.JsonRootProperty) && string.IsNullOrWhiteSpace(column.JsonRootProperty))
        {
            column.JsonRootProperty = metadata.JsonRootProperty;
        }

        if (string.IsNullOrWhiteSpace(column.JsonElementClrType) && !string.IsNullOrWhiteSpace(metadata.JsonElementClrType))
        {
            column.JsonElementClrType = metadata.JsonElementClrType;
        }

        if (string.IsNullOrWhiteSpace(column.JsonElementSqlType) && !string.IsNullOrWhiteSpace(metadata.JsonElementSqlType))
        {
            column.JsonElementSqlType = metadata.JsonElementSqlType;
        }
    }

    private static string? NormalizeOptionalSqlType(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : NormalizeSqlType(value);
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.ToString(),
            _ => null
        };
    }

    private static int? TryGetInt32(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value) || value.ValueKind != JsonValueKind.Number)
        {
            return null;
        }

        return value.TryGetInt32(out var parsed) ? parsed : (int?)null;
    }

    private static bool? ReadNullableBool(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => value.TryGetInt32(out var numeric) ? numeric != 0 : (bool?)null,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) ? parsed : (bool?)null,
            _ => null
        };
    }

    /// <summary>
    /// Ensures JSON array columns carry scalar element SQL and CLR metadata when available.
    /// </summary>
    /// <param name="column">The JSON-returning column to inspect.</param>
    private static void EnsureJsonArrayElementMetadata(ProcedureResultColumn column)
    {
        if (column?.ReturnsJsonArray != true)
        {
            return;
        }

        var elementCandidate = ExtractJsonArrayElementCandidate(column);

        var elementSqlType = string.IsNullOrWhiteSpace(column.JsonElementSqlType)
            ? TryResolveElementSqlType(elementCandidate)
            : column.JsonElementSqlType;

        if (string.IsNullOrWhiteSpace(column.JsonElementSqlType) && !string.IsNullOrWhiteSpace(elementSqlType))
        {
            column.JsonElementSqlType = elementSqlType;
        }

        if (!string.IsNullOrWhiteSpace(column.JsonElementClrType))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(elementSqlType))
        {
            return;
        }

        var elementNullable = elementCandidate?.IsNullable ?? true;
        column.JsonElementClrType = Xtraq.Metadata.SqlClrTypeMapper.Map(elementSqlType, elementNullable);
    }

    /// <summary>
    /// Locates the scalar element column describing a JSON array item emitted by the analyzer.
    /// </summary>
    /// <param name="column">The JSON array container column.</param>
    /// <returns>The scalar element column when it can be resolved; otherwise <c>null</c>.</returns>
    private static ProcedureResultColumn? ExtractJsonArrayElementCandidate(ProcedureResultColumn column)
    {
        if (column?.Columns is not { Count: 1 })
        {
            return null;
        }

        var candidate = column.Columns[0];
        if (candidate == null)
        {
            return null;
        }

        if (candidate.ReturnsJson == true || candidate.ReturnsJsonArray == true)
        {
            return null;
        }

        if (candidate.Columns is { Count: > 0 })
        {
            return null;
        }

        return candidate;
    }

    /// <summary>
    /// Attempts to resolve an SQL type declaration for a JSON element based on analyzer metadata.
    /// </summary>
    /// <param name="candidate">The column describing the JSON array element.</param>
    /// <returns>The SQL type declaration if it can be inferred; otherwise <c>null</c>.</returns>
    private static string? TryResolveElementSqlType(ProcedureResultColumn? candidate)
    {
        if (candidate == null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(candidate.CastTargetType))
        {
            return candidate.CastTargetType;
        }

        if (!string.IsNullOrWhiteSpace(candidate.SqlTypeName))
        {
            return candidate.SqlTypeName;
        }

        if (!string.IsNullOrWhiteSpace(candidate.UserTypeName))
        {
            return string.IsNullOrWhiteSpace(candidate.UserTypeSchemaName)
                ? candidate.UserTypeName
                : string.Concat(candidate.UserTypeSchemaName, '.', candidate.UserTypeName);
        }

        return null;
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

    private void RegisterUnresolvedColumn(ProcedureResultColumn column, string descriptorLabel, string? warningPath, ISet<string> unresolvedColumns)
    {
        if (string.IsNullOrWhiteSpace(warningPath))
        {
            return;
        }

        var key = string.Concat(descriptorLabel, "|", warningPath);
        if (!unresolvedColumns.Add(key))
        {
            return;
        }

        var sourceDetails = BuildColumnSourceDetails(column);
        var detail = $"[snapshot-analyze] Column '{warningPath}' in {descriptorLabel}{sourceDetails} still lacks SQL type after full metadata enrichment; snapshot falls back to nvarchar(max).";
        _console.WarnBuffered("[snapshot-analyze] unresolved SQL type column (fallback to nvarchar(max))", detail);
    }

    private static string BuildWarningPath(string? currentPath, ProcedureResultColumn column)
    {
        if (!string.IsNullOrWhiteSpace(currentPath))
        {
            return currentPath;
        }

        if (!string.IsNullOrWhiteSpace(column?.Name))
        {
            return column.Name;
        }

        return "(unnamed)";
    }

    private static string BuildColumnSourceDetails(ProcedureResultColumn column)
    {
        if (column == null)
        {
            return string.Empty;
        }

        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(column.SourceSchema))
        {
            parts.Add(column.SourceSchema);
        }
        if (!string.IsNullOrWhiteSpace(column.SourceTable))
        {
            parts.Add(column.SourceTable);
        }

        var location = parts.Count > 0 ? string.Join('.', parts) : null;
        if (!string.IsNullOrWhiteSpace(column.SourceColumn))
        {
            location = string.IsNullOrWhiteSpace(location)
                ? column.SourceColumn
                : string.Concat(location, '.', column.SourceColumn);
        }

        if (!string.IsNullOrWhiteSpace(location))
        {
            return string.Concat(" (source: ", location, ")");
        }

        if (!string.IsNullOrWhiteSpace(column.SourceAlias))
        {
            return string.Concat(" (source alias: ", column.SourceAlias, ")");
        }

        return string.Empty;
    }

    private static string FormatProcedureLabel(ProcedureDescriptor descriptor)
    {
        if (descriptor == null)
        {
            return "(unknown procedure)";
        }

        var schema = descriptor.Schema?.Trim();
        var name = descriptor.Name?.Trim();

        if (string.IsNullOrWhiteSpace(schema))
        {
            return string.IsNullOrWhiteSpace(name) ? "(unknown procedure)" : name;
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            return schema;
        }

        return string.Concat(schema, ".", name);
    }

    private sealed record ColumnMetadata(
        string SqlType,
        int? MaxLength,
        int? Precision,
        int? Scale,
        bool? IsNullable,
        string? UserTypeSchema,
        string? UserTypeName
    );

    private sealed class SnapshotColumnMetadata
    {
        public string? TypeRef { get; set; }
        public string? SqlTypeName { get; set; }
        public bool? IsNullable { get; set; }
        public int? MaxLength { get; set; }
        public int? Precision { get; set; }
        public int? Scale { get; set; }
        public bool? ReturnsJson { get; set; }
        public bool? ReturnsJsonArray { get; set; }
        public bool? IsNestedJson { get; set; }
        public string? JsonRootProperty { get; set; }
        public string? JsonElementClrType { get; set; }
        public string? JsonElementSqlType { get; set; }
        public string? UserTypeSchema { get; set; }
        public string? UserTypeName { get; set; }
    }
}
