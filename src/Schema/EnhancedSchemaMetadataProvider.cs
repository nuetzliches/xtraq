using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Writers;
using Xtraq.Utils;

namespace Xtraq.Schema;

/// <summary>
/// Enhanced schema metadata provider that uses snapshot index metadata for offline build scenarios.
/// Falls back to database queries when snapshot index is not available.
/// </summary>
internal interface IEnhancedSchemaMetadataProvider
{
    /// <summary>
    /// Resolves table column metadata, preferring snapshot index over database queries.
    /// </summary>
    Task<ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all columns for a specific table, with offline-first approach.
    /// </summary>
    Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves function return metadata (scalar or table-valued) with offline-first approach.
    /// </summary>
    Task<FunctionReturnMetadata?> ResolveFunctionReturnAsync(string schema, string functionName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if offline mode is available (snapshot index exists).
    /// </summary>
    Task<bool> IsOfflineModeAvailableAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Column metadata with source information for code generation.
/// </summary>
internal sealed class ColumnMetadata
{
    public string Name { get; init; } = string.Empty;
    public string? Catalog { get; init; }
    public string SqlTypeName { get; init; } = string.Empty;
    public bool IsNullable { get; init; }
    public int? MaxLength { get; init; }
    public int? Precision { get; init; }
    public int? Scale { get; init; }
    public string? UserTypeSchema { get; init; }
    public string? UserTypeName { get; init; }
    public bool IsFromSnapshot { get; init; }
    public bool HasDefaultValue { get; init; }
    public string? DefaultDefinition { get; init; }
    public string? DefaultConstraintName { get; init; }
    public bool IsComputed { get; init; }
    public string? ComputedDefinition { get; init; }
    public bool IsComputedPersisted { get; init; }
    public bool IsRowGuid { get; init; }
    public bool IsSparse { get; init; }
    public bool IsHidden { get; init; }
    public bool IsColumnSet { get; init; }
    public string? GeneratedAlwaysType { get; init; }
}

/// <summary>
/// Function return metadata containing scalar return information or table-valued marker.
/// </summary>
internal sealed class FunctionReturnMetadata
{
    public string Schema { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public bool IsTableValued { get; init; }
    public string? SqlTypeName { get; init; }
    public int? MaxLength { get; init; }
    public int? Precision { get; init; }
    public int? Scale { get; init; }
    public bool? IsNullable { get; init; }
    public string? UserTypeSchema { get; init; }
    public string? UserTypeName { get; init; }
}

/// <summary>
/// Implementation that prioritizes snapshot index metadata over database queries.
/// Enables offline build scenarios while maintaining database fallback capability.
/// </summary>
internal sealed class EnhancedSchemaMetadataProvider : IEnhancedSchemaMetadataProvider
{
    private readonly ISnapshotIndexMetadataProvider _snapshotIndexProvider;
    private readonly DbContext? _dbContext;
    private readonly IConsoleService _console;
    private readonly ConcurrentDictionary<string, FunctionReturnMetadata?> _functionReturnCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SnapshotFunction?> _snapshotFunctionCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public EnhancedSchemaMetadataProvider(
        ISnapshotIndexMetadataProvider snapshotIndexProvider,
        DbContext? dbContext,
        IConsoleService console)
    {
        _snapshotIndexProvider = snapshotIndexProvider ?? throw new ArgumentNullException(nameof(snapshotIndexProvider));
        _dbContext = dbContext;
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task<ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(columnName))
        {
            return null;
        }

        var snapshotColumn = await ResolveFromSnapshotAsync(schema, tableName, columnName, catalog, cancellationToken).ConfigureAwait(false);
        if (snapshotColumn != null)
        {
            return snapshotColumn;
        }

        // Try snapshot index first
        try
        {
            var indexColumn = await _snapshotIndexProvider.GetColumnMetadataAsync(schema, tableName, columnName, cancellationToken).ConfigureAwait(false);
            if (indexColumn == null && !string.IsNullOrWhiteSpace(catalog))
            {
                var qualifiedSchema = BuildQualifiedSchema(catalog!, schema);
                indexColumn = await _snapshotIndexProvider.GetColumnMetadataAsync(qualifiedSchema, tableName, columnName, cancellationToken).ConfigureAwait(false);
            }
            if (indexColumn != null)
            {
                _console.Verbose($"[enhanced-schema] Resolved {schema}.{tableName}.{columnName} from snapshot index");
                return MapIndexColumn(indexColumn, columnName);
            }

            var indexColumns = await _snapshotIndexProvider.GetTableColumnsMetadataAsync(schema, tableName, cancellationToken).ConfigureAwait(false);
            if ((indexColumns == null || indexColumns.Count == 0) && !string.IsNullOrWhiteSpace(catalog))
            {
                var qualifiedSchema = BuildQualifiedSchema(catalog!, schema);
                indexColumns = await _snapshotIndexProvider.GetTableColumnsMetadataAsync(qualifiedSchema, tableName, cancellationToken).ConfigureAwait(false);
            }
            var fallback = indexColumns?.FirstOrDefault(entry => MatchesColumn(entry, columnName));
            if (fallback != null)
            {
                _console.Verbose($"[enhanced-schema] Resolved {schema}.{tableName}.{columnName} from snapshot index (table scan)");
                return MapIndexColumn(fallback, columnName);
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Snapshot index lookup failed for {schema}.{tableName}.{columnName}: {ex.Message}");
        }

        // Fallback to database query
        if (_dbContext != null)
        {
            try
            {
                var dbColumn = await ResolveFromDatabaseAsync(schema, tableName, columnName, catalog, cancellationToken).ConfigureAwait(false);
                if (dbColumn != null)
                {
                    _console.Verbose($"[enhanced-schema] Resolved {schema}.{tableName}.{columnName} from database");
                    return dbColumn;
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[enhanced-schema] Database lookup failed for {schema}.{tableName}.{columnName}: {ex.Message}");
            }
        }

        _console.Verbose($"[enhanced-schema] Could not resolve {schema}.{tableName}.{columnName} from any source");
        return null;
    }

    public async Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(tableName))
        {
            return Array.Empty<ColumnMetadata>();
        }

        var snapshotColumns = await LoadColumnsFromSnapshotAsync(schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
        if (snapshotColumns.Count > 0)
        {
            return snapshotColumns;
        }
        var resultMap = new Dictionary<string, ColumnMetadata>(StringComparer.OrdinalIgnoreCase);

        // Try snapshot index first
        try
        {
            var indexColumns = await _snapshotIndexProvider.GetTableColumnsMetadataAsync(schema, tableName, cancellationToken).ConfigureAwait(false);
            if ((indexColumns == null || indexColumns.Count == 0) && !string.IsNullOrWhiteSpace(catalog))
            {
                var qualifiedSchema = BuildQualifiedSchema(catalog!, schema);
                indexColumns = await _snapshotIndexProvider.GetTableColumnsMetadataAsync(qualifiedSchema, tableName, cancellationToken).ConfigureAwait(false);
            }
            if (indexColumns != null && indexColumns.Count > 0)
            {
                foreach (var indexColumn in indexColumns)
                {
                    var mapped = MapIndexColumn(indexColumn, indexColumn.SourceColumn ?? indexColumn.Name ?? string.Empty);
                    if (!string.IsNullOrWhiteSpace(mapped.Name))
                    {
                        resultMap[mapped.Name] = mapped;
                    }
                }

                if (resultMap.Count > 0)
                {
                    _console.Verbose($"[enhanced-schema] Seeded {resultMap.Count} column(s) for {schema}.{tableName} from snapshot index");
                }
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Snapshot index table lookup failed for {schema}.{tableName}: {ex.Message}");
        }

        // Fallback to database query
        if (_dbContext != null)
        {
            try
            {
                var dbColumns = await GetTableColumnsFromDatabaseAsync(schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
                foreach (var column in dbColumns)
                {
                    if (!string.IsNullOrWhiteSpace(column?.Name))
                    {
                        resultMap[column.Name] = column;
                    }
                }

                if (dbColumns.Count > 0)
                {
                    _console.Verbose($"[enhanced-schema] Added {dbColumns.Count} column(s) for {schema}.{tableName} from database metadata");
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[enhanced-schema] Database table lookup failed for {schema}.{tableName}: {ex.Message}");
            }
        }

        if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            var systemColumns = await GetSystemColumnsAsync(schema, tableName, cancellationToken).ConfigureAwait(false);
            foreach (var column in systemColumns)
            {
                if (!string.IsNullOrWhiteSpace(column?.Name))
                {
                    resultMap[column.Name] = column;
                }
            }

            if (systemColumns.Count > 0)
            {
                _console.Verbose($"[enhanced-schema] Enriched {schema}.{tableName} with {systemColumns.Count} system column(s)");
            }
        }

        if (resultMap.Count > 0)
        {
            _console.Verbose($"[enhanced-schema] Aggregated {resultMap.Count} column(s) for {schema}.{tableName}");
            return resultMap.Values.ToList();
        }

        _console.Verbose($"[enhanced-schema] No columns found for {schema}.{tableName} from any source");
        return Array.Empty<ColumnMetadata>();
    }

    public async Task<FunctionReturnMetadata?> ResolveFunctionReturnAsync(string schema, string functionName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(functionName))
        {
            return null;
        }

        var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
        var normalizedName = functionName.Trim();
        var cacheKey = string.Concat(normalizedSchema, ".", normalizedName);

        if (_functionReturnCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        FunctionReturnMetadata? metadata = null;
        try
        {
            metadata = await ResolveFunctionFromSnapshotAsync(normalizedSchema, normalizedName, null, cancellationToken).ConfigureAwait(false);
            if (metadata == null && _dbContext != null)
            {
                metadata = await ResolveFunctionFromDatabaseAsync(normalizedSchema, normalizedName, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _functionReturnCache[cacheKey] = metadata;
        }

        return metadata;
    }

    private async Task<ColumnMetadata?> ResolveFromSnapshotAsync(string schema, string tableName, string columnName, string? catalog, CancellationToken cancellationToken)
    {
        var columns = await LoadColumnsFromSnapshotAsync(schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
        return columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }

    private async Task<IReadOnlyList<ColumnMetadata>> LoadColumnsFromSnapshotAsync(string schema, string tableName, string? catalog, CancellationToken cancellationToken)
    {
        var tablePath = BuildTableSnapshotPath(schema, tableName, catalog);
        if (!string.IsNullOrEmpty(tablePath) && File.Exists(tablePath))
        {
            var tableColumns = await LoadSnapshotTableColumnsAsync(tablePath, schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
            if (tableColumns.Count > 0)
            {
                return tableColumns;
            }
        }

        return await LoadSnapshotFunctionColumnsAsync(schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
    }

    private static ColumnMetadata MapSnapshotColumn(SnapshotTableColumn column, string? catalog)
    {
        var (schema, name) = SplitTypeRef(column.TypeRef);
        var isSystem = string.IsNullOrWhiteSpace(schema) || string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase);

        var normalizedType = isSystem ? NormalizeSqlTypeName(name) : null;

        return new ColumnMetadata
        {
            Name = column.Name,
            Catalog = NormalizeOptional(catalog),
            SqlTypeName = normalizedType ?? string.Empty,
            IsNullable = column.IsNullable ?? false,
            MaxLength = column.MaxLength,
            Precision = column.Precision,
            Scale = column.Scale,
            UserTypeSchema = isSystem ? null : schema,
            UserTypeName = isSystem ? null : name,
            IsFromSnapshot = true,
            HasDefaultValue = column.HasDefaultValue == true,
            DefaultDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.DefaultDefinition),
            DefaultConstraintName = NormalizeOptional(column.DefaultConstraintName),
            IsComputed = column.IsComputed == true,
            ComputedDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.ComputedDefinition),
            IsComputedPersisted = column.IsComputedPersisted == true,
            IsRowGuid = column.IsRowGuid == true,
            IsSparse = column.IsSparse == true,
            IsHidden = column.IsHidden == true,
            IsColumnSet = column.IsColumnSet == true,
            GeneratedAlwaysType = NormalizeGeneratedAlwaysType(column.GeneratedAlwaysType)
        };
    }

    private static ColumnMetadata MapDatabaseColumn(Column column, string? fallbackCatalog)
    {
        if (column == null)
        {
            return new ColumnMetadata
            {
                Name = string.Empty,
                Catalog = NormalizeOptional(fallbackCatalog),
                SqlTypeName = string.Empty,
                IsNullable = false,
                IsFromSnapshot = false
            };
        }

        var normalizedCatalog = NormalizeOptional(column.CatalogName) ?? NormalizeOptional(fallbackCatalog);
        var userTypeSchema = NormalizeOptional(column.UserTypeSchemaName);
        var userTypeName = NormalizeOptional(column.UserTypeName);
        var rawSqlType = string.IsNullOrWhiteSpace(column.SqlTypeName) ? column.BaseSqlTypeName : column.SqlTypeName;
        var normalizedSqlType = NormalizeSqlTypeName(rawSqlType);
        if (!string.IsNullOrWhiteSpace(userTypeName))
        {
            normalizedSqlType = string.Empty;
        }
        else if (string.IsNullOrWhiteSpace(normalizedSqlType) && !string.IsNullOrWhiteSpace(rawSqlType))
        {
            normalizedSqlType = rawSqlType.Trim().ToLowerInvariant();
        }

        var maxLength = NormalizeMaxLength(column.MaxLength);
        var precision = NormalizeNumeric(column.Precision);
        var scale = NormalizeNumeric(column.Scale);

        return new ColumnMetadata
        {
            Name = column.Name,
            Catalog = normalizedCatalog,
            SqlTypeName = normalizedSqlType ?? string.Empty,
            IsNullable = column.IsNullable,
            MaxLength = maxLength,
            Precision = precision,
            Scale = scale,
            UserTypeSchema = userTypeSchema,
            UserTypeName = userTypeName,
            IsFromSnapshot = false,
            HasDefaultValue = column.HasDefaultValue,
            DefaultDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.DefaultDefinition),
            DefaultConstraintName = NormalizeOptional(column.DefaultConstraintName),
            IsComputed = column.IsComputed,
            ComputedDefinition = SnapshotWriterUtilities.NormalizeSqlExpression(column.ComputedDefinition),
            IsComputedPersisted = column.IsComputedPersisted,
            IsRowGuid = column.IsRowGuid,
            IsSparse = column.IsSparse,
            IsHidden = column.IsHidden,
            IsColumnSet = column.IsColumnSet,
            GeneratedAlwaysType = NormalizeGeneratedAlwaysType(column.GeneratedAlwaysType)
        };
    }

    private static ColumnMetadata MapIndexColumn(IndexColumnEntry column, string fallbackName)
    {
        var normalizedType = NormalizeSqlTypeName(column.SqlTypeName);
        var name = !string.IsNullOrWhiteSpace(column.SourceColumn) ? column.SourceColumn! : column.Name ?? fallbackName;

        return new ColumnMetadata
        {
            Name = name ?? string.Empty,
            SqlTypeName = normalizedType ?? string.Empty,
            IsNullable = column.IsNullable,
            MaxLength = column.MaxLength,
            Precision = column.Precision,
            Scale = column.Scale,
            UserTypeSchema = column.UserTypeSchema,
            UserTypeName = column.UserTypeName,
            IsFromSnapshot = true
        };
    }

    private static bool MatchesColumn(IndexColumnEntry entry, string columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
        {
            return false;
        }

        return string.Equals(entry.SourceColumn, columnName, StringComparison.OrdinalIgnoreCase) ||
               string.Equals(entry.Name, columnName, StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildQualifiedSchema(string catalog, string schema)
    {
        var catalogPart = catalog?.Trim();
        var schemaPart = schema?.Trim();
        if (string.IsNullOrWhiteSpace(catalogPart))
        {
            return schemaPart ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(schemaPart))
        {
            return catalogPart;
        }

        return string.Concat(catalogPart, ".", schemaPart);
    }

    private static int? NormalizeMaxLength(int value)
    {
        if (value == 0)
        {
            return null;
        }

        if (value < 0)
        {
            return -1;
        }

        return value;
    }

    private static int? NormalizeNumeric(int? value)
    {
        if (!value.HasValue || value.Value <= 0)
        {
            return null;
        }

        return value.Value;
    }

    private static string? BuildTableSnapshotPath(string schema, string tableName, string? catalog)
    {
        var working = DirectoryUtils.GetWorkingDirectory();
        if (string.IsNullOrWhiteSpace(working))
        {
            return null;
        }

        var sanitizedSchema = NameSanitizer.SanitizeForFile(schema);
        var sanitizedTable = NameSanitizer.SanitizeForFile(tableName);
        if (!string.IsNullOrWhiteSpace(catalog))
        {
            var sanitizedCatalog = NameSanitizer.SanitizeForFile(catalog);
            return Path.Combine(working, ".xtraq", "snapshots", "tables", string.Concat(sanitizedCatalog, ".", sanitizedSchema, ".", sanitizedTable, ".json"));
        }

        return Path.Combine(working, ".xtraq", "snapshots", "tables", string.Concat(sanitizedSchema, ".", sanitizedTable, ".json"));
    }

    private static string? BuildFunctionSnapshotPath(string schema, string functionName, string? catalog)
    {
        var working = DirectoryUtils.GetWorkingDirectory();
        if (string.IsNullOrWhiteSpace(working))
        {
            return null;
        }

        var sanitizedSchema = NameSanitizer.SanitizeForFile(schema);
        var sanitizedFunction = NameSanitizer.SanitizeForFile(functionName);
        if (!string.IsNullOrWhiteSpace(catalog))
        {
            var sanitizedCatalog = NameSanitizer.SanitizeForFile(catalog);
            return Path.Combine(working, ".xtraq", "snapshots", "functions", string.Concat(sanitizedCatalog, ".", sanitizedSchema, ".", sanitizedFunction, ".json"));
        }

        return Path.Combine(working, ".xtraq", "snapshots", "functions", string.Concat(sanitizedSchema, ".", sanitizedFunction, ".json"));
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

    private static string? NormalizeSqlTypeName(string? sqlTypeName)
    {
        if (string.IsNullOrWhiteSpace(sqlTypeName))
        {
            return null;
        }

        return sqlTypeName.Trim().ToLowerInvariant();
    }

    private static string? NormalizeOptional(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string? NormalizeGeneratedAlwaysType(string? value)
    {
        var normalized = NormalizeOptional(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return null;
        }

        return string.Equals(normalized, "NOT_APPLICABLE", StringComparison.OrdinalIgnoreCase) ? null : normalized;
    }

    public async Task<bool> IsOfflineModeAvailableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _snapshotIndexProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            return false;
        }
    }

    private async Task<ColumnMetadata?> ResolveFromDatabaseAsync(string schema, string tableName, string columnName, string? catalog, CancellationToken cancellationToken)
    {
        var columns = await GetTableColumnsFromDatabaseAsync(schema, tableName, catalog, cancellationToken).ConfigureAwait(false);
        var match = columns.FirstOrDefault(c => c != null &&
            !string.IsNullOrWhiteSpace(c.Name) &&
            string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        if (match != null)
        {
            return match;
        }

        if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            var systemColumns = await GetSystemColumnsAsync(schema, tableName, cancellationToken).ConfigureAwait(false);
            return systemColumns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        }

        return null;
    }

    private async Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsFromDatabaseAsync(string schema, string tableName, string? catalog, CancellationToken cancellationToken)
    {
        if (_dbContext == null)
        {
            return Array.Empty<ColumnMetadata>();
        }

        try
        {
            List<Column> dbColumns;
            if (!string.IsNullOrWhiteSpace(catalog))
            {
                dbColumns = await _dbContext.TableColumnsListAsync(catalog!, schema, tableName, cancellationToken).ConfigureAwait(false) ?? new List<Column>();
            }
            else
            {
                dbColumns = await _dbContext.TableColumnsListAsync(schema, tableName, cancellationToken).ConfigureAwait(false) ?? new List<Column>();
            }

            if (dbColumns.Count == 0)
            {
                return Array.Empty<ColumnMetadata>();
            }

            var fallbackCatalog = NormalizeOptional(catalog);
            return dbColumns
                .Where(static column => column != null && !string.IsNullOrWhiteSpace(column.Name))
                .Select(column => MapDatabaseColumn(column, fallbackCatalog))
                .ToList();
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Database table lookup failed for {schema}.{tableName}: {ex.Message}");
            return Array.Empty<ColumnMetadata>();
        }
    }

    private async Task<IReadOnlyList<ColumnMetadata>> LoadSnapshotTableColumnsAsync(string path, string schema, string tableName, string? catalog, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = File.OpenRead(path);
            var snapshotTable = await JsonSerializer.DeserializeAsync<SnapshotTable>(stream, _jsonOptions, cancellationToken).ConfigureAwait(false);
            if (snapshotTable?.Columns == null || snapshotTable.Columns.Count == 0)
            {
                return Array.Empty<ColumnMetadata>();
            }

            var tableCatalog = !string.IsNullOrWhiteSpace(snapshotTable.Catalog) ? snapshotTable.Catalog : catalog;

            var mapped = snapshotTable.Columns
                .Where(column => column != null && !string.IsNullOrWhiteSpace(column.Name))
                .Select(column => MapSnapshotColumn(column, tableCatalog))
                .ToList();

            if (mapped.Count > 0)
            {
                _console.Verbose($"[enhanced-schema] Resolved {mapped.Count} columns for {schema}.{tableName} from snapshot tables");
            }

            return mapped;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Snapshot table lookup failed for {schema}.{tableName}: {ex.Message}");
            return Array.Empty<ColumnMetadata>();
        }
    }

    private async Task<IReadOnlyList<ColumnMetadata>> GetSystemColumnsAsync(string schema, string tableName, CancellationToken cancellationToken)
    {
        if (_dbContext == null)
        {
            return Array.Empty<ColumnMetadata>();
        }

        var statement = BuildSystemObjectQuery(schema, tableName);
        const string sql = """
SELECT
    dfrs.name AS ColumnName,
    dfrs.system_type_name AS SystemTypeName,
    dfrs.is_nullable AS IsNullable,
    dfrs.column_ordinal AS ColumnOrdinal,
    dfrs.max_length AS MaxLength,
    dfrs.precision AS Precision,
    dfrs.scale AS Scale,
    dfrs.user_type_schema AS UserTypeSchema,
    dfrs.user_type_name AS UserTypeName
FROM sys.dm_exec_describe_first_result_set(@statement, NULL, 0) AS dfrs
WHERE dfrs.name IS NOT NULL
ORDER BY dfrs.column_ordinal;
""";

        try
        {
            var parameters = new List<Microsoft.Data.SqlClient.SqlParameter>
            {
                new("@statement", statement)
            };

            var columns = await _dbContext.ListAsync<SystemColumnInfoDto>(
                sql,
                parameters,
                cancellationToken,
                telemetryOperation: "EnhancedSchemaMetadataProvider.SystemColumns",
                telemetryCategory: "Analyzer.SchemaFallback").ConfigureAwait(false);

            if (columns.Count == 0)
            {
                return Array.Empty<ColumnMetadata>();
            }

            var mapped = new List<ColumnMetadata>(columns.Count);

            foreach (var column in columns)
            {
                if (string.IsNullOrWhiteSpace(column.ColumnName))
                {
                    continue;
                }

                var (baseType, parsedMaxLength, parsedPrecision, parsedScale) = ParseSystemTypeName(column.SystemTypeName);
                var normalizedType = NormalizeSqlTypeName(baseType);
                var typeDiscriminator = normalizedType ?? baseType;
                var metadataMaxLength = column.MaxLength.HasValue ? (int?)column.MaxLength.Value : null;
                var metadataPrecision = column.Precision.HasValue ? (int?)column.Precision.Value : null;
                var metadataScale = column.Scale.HasValue ? (int?)column.Scale.Value : null;
                var maxLength = DetermineMaxLength(typeDiscriminator, parsedMaxLength, metadataMaxLength);
                var precision = parsedPrecision ?? metadataPrecision;
                var scale = parsedScale ?? metadataScale;
                var userTypeSchema = NormalizeUserTypeComponent(column.UserTypeSchema);
                var userTypeName = NormalizeUserTypeComponent(column.UserTypeName);
                if (!string.IsNullOrEmpty(userTypeSchema) && string.Equals(userTypeSchema, "sys", StringComparison.OrdinalIgnoreCase))
                {
                    userTypeSchema = null;
                    userTypeName = null;
                }
                var hasUserType = !string.IsNullOrEmpty(userTypeName);

                mapped.Add(new ColumnMetadata
                {
                    Name = column.ColumnName,
                    SqlTypeName = hasUserType ? string.Empty : normalizedType ?? string.Empty,
                    IsNullable = column.IsNullable ?? false,
                    MaxLength = maxLength,
                    Precision = precision,
                    Scale = scale,
                    UserTypeSchema = userTypeSchema,
                    UserTypeName = userTypeName,
                    IsFromSnapshot = false
                });
            }

            if (mapped.Count > 0)
            {
                _console.Verbose($"[enhanced-schema] Resolved {mapped.Count} columns for system object {schema}.{tableName} using live metadata");
            }

            return mapped;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] System column lookup failed for {schema}.{tableName}: {ex.Message}");
            return Array.Empty<ColumnMetadata>();
        }
    }

    private static string BuildSystemObjectQuery(string schema, string tableName)
    {
        var qualifiedSchema = string.IsNullOrWhiteSpace(schema) ? "sys" : schema.Trim();
        var qualifiedName = string.IsNullOrWhiteSpace(tableName) ? string.Empty : tableName.Trim();
        return string.Concat("SELECT TOP 0 * FROM ", QuoteIdentifier(qualifiedSchema), ".", QuoteIdentifier(qualifiedName));
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            return "[]";
        }

        return string.Concat("[", identifier.Replace("]", "]]", StringComparison.Ordinal), "]");
    }

    private static (string BaseType, int? MaxLength, int? Precision, int? Scale) ParseSystemTypeName(string? systemTypeName)
    {
        if (string.IsNullOrWhiteSpace(systemTypeName))
        {
            return (string.Empty, null, null, null);
        }

        var trimmed = systemTypeName.Trim();

        if (trimmed.Equals("sysname", StringComparison.OrdinalIgnoreCase))
        {
            return ("nvarchar", 128, null, null);
        }

        var openParenIndex = trimmed.IndexOf('(');
        if (openParenIndex > 0 && trimmed.EndsWith(")", StringComparison.Ordinal))
        {
            var baseType = trimmed[..openParenIndex].Trim();
            var inner = trimmed[(openParenIndex + 1)..^1];
            var segments = inner.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

            if (segments.Length == 1)
            {
                var token = segments[0];
                if (string.Equals(token, "max", StringComparison.OrdinalIgnoreCase))
                {
                    return (baseType, -1, null, null);
                }

                if (int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lengthValue))
                {
                    return (baseType, lengthValue, null, null);
                }
            }
            else if (segments.Length == 2 && int.TryParse(segments[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var precisionValue))
            {
                var scaleValue = int.TryParse(segments[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedScale)
                    ? parsedScale
                    : (int?)null;
                return (baseType, null, precisionValue, scaleValue);
            }

            return (baseType, null, null, null);
        }

        if (trimmed.Contains('.'))
        {
            var tokens = trimmed.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length == 2)
            {
                return (tokens[1], null, null, null);
            }
        }

        return (trimmed, null, null, null);
    }

    private static int? DetermineMaxLength(string? baseType, int? parsedMaxLength, int? metadataMaxLength)
    {
        if (parsedMaxLength.HasValue)
        {
            return parsedMaxLength.Value < 0 ? -1 : parsedMaxLength;
        }

        if (!metadataMaxLength.HasValue)
        {
            return null;
        }

        if (metadataMaxLength.Value < 0)
        {
            return -1;
        }

        if (!string.IsNullOrWhiteSpace(baseType) &&
            (string.Equals(baseType, "nvarchar", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(baseType, "nchar", StringComparison.OrdinalIgnoreCase)))
        {
            return metadataMaxLength.Value / 2;
        }

        return metadataMaxLength;
    }

    private static string? NormalizeUserTypeComponent(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private async Task<IReadOnlyList<ColumnMetadata>> LoadSnapshotFunctionColumnsAsync(string schema, string functionName, string? catalog, CancellationToken cancellationToken)
    {
        try
        {
            var snapshotFunction = await LoadSnapshotFunctionAsync(schema, functionName, catalog, cancellationToken).ConfigureAwait(false);
            if (snapshotFunction?.Columns == null || snapshotFunction.Columns.Count == 0)
            {
                return Array.Empty<ColumnMetadata>();
            }

            var mapped = snapshotFunction.Columns
                .Where(column => column != null && !string.IsNullOrWhiteSpace(column.Name))
                .Select(MapSnapshotFunctionColumn)
                .ToList();

            if (mapped.Count > 0)
            {
                _console.Verbose($"[enhanced-schema] Resolved {mapped.Count} columns for function {schema}.{functionName} from snapshot metadata");
            }

            return mapped;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Snapshot function lookup failed for {schema}.{functionName}: {ex.Message}");
            return Array.Empty<ColumnMetadata>();
        }
    }

    private static ColumnMetadata MapSnapshotFunctionColumn(SnapshotFunctionColumn column)
    {
        var (schema, name) = SplitTypeRef(column.TypeRef);
        var isSystem = string.IsNullOrWhiteSpace(schema) || string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase);
        var normalizedType = isSystem ? NormalizeSqlTypeName(name) : null;

        return new ColumnMetadata
        {
            Name = column.Name,
            SqlTypeName = normalizedType ?? string.Empty,
            IsNullable = column.IsNullable ?? false,
            MaxLength = column.MaxLength,
            Precision = column.Precision,
            Scale = column.Scale,
            UserTypeSchema = isSystem ? null : schema,
            UserTypeName = isSystem ? null : name,
            IsFromSnapshot = true
        };
    }

    private async Task<FunctionReturnMetadata?> ResolveFunctionFromSnapshotAsync(string schema, string functionName, string? catalog, CancellationToken cancellationToken)
    {
        var snapshotFunction = await LoadSnapshotFunctionAsync(schema, functionName, catalog, cancellationToken).ConfigureAwait(false);
        if (snapshotFunction == null)
        {
            return null;
        }

        return MapSnapshotFunctionReturn(schema, functionName, snapshotFunction);
    }

    private async Task<SnapshotFunction?> LoadSnapshotFunctionAsync(string schema, string functionName, string? catalog, CancellationToken cancellationToken)
    {
        var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
        var normalizedName = functionName.Trim();
        var normalizedCatalog = string.IsNullOrWhiteSpace(catalog) ? string.Empty : catalog.Trim();
        var cacheKey = string.IsNullOrWhiteSpace(normalizedCatalog)
            ? string.Concat(normalizedSchema, ".", normalizedName)
            : string.Concat(normalizedCatalog, ".", normalizedSchema, ".", normalizedName);

        if (_snapshotFunctionCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        SnapshotFunction? snapshot = null;
        var path = BuildFunctionSnapshotPath(normalizedSchema, normalizedName, normalizedCatalog);
        if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
        {
            try
            {
                await using var stream = File.OpenRead(path);
                snapshot = await JsonSerializer.DeserializeAsync<SnapshotFunction>(stream, _jsonOptions, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _console.Verbose($"[enhanced-schema] Snapshot function load failed for {normalizedSchema}.{normalizedName}: {ex.Message}");
            }
        }

        _snapshotFunctionCache[cacheKey] = snapshot;
        return snapshot;
    }

    private static FunctionReturnMetadata? MapSnapshotFunctionReturn(string schema, string functionName, SnapshotFunction snapshotFunction)
    {
        var isTableValued = snapshotFunction.IsTableValued == true;
        if (isTableValued)
        {
            return new FunctionReturnMetadata
            {
                Schema = schema,
                Name = functionName,
                IsTableValued = true
            };
        }

        var rawType = snapshotFunction.ReturnSqlType ?? string.Empty;
        var (sqlType, userTypeSchema, userTypeName) = NormalizeSnapshotFunctionType(rawType);

        return new FunctionReturnMetadata
        {
            Schema = schema,
            Name = functionName,
            IsTableValued = false,
            SqlTypeName = sqlType,
            MaxLength = snapshotFunction.ReturnMaxLength,
            Precision = null,
            Scale = null,
            IsNullable = snapshotFunction.ReturnIsNullable,
            UserTypeSchema = userTypeSchema,
            UserTypeName = userTypeName
        };
    }

    private static (string? SqlType, string? UserTypeSchema, string? UserTypeName) NormalizeSnapshotFunctionType(string rawType)
    {
        var trimmed = rawType.Trim();
        if (trimmed.IndexOf('(') >= 0 && trimmed.IndexOf(')') > 0)
        {
            return (trimmed.ToLowerInvariant(), null, null);
        }

        if (trimmed.Contains('.'))
        {
            var parts = trimmed.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                return (null, parts[0], parts[1]);
            }
        }

        return (trimmed.ToLowerInvariant(), null, null);
    }

    private async Task<FunctionReturnMetadata?> ResolveFunctionFromDatabaseAsync(string schema, string functionName, CancellationToken cancellationToken)
    {
        if (_dbContext == null)
        {
            return null;
        }

        const string sql = """
SELECT ROUTINE_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = @schema AND ROUTINE_NAME = @name
""";

        try
        {
            var parameters = new List<Microsoft.Data.SqlClient.SqlParameter>
            {
                new("@schema", schema),
                new("@name", functionName)
            };

            var result = await _dbContext.SingleAsync<RoutineInfoDto>(
                sql,
                parameters,
                cancellationToken,
                telemetryOperation: "EnhancedSchemaMetadataProvider.ResolveFunction",
                telemetryCategory: "Analyzer.SchemaFallback").ConfigureAwait(false);

            if (result == null)
            {
                return null;
            }

            var isTableValued = string.Equals(result.DATA_TYPE, "TABLE", StringComparison.OrdinalIgnoreCase);
            var sqlType = isTableValued ? null : NormalizeRoutineSqlType(result.DATA_TYPE, result.CHARACTER_MAXIMUM_LENGTH, result.NUMERIC_PRECISION, result.NUMERIC_SCALE);
            var precision = isTableValued ? (int?)null : (result.NUMERIC_PRECISION.HasValue ? result.NUMERIC_PRECISION.Value : null);

            return new FunctionReturnMetadata
            {
                Schema = schema,
                Name = functionName,
                IsTableValued = isTableValued,
                SqlTypeName = sqlType,
                MaxLength = isTableValued ? null : result.CHARACTER_MAXIMUM_LENGTH,
                Precision = precision,
                Scale = isTableValued ? null : result.NUMERIC_SCALE,
                IsNullable = isTableValued ? null : true
            };
        }
        catch (Exception ex)
        {
            _console.Verbose($"[enhanced-schema] Database function lookup failed for {schema}.{functionName}: {ex.Message}");
            return null;
        }
    }

    private static string? NormalizeRoutineSqlType(string? dataType, int? maxLength, byte? precision, int? scale)
    {
        if (string.IsNullOrWhiteSpace(dataType))
        {
            return null;
        }

        var normalized = dataType.Trim().ToLowerInvariant();
        return normalized switch
        {
            "nvarchar" or "varchar" or "nchar" or "char" or "varbinary" or "binary" => FormatLengthType(normalized, maxLength),
            "decimal" or "numeric" => FormatPrecisionType(normalized, precision, scale),
            "datetime2" or "datetimeoffset" or "time" => FormatScaleType(normalized, scale),
            _ => normalized
        };
    }

    private static string FormatLengthType(string baseType, int? length)
    {
        if (!length.HasValue)
        {
            return string.Concat(baseType, "(max)");
        }

        if (length.Value < 0)
        {
            return string.Concat(baseType, "(max)");
        }

        return string.Concat(baseType, "(", length.Value, ")");
    }

    private static string FormatPrecisionType(string baseType, byte? precision, int? scale)
    {
        if (precision.HasValue)
        {
            return string.Concat(baseType, "(", precision.Value, ",", scale ?? 0, ")");
        }

        return baseType;
    }

    private static string FormatScaleType(string baseType, int? scale)
    {
        if (scale.HasValue)
        {
            return string.Concat(baseType, "(", scale.Value, ")");
        }

        return baseType;
    }
}

/// <summary>
/// DTO for mapping INFORMATION_SCHEMA.COLUMNS results.
/// Note: SQL Server returns some fields as tinyint (byte) or smallint, not int.
/// </summary>
internal sealed class ColumnInfoDto
{
    public string COLUMN_NAME { get; set; } = string.Empty;
    public string DATA_TYPE { get; set; } = string.Empty;
    public string IS_NULLABLE { get; set; } = string.Empty;
    public int? CHARACTER_MAXIMUM_LENGTH { get; set; }
    public byte? NUMERIC_PRECISION { get; set; }
    public int? NUMERIC_SCALE { get; set; }
    public string? DOMAIN_SCHEMA { get; set; }
    public string? DOMAIN_NAME { get; set; }
}

internal sealed class RoutineInfoDto
{
    public string? ROUTINE_TYPE { get; set; }
    public string? DATA_TYPE { get; set; }
    public int? CHARACTER_MAXIMUM_LENGTH { get; set; }
    public byte? NUMERIC_PRECISION { get; set; }
    public int? NUMERIC_SCALE { get; set; }
}

internal sealed class SystemColumnInfoDto
{
    public string? ColumnName { get; set; }
    public string? SystemTypeName { get; set; }
    public bool? IsNullable { get; set; }
    public int? ColumnOrdinal { get; set; }
    public short? MaxLength { get; set; }
    public byte? Precision { get; set; }
    public byte? Scale { get; set; }
    public string? UserTypeSchema { get; set; }
    public string? UserTypeName { get; set; }
}
