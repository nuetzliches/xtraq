using Microsoft.Data.SqlClient;
using Xtraq.Data.Models;

namespace Xtraq.Data.Queries;

internal static class TableQueries
{
    public static Task<List<Table>> TableListAsync(this DbContext context, string schemaName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(schemaName))
        {
            return Task.FromResult(new List<Table>());
        }

        var parameters = new List<SqlParameter> { new("@schemaName", schemaName) };

        const string queryString = @"SELECT tbl.object_id,
               tbl.name AS table_name,
               s.name AS schema_name,
               tbl.modify_date
        FROM sys.tables AS tbl
        INNER JOIN sys.schemas AS s ON s.schema_id = tbl.schema_id
        WHERE s.name = @schemaName
        ORDER BY tbl.name;";

        return context.ListAsync<Table>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableList",
            telemetryCategory: "Collector.Tables");
    }

    public static Task<List<Column>> TableColumnsListAsync(this DbContext context, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        return TableColumnsForTableInternalAsync(context, null, schemaName, tableName, cancellationToken);
    }

    public static async Task<Table?> TableAsync(this DbContext context, string catalogName, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(catalogName) || string.IsNullOrWhiteSpace(schemaName) || string.IsNullOrWhiteSpace(tableName))
        {
            return null;
        }

        var catalogPrefix = QuoteIdentifier(catalogName);
        var queryString = $@"SELECT @catalogName AS catalog_name,
         tbl.object_id,
               tbl.name AS table_name,
               s.name AS schema_name,
               tbl.modify_date
        FROM {catalogPrefix}.sys.tables AS tbl
        INNER JOIN {catalogPrefix}.sys.schemas AS s ON s.schema_id = tbl.schema_id
        WHERE s.name = @schemaName AND tbl.name = @tableName";

        var parameters = new List<SqlParameter>
        {
            new("@catalogName", catalogName),
            new("@schemaName", schemaName),
            new("@tableName", tableName)
        };

        return await context.SingleAsync<Table>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableByCatalog",
            telemetryCategory: "Collector.Tables").ConfigureAwait(false);
    }

    public static Task<List<Column>> TableColumnsListAsync(this DbContext context, string catalogName, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        return TableColumnsForTableInternalAsync(context, catalogName, schemaName, tableName, cancellationToken);
    }

    public static Task<List<Column>> TableColumnsCatalogAsync(this DbContext context, IReadOnlyCollection<string>? schemaFilter, CancellationToken cancellationToken)
    {
        return TableColumnsCatalogAsync(context, null, schemaFilter, cancellationToken);
    }

    public static Task<List<Column>> TableColumnsCatalogAsync(this DbContext context, string? catalogName, IReadOnlyCollection<string>? schemaFilter, CancellationToken cancellationToken)
    {
        var normalizedSchemas = NormalizeSchemaFilter(schemaFilter);
        var parameters = new List<SqlParameter>
        {
            new("@catalogName", NormalizeCatalogParameter(catalogName))
        };

        string whereClause = string.Empty;
        if (normalizedSchemas.Count > 0)
        {
            var placeholders = new List<string>(normalizedSchemas.Count);
            for (var i = 0; i < normalizedSchemas.Count; i++)
            {
                var parameterName = $"@schemaFilter{i}";
                placeholders.Add(parameterName);
                parameters.Add(new SqlParameter(parameterName, normalizedSchemas[i]));
            }

            whereClause = $"WHERE s.name IN ({string.Join(", ", placeholders)})";
        }

        var queryString = BuildColumnSelectQuery(BuildSysCatalogPrefix(catalogName), whereClause);

        return context.ListAsync<Column>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableColumnsCatalog",
            telemetryCategory: "Collector.Tables");
    }

    private static Task<List<Column>> TableColumnsForTableInternalAsync(
        DbContext context,
        string? catalogName,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(schemaName) || string.IsNullOrWhiteSpace(tableName))
        {
            return Task.FromResult(new List<Column>());
        }

        var parameters = new List<SqlParameter>
        {
            new("@catalogName", NormalizeCatalogParameter(catalogName)),
            new("@schemaName", schemaName),
            new("@tableName", tableName)
        };

        const string whereClause = "WHERE s.name = @schemaName AND tbl.name = @tableName";

        return context.ListAsync<Column>(
            BuildColumnSelectQuery(BuildSysCatalogPrefix(catalogName), whereClause),
            parameters,
            cancellationToken,
            telemetryOperation: string.IsNullOrWhiteSpace(catalogName)
                ? "TableQueries.TableColumnsSingle"
                : "TableQueries.TableColumnsByCatalog",
            telemetryCategory: "Collector.Tables");
    }

    private static string BuildColumnSelectQuery(string sysCatalogPrefix, string whereClause)
    {
        var whereSegment = string.IsNullOrWhiteSpace(whereClause) ? string.Empty : $"{Environment.NewLine}{whereClause}";

        return $@"SELECT 
    CASE WHEN @catalogName IS NULL OR LTRIM(RTRIM(@catalogName)) = '' THEN CAST(NULL AS sysname) ELSE @catalogName END AS catalog_name,
    s.name AS schema_name,
    tbl.name AS table_name,
    c.name,
    c.is_nullable,
    t.name AS system_type_name,
    IIF(t.name LIKE 'nvarchar%', c.max_length / 2, c.max_length) AS max_length,
    COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS is_identity,
    t1.name AS user_type_name,
    s1.name AS user_type_schema_name,
    t.name AS base_type_name,
    CAST(c.precision AS int) AS precision,
    CAST(c.scale AS int) AS scale,
    IIF(dc.object_id IS NULL, CAST(0 AS bit), CAST(1 AS bit)) AS has_default_value,
    dc.definition AS default_definition,
    dc.name AS default_constraint_name,
    c.is_computed,
    cc.definition AS computed_definition,
    cc.is_persisted AS is_computed_persisted,
    c.is_rowguidcol,
    c.is_sparse,
    c.generated_always_type_desc,
    c.is_hidden,
    CASE WHEN COLUMNPROPERTY(c.object_id, c.name, 'IsColumnSet') = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_columnset
FROM {sysCatalogPrefix}.tables AS tbl
INNER JOIN {sysCatalogPrefix}.schemas AS s ON s.schema_id = tbl.schema_id
INNER JOIN {sysCatalogPrefix}.columns AS c ON c.object_id = tbl.object_id
INNER JOIN {sysCatalogPrefix}.types AS t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.system_type_id
LEFT JOIN {sysCatalogPrefix}.types AS t1 ON t1.system_type_id = c.system_type_id AND t1.user_type_id = c.user_type_id AND t1.is_user_defined = 1 AND t1.is_table_type = 0
LEFT JOIN {sysCatalogPrefix}.schemas AS s1 ON s1.schema_id = t1.schema_id
LEFT JOIN {sysCatalogPrefix}.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
LEFT JOIN {sysCatalogPrefix}.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
{whereSegment}
ORDER BY s.name, tbl.name, c.column_id;";
    }

    private static string BuildSysCatalogPrefix(string? catalogName)
    {
        return string.IsNullOrWhiteSpace(catalogName)
            ? "sys"
            : string.Concat(QuoteIdentifier(catalogName), ".sys");
    }

    private static List<string> NormalizeSchemaFilter(IReadOnlyCollection<string>? schemaFilter)
    {
        if (schemaFilter == null || schemaFilter.Count == 0)
        {
            return new List<string>();
        }

        var result = new List<string>(schemaFilter.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var schema in schemaFilter)
        {
            if (string.IsNullOrWhiteSpace(schema))
            {
                continue;
            }

            var normalized = schema.Trim();
            if (seen.Add(normalized))
            {
                result.Add(normalized);
            }
        }

        return result;
    }

    private static object NormalizeCatalogParameter(string? catalogName)
    {
        return string.IsNullOrWhiteSpace(catalogName) ? DBNull.Value : catalogName;
    }

    private static string QuoteIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        return "[" + value.Replace("]", "]]", StringComparison.Ordinal) + "]";
    }
}
