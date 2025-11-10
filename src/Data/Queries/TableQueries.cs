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

    public static async Task<Column?> TableColumnAsync(this DbContext context, string schemaName, string tableName, string columnName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(schemaName) || string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(columnName))
        {
            return null;
        }

        var parameters = new List<SqlParameter>
        {
            new("@schemaName", schemaName),
            new("@tableName", tableName),
            new("@columnName", columnName)
        };

        const string queryString = @"SELECT 
                     CAST(NULL AS sysname) AS catalog_name,
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
                 FROM sys.tables AS tbl
                 INNER JOIN sys.schemas AS s ON s.schema_id = tbl.schema_id
                 INNER JOIN sys.columns AS c ON c.object_id = tbl.object_id
                 INNER JOIN sys.types AS t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.system_type_id
                 LEFT JOIN sys.types AS t1 ON t1.system_type_id = c.system_type_id AND t1.user_type_id = c.user_type_id AND t1.is_user_defined = 1 AND t1.is_table_type = 0
                 LEFT JOIN sys.schemas AS s1 ON s1.schema_id = t1.schema_id
                 LEFT JOIN sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                 LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
                 WHERE s.name = @schemaName AND tbl.name = @tableName AND c.name = @columnName;";

        return await context.SingleAsync<Column>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableColumn",
            telemetryCategory: "Collector.Tables").ConfigureAwait(false);
    }

    public static Task<List<Column>> TableColumnsListAsync(this DbContext context, string schemaName, string tableName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(schemaName) || string.IsNullOrWhiteSpace(tableName))
        {
            return Task.FromResult(new List<Column>());
        }

        var parameters = new List<SqlParameter>
        {
            new("@schemaName", schemaName),
            new("@tableName", tableName)
        };

        const string queryString = @"SELECT CAST(NULL AS sysname) AS catalog_name,
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
         FROM sys.tables AS tbl
         INNER JOIN sys.schemas AS s ON s.schema_id = tbl.schema_id
         INNER JOIN sys.columns AS c ON c.object_id = tbl.object_id
         INNER JOIN sys.types AS t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.system_type_id
         LEFT JOIN sys.types AS t1 ON t1.system_type_id = c.system_type_id AND t1.user_type_id = c.user_type_id AND t1.is_user_defined = 1 AND t1.is_table_type = 0
         LEFT JOIN sys.schemas AS s1 ON s1.schema_id = t1.schema_id
         LEFT JOIN sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
         LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
         WHERE s.name = @schemaName AND tbl.name = @tableName
         ORDER BY c.column_id;";

        return context.ListAsync<Column>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableColumns",
            telemetryCategory: "Collector.Tables");
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
        if (string.IsNullOrWhiteSpace(catalogName) || string.IsNullOrWhiteSpace(schemaName) || string.IsNullOrWhiteSpace(tableName))
        {
            return Task.FromResult(new List<Column>());
        }

        var catalogPrefix = QuoteIdentifier(catalogName);
        var queryString = $@"SELECT @catalogName AS catalog_name,
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
         FROM {catalogPrefix}.sys.tables AS tbl
         INNER JOIN {catalogPrefix}.sys.schemas AS s ON s.schema_id = tbl.schema_id
         INNER JOIN {catalogPrefix}.sys.columns AS c ON c.object_id = tbl.object_id
         INNER JOIN {catalogPrefix}.sys.types AS t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.system_type_id
         LEFT JOIN {catalogPrefix}.sys.types AS t1 ON t1.system_type_id = c.system_type_id AND t1.user_type_id = c.user_type_id AND t1.is_user_defined = 1 AND t1.is_table_type = 0
         LEFT JOIN {catalogPrefix}.sys.schemas AS s1 ON s1.schema_id = t1.schema_id
         LEFT JOIN {catalogPrefix}.sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
         LEFT JOIN {catalogPrefix}.sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
         WHERE s.name = @schemaName AND tbl.name = @tableName
         ORDER BY c.column_id;";

        var parameters = new List<SqlParameter>
        {
            new("@catalogName", catalogName),
            new("@schemaName", schemaName),
            new("@tableName", tableName)
        };

        return context.ListAsync<Column>(
            queryString,
            parameters,
            cancellationToken,
            telemetryOperation: "TableQueries.TableColumnsByCatalog",
            telemetryCategory: "Collector.Tables");
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
