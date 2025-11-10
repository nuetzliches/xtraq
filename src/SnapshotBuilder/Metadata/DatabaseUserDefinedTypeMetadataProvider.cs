using Xtraq.Data;
using Xtraq.Data.Queries;
using Xtraq.Services;

namespace Xtraq.SnapshotBuilder.Metadata;

internal sealed class DatabaseUserDefinedTypeMetadataProvider : IUserDefinedTypeMetadataProvider
{
    private readonly DbContext _dbContext;
    private readonly IConsoleService _console;

    public DatabaseUserDefinedTypeMetadataProvider(DbContext dbContext, IConsoleService console)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task<IReadOnlyList<UserDefinedTypeRow>> GetUserDefinedTypesAsync(ISet<string> schemas, CancellationToken cancellationToken)
    {
        try
        {
            var list = await _dbContext.UserDefinedScalarTypesAsync(cancellationToken).ConfigureAwait(false);
            if (list == null || list.Count == 0)
            {
                return Array.Empty<UserDefinedTypeRow>();
            }

            if (schemas == null || schemas.Count == 0)
            {
                return list;
            }

            return list
                .Where(row => row != null && !string.IsNullOrWhiteSpace(row.schema_name) && schemas.Contains(row.schema_name))
                .ToList();
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-udt] failed to enumerate user-defined types: {ex.Message}");
            return Array.Empty<UserDefinedTypeRow>();
        }
    }

    public async Task<UserDefinedTypeRow?> GetUserDefinedTypeAsync(string? catalog, string schema, string name, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        try
        {
            var sql = BuildScalarTypeLookupSql(catalog);
            var parameters = new List<Microsoft.Data.SqlClient.SqlParameter>
            {
                new("@schemaName", schema),
                new("@typeName", name)
            };

            if (!string.IsNullOrWhiteSpace(catalog))
            {
                parameters.Add(new("@catalogName", catalog));
            }

            return await _dbContext.SingleAsync<UserDefinedTypeRow>(
                sql,
                parameters,
                cancellationToken,
                telemetryOperation: "UserDefinedTypeQueries.ScalarTypeSingle",
                telemetryCategory: "Collector.UserTypes").ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var target = string.IsNullOrWhiteSpace(catalog) ? "(default catalog)" : catalog;
            _console.Verbose($"[snapshot-udt] failed to resolve user-defined type {target}.{schema}.{name}: {ex.Message}");
            return null;
        }
    }

    private static string BuildScalarTypeLookupSql(string? catalog)
    {
        var catalogQualifier = string.IsNullOrWhiteSpace(catalog)
            ? string.Empty
            : string.Concat(QuoteIdentifier(catalog), ".");

        var catalogSelect = string.IsNullOrWhiteSpace(catalog)
            ? "CAST(NULL AS sysname)"
            : "@catalogName";

        return $@"SELECT {catalogSelect} AS catalog_name,
            s.name AS schema_name,
            t1.name AS user_type_name,
            t.name AS base_type_name,
            IIF(t.name LIKE 'nvarchar%', t1.max_length / 2, t1.max_length) AS max_length,
            CAST(t1.precision AS int) AS precision,
            CAST(t1.scale AS int) AS scale,
            CAST(t1.is_nullable AS int) AS is_nullable
             FROM {catalogQualifier}sys.types AS t1
             INNER JOIN {catalogQualifier}sys.schemas AS s ON s.schema_id = t1.schema_id
             INNER JOIN {catalogQualifier}sys.types AS t ON t.system_type_id = t1.system_type_id AND t.user_type_id = t1.system_type_id
             WHERE t1.is_user_defined = 1
               AND t1.is_table_type = 0
               AND s.name = @schemaName
               AND t1.name = @typeName;";
    }

    private static string QuoteIdentifier(string value)
        => string.Concat("[", value.Replace("]", "]]", StringComparison.Ordinal), "]");
}
