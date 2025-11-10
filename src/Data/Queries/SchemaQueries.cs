using Microsoft.Data.SqlClient;

using DbSchema = Xtraq.Data.Models.Schema;

namespace Xtraq.Data.Queries;

internal static class SchemaQueries
{
    public static Task<List<DbSchema>> SchemaListAsync(this DbContext context, CancellationToken cancellationToken)
    {
        const string queryString = "SELECT name FROM sys.schemas WHERE principal_id = 1 ORDER BY name;";
        return context.ListAsync<DbSchema>(
            queryString,
            new List<SqlParameter>(),
            cancellationToken,
            telemetryOperation: "SchemaQueries.SchemaList",
            telemetryCategory: "Collector.Schema");
    }
}
