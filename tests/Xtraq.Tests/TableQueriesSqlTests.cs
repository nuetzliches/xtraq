using System.Reflection;
using Xunit;

namespace Xtraq.Tests;

public class TableQueriesSqlTests
{
    [Fact]
    public void BuildColumnSelectQuery_SuppressesUserTypeForComputedColumns()
    {
        // Computed columns inherit the UDT of the first operand; we explicitly null out user_type fields
        // to avoid leaking operand UDTs into snapshots (e.g. ISNULL(nullable_udt, not_null_udt)).
        var type = typeof(Xtraq.Data.Queries.TableQueries);
        var method = type.GetMethod("BuildColumnSelectQuery", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var sql = (string)method!.Invoke(null, new object[] { "sys", "WHERE 1=1" })!;

        Assert.Contains("CASE WHEN c.is_computed = 1 THEN NULL ELSE t1.name END AS user_type_name", sql);
        Assert.Contains("CASE WHEN c.is_computed = 1 THEN NULL ELSE s1.name END AS user_type_schema_name", sql);
    }
}
