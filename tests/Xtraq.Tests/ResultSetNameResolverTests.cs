using Xtraq.Metadata;

namespace Xtraq.Tests;

/// <summary>
/// Regression tests for <see cref="ResultSetNameResolver"/>.
/// </summary>
public static class ResultSetNameResolverTests
{
    /// <summary>
    /// Dynamic SQL should not influence result-set naming suggestions.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithDynamicSql_ReturnsNull()
    {
        const string sql = """
        CREATE OR ALTER PROCEDURE sample.DynamicQuery
        AS
        BEGIN
            DECLARE @sql nvarchar(max) = N'SELECT Id FROM dbo.Users';
            EXEC(@sql);
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Null(suggested);
    }

    /// <summary>
    /// When a base table is present and no dynamic SQL is used, the resolver should propose the table name.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithStaticSelect_UsesBaseTable()
    {
        const string sql = """
        CREATE OR ALTER PROCEDURE sample.FetchUsers
        AS
        BEGIN
            SELECT u.Id, u.DisplayName FROM dbo.Users AS u;
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Equal("Users", suggested);
    }
}
