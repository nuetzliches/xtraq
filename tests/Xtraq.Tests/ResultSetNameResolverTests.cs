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

    /// <summary>
    /// Invalid SQL should fall back to the generic naming strategy.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithInvalidSql_ReturnsNull()
    {
        const string sql = """
        CREATE PROCEDURE sample.Oops
        AS
        BEGIN
            SELECT Id FROM -- missing table name forces parse failure
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Null(suggested);
    }

    /// <summary>
    /// CTE-backed queries should resolve to the underlying base table.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithCteReference_UsesBaseTable()
    {
        const string sql = """
        CREATE OR ALTER PROCEDURE sample.LatestUser
        AS
        BEGIN
            WITH Latest AS (
                SELECT TOP (1) u.Id, u.DisplayName
                FROM dbo.Users AS u
                ORDER BY u.CreatedAtUtc DESC
            )
            SELECT l.Id, l.DisplayName FROM Latest AS l;
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Equal("Users", suggested);
    }

    /// <summary>
    /// FOR JSON PATH statements with a literal ROOT alias should reuse the declared name.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithForJsonRootAlias_UsesRootName()
    {
        const string sql = """
        CREATE OR ALTER PROCEDURE sample.JsonUsers
        AS
        BEGIN
            SELECT u.Id, u.DisplayName
            FROM dbo.Users AS u
            FOR JSON PATH, ROOT('UsersPayload');
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Equal("UsersPayload", suggested);
    }

    /// <summary>
    /// Dynamic ROOT expressions should fall back to the generic naming scheme.
    /// </summary>
    [Xunit.Fact]
    public static void TryResolve_WithForJsonDynamicRoot_ReturnsNull()
    {
        const string sql = """
        CREATE OR ALTER PROCEDURE sample.JsonUsersDynamic
        AS
        BEGIN
            DECLARE @root nvarchar(50) = N'UsersPayload';
            SELECT u.Id, u.DisplayName
            FROM dbo.Users AS u
            FOR JSON PATH, ROOT(@root);
        END
        """;

        var suggested = ResultSetNameResolver.TryResolve(index: 0, procedureSql: sql);

        Xunit.Assert.Null(suggested);
    }
}
