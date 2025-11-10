
namespace Xtraq.IntegrationTests;

/// <summary>
/// Validates that the curated sample stored procedures parse cleanly and emit the expected metadata.
/// </summary>
public sealed class SampleStoredProcedureParsingTests : Xtraq.TestFramework.SqlServerTestBase
{
    public static IEnumerable<object[]> SampleProcedureFiles()
    {
        var proceduresDirectory = Path.Combine(GetRepositoryRoot(), "samples", "sql", "procedures");
        foreach (var file in Directory.EnumerateFiles(proceduresDirectory, "*.sql", SearchOption.TopDirectoryOnly))
        {
            yield return new object[] { file };
        }
    }

    [Xunit.Theory]
    [Xunit.MemberData(nameof(SampleProcedureFiles))]
    public void Procedure_ShouldParseWithoutErrors(string filePath)
    {
        var sql = File.ReadAllText(filePath);
        var parsed = ParseProcedure(sql, "sample");

        Xunit.Assert.True(parsed.ParseErrorCount == 0, $"Expected no parse errors but found {parsed.ParseErrorCount} in {Path.GetFileName(filePath)}");
        Xunit.Assert.NotNull(parsed.ResultSets);
    }

    [Xunit.Fact]
    public void UserDetailsWithOrders_ShouldBeRecognizedAsSelect()
    {
        var parsed = ParseSampleProcedure("UserDetailsWithOrders.sql");
        Xunit.Assert.True(parsed.ContainsSelect);
        Xunit.Assert.Empty(parsed.ResultSets);
    }

    [Xunit.Fact]
    public void OrderListByUserAsJson_ShouldBeMarkedAsJson()
    {
        var parsed = ParseSampleProcedure("OrderListByUserAsJson.sql");
        var result = Xunit.Assert.Single(parsed.ResultSets);
        Xunit.Assert.True(result.ReturnsJson);
        Xunit.Assert.False(result.ReturnsJsonArray);
    }

    [Xunit.Fact]
    public void UserOrderHierarchyJson_ShouldExposeNestedJsonColumn()
    {
        var parsed = ParseSampleProcedure("UserOrderHierarchyJson.sql");
        var result = Xunit.Assert.Single(parsed.ResultSets);
        var ordersColumn = Xunit.Assert.Single(result.Columns.Where(c => string.Equals(c.Name, "Orders", StringComparison.OrdinalIgnoreCase)));
        Xunit.Assert.True(ordersColumn.ReturnsJson);
        Xunit.Assert.True(ordersColumn.ReturnsJsonArray);
    }

    private static Xtraq.TestFramework.SqlServerTestBase.ParsedProcedure ParseSampleProcedure(string fileName)
    {
        var filePath = Path.Combine(GetRepositoryRoot(), "samples", "sql", "procedures", fileName);
        var sql = File.ReadAllText(filePath);
        return ParseProcedure(sql, "sample");
    }

    private static string GetRepositoryRoot()
    {
        // Resolve ..\..\..\.. from the integration test bin directory back to the repository root.
        return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
    }
}
