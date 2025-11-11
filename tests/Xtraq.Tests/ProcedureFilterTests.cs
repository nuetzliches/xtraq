using System.Collections.Generic;
using System.Linq;

using Xtraq.Schema;
using Xunit;

namespace Xtraq.Tests;

public static class ProcedureFilterTests
{
    [Fact]
    public static void Create_WithNullExpression_ReturnsInactiveFilter()
    {
        var console = new TestConsoleService();

        var filter = ProcedureFilter.Create(null, console);

        Assert.False(filter.HasFilter);
        Assert.False(filter.Matches("dbo.Test"));
    }

    [Fact]
    public static void Matches_SupportsExactAndWildcardTokens()
    {
        var console = new TestConsoleService();

        var filter = ProcedureFilter.Create("dbo.GetCustomer,core.*", console);

        Assert.True(filter.HasFilter);
        Assert.True(filter.Matches("dbo.GetCustomer"));
        Assert.True(filter.Matches("core.GetInvoices"));
        Assert.False(filter.Matches("sales.GetCustomer"));
    }

    [Fact]
    public static void Apply_FiltersSequenceUsingSelector()
    {
        var console = new TestConsoleService();
        var filter = ProcedureFilter.Create("dbo.GetBar;core.Ping*", console);
        var procedures = new List<string> { "dbo.GetBar", "dbo.GetFoo", "core.PingStats", "sales.GetBar" };

        var filtered = filter.Apply(procedures, static name => name).ToList();

        Assert.Equal(new[] { "dbo.GetBar", "core.PingStats" }, filtered);
    }
}
