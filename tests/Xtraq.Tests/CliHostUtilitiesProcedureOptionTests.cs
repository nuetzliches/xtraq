using System;
using Xtraq.Cli.Hosting;
using Xunit;

namespace Xtraq.Tests;

public sealed class CliHostUtilitiesProcedureOptionTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TryNormalizeProcedureFilter_WithEmptyValue_ReturnsTrue(string? input)
    {
        var success = CliHostUtilities.TryNormalizeProcedureFilter(input, out var normalized, out var error);

        Assert.True(success);
        Assert.Null(normalized);
        Assert.Null(error);
    }

    [Fact]
    public void TryNormalizeProcedureFilter_DeduplicatesAndTrims()
    {
        var success = CliHostUtilities.TryNormalizeProcedureFilter("dbo.GetOrders, dbo.GetOrders , sales.Process_*", out var normalized, out var error);

        Assert.True(success);
        Assert.Equal("dbo.GetOrders,sales.Process_*", normalized);
        Assert.Null(error);
    }

    [Fact]
    public void TryNormalizeProcedureFilter_AllowsWildcardsAndHyphen()
    {
        var success = CliHostUtilities.TryNormalizeProcedureFilter("workflow-state.Get?Report", out var normalized, out var error);

        Assert.True(success);
        Assert.Equal("workflow-state.Get?Report", normalized);
        Assert.Null(error);
    }

    [Theory]
    [InlineData("dbo")]
    [InlineData("dbo..proc")]
    [InlineData("dbo.[proc]")]
    public void TryNormalizeProcedureFilter_InvalidTokensFail(string input)
    {
        var success = CliHostUtilities.TryNormalizeProcedureFilter(input, out var normalized, out var error);

        Assert.False(success);
        Assert.Null(normalized);
        Assert.False(string.IsNullOrWhiteSpace(error));
        Assert.Contains("Invalid procedure filter", error, StringComparison.OrdinalIgnoreCase);
    }
}
