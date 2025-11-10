namespace Xtraq.Tests.Engine;

/// <summary>
/// Verifies the modernised simple template engine feature set.
/// </summary>
public sealed class SimpleTemplateEngineTests
{
    /// <summary>
    /// Ensures basic placeholders are replaced with their corresponding values.
    /// </summary>
    [Xunit.Fact]
    public void Render_ReplacesSimplePlaceholder()
    {
        var engine = new Xtraq.Engine.SimpleTemplateEngine();
        const string template = "Hello {{Name}}!";
        var result = engine.Render(template, new { Name = "World" });
        Xunit.Assert.Equal("Hello World!", result);
    }

    /// <summary>
    /// Confirms that nested if, elseif, and else branches evaluate in order.
    /// </summary>
    [Xunit.Fact]
    public void Render_SupportsNestedConditionalBranches()
    {
        var engine = new Xtraq.Engine.SimpleTemplateEngine();
        const string template = "{{#if User}}{{#if User.IsActive}}active{{#elseif User.IsPending}}pending{{else}}inactive{{/if}}{{else}}missing{{/if}}";
        var result = engine.Render(template, new { User = new { IsActive = false, IsPending = true } });
        Xunit.Assert.Equal("pending", result);
    }

    /// <summary>
    /// Validates alias handling inside each loops and fallback to parent scope values.
    /// </summary>
    [Xunit.Fact]
    public void Render_SupportsEachAliasAndParentLookup()
    {
        var engine = new Xtraq.Engine.SimpleTemplateEngine();
        const string template = "{{#each Orders as order}}{{OrderPrefix}}-{{order.Id}}:{{this.Amount}};{{/each}}";
        var model = new
        {
            OrderPrefix = "ORD",
            Orders = new[]
            {
                new { Id = 1, Amount = 125.5m },
                new { Id = 2, Amount = 80m }
            }
        };
        var result = engine.Render(template, model);
        Xunit.Assert.Equal("ORD-1:125.5;ORD-2:80;", result);
    }

    /// <summary>
    /// Ensures nested each blocks render correctly for hierarchical data.
    /// </summary>
    [Xunit.Fact]
    public void Render_SupportsNestedEachBlocks()
    {
        var engine = new Xtraq.Engine.SimpleTemplateEngine();
        const string template = "{{#each Groups as group}}{{group.Name}}:[{{#each group.Items}}{{this}},{{/each}}];{{/each}}";
        var model = new
        {
            Groups = new[]
            {
                new { Name = "A", Items = new[] { "x", "y" } },
                new { Name = "B", Items = new[] { "z" } }
            }
        };
        var result = engine.Render(template, model);
        Xunit.Assert.Equal("A:[x,y,];B:[z,];", result);
    }

    /// <summary>
    /// Guarantees the parser reports malformed templates instead of returning partial output.
    /// </summary>
    [Xunit.Fact]
    public void Render_UnbalancedBlocks_Throws()
    {
        var engine = new Xtraq.Engine.SimpleTemplateEngine();
        const string template = "{{#if Flag}}open";
        Xunit.Assert.Throws<System.InvalidOperationException>(() => engine.Render(template, new { Flag = true }));
    }
}
