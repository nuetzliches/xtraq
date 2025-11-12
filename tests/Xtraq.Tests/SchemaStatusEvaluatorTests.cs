using System.Collections.Generic;
using System.Linq;

using Xtraq.Models;
using Xtraq.Schema;
using Xunit;

namespace Xtraq.Tests;

public static class SchemaStatusEvaluatorTests
{
    [Fact]
    public static void Evaluate_RespectsExplicitSchemaConfiguration()
    {
        var evaluator = new SchemaStatusEvaluator();
        var context = new SchemaSelectionContext
        {
            DefaultSchemaStatus = SchemaStatusEnum.Build,
            ExplicitSchemaStatuses = new[] { new SchemaStatusOverride("dbo", SchemaStatusEnum.Ignore) }
        };
        var schemas = new List<SchemaModel>
        {
            new() { Name = "dbo" },
            new() { Name = "sales" }
        };

        var result = evaluator.Evaluate(context, schemas);

        Assert.Equal(SchemaStatusEnum.Ignore, result.Schemas.First(s => s.Name == "dbo").Status);
        Assert.Equal(SchemaStatusEnum.Build, result.Schemas.First(s => s.Name == "sales").Status);
        Assert.Single(result.ActiveSchemas, s => s.Name == "sales");
        Assert.DoesNotContain(result.ActiveSchemas, s => s.Name == "dbo");
        Assert.True(result.BuildSchemasChanged);
        Assert.Equal(new[] { "sales" }, result.BuildSchemas);
    }

    [Fact]
    public static void Evaluate_HonorsDefaultStatus()
    {
        var evaluator = new SchemaStatusEvaluator();
        var context = new SchemaSelectionContext
        {
            DefaultSchemaStatus = SchemaStatusEnum.Ignore
        };
        var schemas = new List<SchemaModel>
        {
            new() { Name = "dbo" },
            new() { Name = "extra" }
        };

        var result = evaluator.Evaluate(context, schemas);

        Assert.Equal(SchemaStatusEnum.Ignore, result.Schemas.First(s => s.Name == "dbo").Status);
        Assert.Equal(SchemaStatusEnum.Ignore, result.Schemas.First(s => s.Name == "extra").Status);
        Assert.Empty(result.ActiveSchemas);
        Assert.False(result.BuildSchemasChanged);
        Assert.Empty(result.BuildSchemas);
    }

    [Fact]
    public static void Evaluate_UsesBuildSchemasAllowList()
    {
        var evaluator = new SchemaStatusEvaluator();
        var context = new SchemaSelectionContext
        {
            DefaultSchemaStatus = SchemaStatusEnum.Build,
            BuildSchemas = new List<string> { "sales" }
        };
        var schemas = new List<SchemaModel>
        {
            new() { Name = "dbo" },
            new() { Name = "sales" }
        };

        var result = evaluator.Evaluate(context, schemas);

        Assert.Equal(SchemaStatusEnum.Ignore, result.Schemas.First(s => s.Name == "dbo").Status);
        Assert.Equal(SchemaStatusEnum.Build, result.Schemas.First(s => s.Name == "sales").Status);
        Assert.Single(result.ActiveSchemas, s => s.Name == "sales");
        Assert.Equal(new[] { "sales" }, result.BuildSchemas);
        Assert.False(result.BuildSchemasChanged); // already matched configuration allow-list
    }
}
