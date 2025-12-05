using System;
using System.IO;
using Xtraq.Engine;
using Xunit;

namespace Xtraq.Tests.TemplateTests;

public sealed class TableTypeTemplateTests
{
    [Fact]
    public void TableTypeRequest_EmitsInitializersForNonNullableColumns()
    {
        var root = GetSolutionRoot();
        var templatePath = Path.Combine(root, "src", "Templates", "TableType.xqt");
        Assert.True(File.Exists(templatePath), $"Template not found: {templatePath}");

        var engine = new SimpleTemplateEngine();
        var model = new
        {
            HEADER = "// generated for tests",
            Namespace = "Test.Namespace",
            Schema = "Test",
            Name = "ActorRef",
            TypeName = "ActorRef",
            TableTypeName = "ActorRef",
            ColumnsCount = 1,
            GeneratedAt = "<generated>",
            EmitRequestContracts = true,
            Columns = new[]
            {
                new
                {
                    PropertyName = "Actor",
                    ClrType = "string",
                    PropertyInitializer = " = string.Empty;",
                    BuilderInitializer = " = string.Empty",
                    Separator = string.Empty,
                    RequestAttributes = Array.Empty<string>(),
                    RequestSeparator = string.Empty
                }
            }
        };

        var output = engine.Render(File.ReadAllText(templatePath), model);
        var occurrences = output.Split("public string Actor { get; init; } = string.Empty;", StringSplitOptions.None).Length - 1;
        Assert.Equal(2, occurrences);
        Assert.Contains("public sealed record ActorRefRequest", output, StringComparison.Ordinal);
    }

    private static string GetSolutionRoot()
    {
        var directory = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(directory))
        {
            if (File.Exists(Path.Combine(directory, "Xtraq.sln")))
            {
                return directory;
            }

            directory = Directory.GetParent(directory)?.FullName;
        }

        throw new InvalidOperationException("Could not locate solution root.");
    }
}
