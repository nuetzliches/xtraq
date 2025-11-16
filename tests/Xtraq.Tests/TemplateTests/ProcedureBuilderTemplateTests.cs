using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Xtraq.Tests.TemplateTests;

public sealed class ProcedureBuilderTemplateTests
{
    [Fact]
    public async Task ProcedureBuildersTemplate_AllowsComposedExecution()
    {
        var root = GetSolutionRoot();
        var templatePath = Path.Combine(root, "src", "Templates", "ProcedureBuilders.spt");
        Assert.True(File.Exists(templatePath), $"Template not found: {templatePath}");

        var template = File.ReadAllText(templatePath);
        var builderSource = template
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");

        var harnessSource = @"// harness
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TestNamespace;

public interface IXtraqDbContext { }

public sealed class FakeContext : IXtraqDbContext { }

public static class BuilderHarness
{
    public static async Task<int> RunCallBuilderAsync()
    {
        var ctx = new FakeContext();
        var builder = ProcedureBuilderExtensions.BuildProcedure(
            ctx,
            3,
            static async (db, value, ct) =>
            {
                await Task.Delay(1, ct).ConfigureAwait(false);
                return value + 1;
            });

        var observed = 0;
        var asyncObserved = 0;

        var result = await builder
            .Select(value => value * 2)
            .Tap(value => observed = value)
            .TapAsync(async (value, ct) =>
            {
                asyncObserved = value + 10;
                await Task.Delay(1, ct).ConfigureAwait(false);
            })
            .ExecuteAsync()
            .ConfigureAwait(false);

        return result + observed + asyncObserved;
    }

    public static async Task<(int RowCount, int Sum, int Output, int ObservedOutput)> RunStreamBuilderAsync()
    {
        var ctx = new FakeContext();
        var builder = ProcedureBuilderExtensions.BuildProcedureStream(
            ctx,
            0,
            static async (db, value, onRow, ct) =>
            {
                await onRow(1, ct).ConfigureAwait(false);
                await onRow(2, ct).ConfigureAwait(false);
                await onRow(3, ct).ConfigureAwait(false);
                await Task.Delay(1, ct).ConfigureAwait(false);
                return 42;
            });

        var sum = 0;
        var count = 0;
        var observedOutput = 0;

        var outcome = await builder
            .ForEach((row, ct) =>
            {
                sum += row;
                count++;
                return ValueTask.CompletedTask;
            })
            .TapCompletion(output => observedOutput = output)
            .CompleteWith((output, _) => new ValueTask<(int RowCount, int Sum, int Output)>((count, sum, output)))
            .ExecuteAsync()
            .ConfigureAwait(false);

        return (outcome.RowCount, outcome.Sum, outcome.Output, observedOutput);
    }

    public static async Task<(int BufferCount, int AggregatedCount, int Output)> RunBufferAndAggregateAsync()
    {
        var ctx = new FakeContext();
        var builder = ProcedureBuilderExtensions.BuildProcedureStream(
            ctx,
            0,
            static async (db, value, onRow, ct) =>
            {
                await onRow(5, ct).ConfigureAwait(false);
                await onRow(6, ct).ConfigureAwait(false);
                await onRow(7, ct).ConfigureAwait(false);
                return 11;
            });

        var buffered = await builder.BufferAsync().ConfigureAwait(false);
        var aggregated = await builder.AggregateAsync((rows, output) => (rows.Count, output)).ConfigureAwait(false);
        return (buffered.Count, aggregated.Item1, aggregated.Item2);
    }
}
";

        var syntaxTrees = new[]
        {
            CSharpSyntaxTree.ParseText(builderSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
            CSharpSyntaxTree.ParseText(harnessSource, new CSharpParseOptions(LanguageVersion.CSharp12))
        };

        var references = GetMetadataReferences();
        var compilation = CSharpCompilation.Create(
            assemblyName: "ProcedureBuilderTests.Dynamic",
            syntaxTrees: syntaxTrees,
            references: references,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        using var peStream = new MemoryStream();
        var emitResult = compilation.Emit(peStream);
        if (!emitResult.Success)
        {
            var diagnostics = string.Join(Environment.NewLine, emitResult.Diagnostics);
            throw new InvalidOperationException($"Compilation failed:{Environment.NewLine}{diagnostics}");
        }

        peStream.Position = 0;
        var assembly = AssemblyLoadContext.Default.LoadFromStream(peStream);
        var harnessType = assembly.GetType("TestNamespace.BuilderHarness")!;

        static async Task<T> InvokeAsync<T>(Type harnessType, string method)
        {
            var mi = harnessType.GetMethod(method, BindingFlags.Public | BindingFlags.Static)!;
            var task = (Task<T>)mi.Invoke(null, Array.Empty<object>())!;
            return await task.ConfigureAwait(false);
        }

        var callResult = await InvokeAsync<int>(harnessType, "RunCallBuilderAsync").ConfigureAwait(false);
        Assert.Equal(34, callResult);

        var streamResult = await InvokeAsync<(int RowCount, int Sum, int Output, int ObservedOutput)>(harnessType, "RunStreamBuilderAsync").ConfigureAwait(false);
        Assert.Equal((3, 6, 42, 42), streamResult);

        var aggregateResult = await InvokeAsync<(int BufferCount, int AggregatedCount, int Output)>(harnessType, "RunBufferAndAggregateAsync").ConfigureAwait(false);
        Assert.Equal((3, 3, 11), aggregateResult);
    }

    private static IEnumerable<MetadataReference> GetMetadataReferences()
    {
        var assemblies = new[]
        {
            typeof(object).Assembly,
            typeof(ValueTask).Assembly,
            typeof(Enumerable).Assembly,
            typeof(List<>).Assembly,
            typeof(CancellationToken).Assembly
        };

        return assemblies
            .Select(asm => asm.Location)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(path => MetadataReference.CreateFromFile(path));
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
