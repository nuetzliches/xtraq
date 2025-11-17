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
        var globalUsingsPath = Path.Combine(root, "src", "GlobalUsings.cs");
        Assert.True(File.Exists(globalUsingsPath), $"Global usings not found: {globalUsingsPath}");
        var globalUsingsSource = File.ReadAllText(globalUsingsPath);
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

public interface IXtraqTransactionOrchestrator { }

public interface IXtraqTransactionOrchestratorAccessor
{
    IXtraqTransactionOrchestrator TransactionOrchestrator { get; }
}

public sealed class FakeOrchestrator : IXtraqTransactionOrchestrator { }

public sealed class FakeContext : IXtraqDbContext, IXtraqTransactionOrchestratorAccessor
{
    public IXtraqTransactionOrchestrator TransactionOrchestrator { get; } = new FakeOrchestrator();
}

public sealed class CapturePolicy : IProcedureExecutionPolicy
{
    public int Invocations { get; private set; }
    public string? Label { get; private set; }

    public ValueTask<TResult> ExecuteAsync<TInput, TResult>(
        ProcedureExecutionContext<TInput> context,
        ProcedureCallDelegate<TInput, TResult> next,
        CancellationToken cancellationToken)
    {
        Invocations++;
        Label = context.Label;
        return next(context, cancellationToken);
    }
}

public static class BuilderHarness
{
    public static async Task<(int Total, string? Label, int PolicyInvocations)> RunCallPipelineAsync()
    {
        var ctx = new FakeContext();
        var policy = new CapturePolicy();

        var execution = ctx.ConfigureProcedure(3)
            .WithLabel(""call-pipeline"")
            .WithPolicy(policy)
            .WithExecutor(static async (db, value, ct) =>
            {
                await Task.Delay(1, ct);
                return value + 1;
            });

        var observed = 0;
        var asyncObserved = 0;

        var result = await execution
            .Select(value => value * 2)
            .Tap(value => observed = value)
            .TapAsync(async (value, ct) =>
            {
                asyncObserved = value + 10;
                await Task.Delay(1, ct);
            })
            .ExecuteAsync();

        return (result + observed + asyncObserved, policy.Label, policy.Invocations);
    }

    public static async Task<(int RowCount, int Sum, int Output, int ObservedOutput, string? Label, int PolicyInvocations)> RunStreamPipelineAsync()
    {
        var ctx = new FakeContext();
        var policy = new CapturePolicy();

        var execution = ctx.ConfigureProcedureStream<int, int>(0)
            .WithLabel(""stream-pipeline"")
            .WithPolicy(policy)
            .WithExecutor(static async (db, value, onRow, ct) =>
        {
            await onRow(1, ct);
            await onRow(2, ct);
            await onRow(3, ct);
            await Task.Delay(1, ct);
            return 42;
        });

        var sum = 0;
        var count = 0;
        var observedOutput = 0;

        var outcome = await execution
            .ForEach((row, ct) =>
            {
                sum += row;
                count++;
                return ValueTask.CompletedTask;
            })
            .TapCompletion(output => observedOutput = output)
            .CompleteWith((output, _) => new ValueTask<(int RowCount, int Sum, int Output)>((count, sum, output)))
            .ExecuteAsync();

        return (outcome.RowCount, outcome.Sum, outcome.Output, observedOutput, policy.Label, policy.Invocations);
    }

    public static async Task<(int BufferCount, int AggregatedCount, int Output)> RunBufferAndAggregateAsync()
    {
        var ctx = new FakeContext();

        var execution = ctx.ConfigureProcedureStream<int, int>(0)
            .WithExecutor(static async (db, value, onRow, ct) =>
            {
                await onRow(5, ct);
                await onRow(6, ct);
                await onRow(7, ct);
                return 11;
            });

        var buffered = await execution.BufferAsync();
        var aggregated = await execution.AggregateAsync((rows, output) => (rows.Count, output));
        return (buffered.Count, aggregated.Item1, aggregated.Item2);
    }
}
";

        var syntaxTrees = new[]
        {
            CSharpSyntaxTree.ParseText(globalUsingsSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
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
            return await task;
        }

        var callResult = await InvokeAsync<(int Total, string? Label, int PolicyInvocations)>(harnessType, "RunCallPipelineAsync");
        Assert.Equal((34, "call-pipeline", 1), callResult);

        var streamResult = await InvokeAsync<(int RowCount, int Sum, int Output, int ObservedOutput, string? Label, int PolicyInvocations)>(harnessType, "RunStreamPipelineAsync");
        Assert.Equal((3, 6, 42, 42, "stream-pipeline", 1), streamResult);

        var aggregateResult = await InvokeAsync<(int BufferCount, int AggregatedCount, int Output)>(harnessType, "RunBufferAndAggregateAsync");
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
            typeof(CancellationToken).Assembly,
            typeof(System.Text.Json.JsonSerializer).Assembly,
            typeof(System.Text.RegularExpressions.Regex).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection).Assembly
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
