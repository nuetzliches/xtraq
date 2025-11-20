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
        var templatePath = Path.Combine(root, "src", "Templates", "ProcedureBuilders.xqt");
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

public delegate XtraqTransactionOptions? TransactionOptionsSelector<TInput>(ProcedureExecutionContext<TInput> context);

public sealed class XtraqTransactionOptions { }

public sealed class XtraqTransactionScope : IAsyncDisposable
{
    private readonly FakeOrchestrator _owner;

    internal XtraqTransactionScope(FakeOrchestrator owner)
    {
        _owner = owner;
    }

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        _owner.NotifyCommitted();
        return ValueTask.CompletedTask;
    }

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        _owner.NotifyRolledBack();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
        => ValueTask.CompletedTask;
}

public interface IXtraqTransactionOrchestrator
{
    ValueTask<XtraqTransactionScope> BeginAsync(XtraqTransactionOptions? options = null, CancellationToken cancellationToken = default);
}

public interface IXtraqTransactionOrchestratorAccessor
{
    IXtraqTransactionOrchestrator TransactionOrchestrator { get; }
}

public sealed class FakeOrchestrator : IXtraqTransactionOrchestrator
{
    public int BeginCount { get; private set; }
    public int CommitCount { get; private set; }
    public int RollbackCount { get; private set; }

    public ValueTask<XtraqTransactionScope> BeginAsync(XtraqTransactionOptions? options = null, CancellationToken cancellationToken = default)
    {
        BeginCount++;
        return ValueTask.FromResult(new XtraqTransactionScope(this));
    }

    internal void NotifyCommitted() => CommitCount++;
    internal void NotifyRolledBack() => RollbackCount++;
}

public sealed class FakeContext : IXtraqDbContext, IXtraqTransactionOrchestratorAccessor
{
    private readonly FakeOrchestrator _orchestrator = new();

    public FakeOrchestrator Orchestrator => _orchestrator;

    IXtraqTransactionOrchestrator IXtraqTransactionOrchestratorAccessor.TransactionOrchestrator => _orchestrator;
}

public sealed class TransactionScopeExecutionPolicy : IProcedureExecutionPolicy
{
    public static int BeginCount { get; private set; }
    public static int CommitCount { get; private set; }
    public static int RollbackCount { get; private set; }

    public static void Reset()
    {
        BeginCount = 0;
        CommitCount = 0;
        RollbackCount = 0;
    }

    public async ValueTask<TResult> ExecuteAsync<TInput, TResult>(
        ProcedureExecutionContext<TInput> context,
        ProcedureCallDelegate<TInput, TResult> next,
        CancellationToken cancellationToken)
    {
        BeginCount++;
        var scope = await context.TransactionOrchestrator.BeginAsync(null, cancellationToken).ConfigureAwait(false);

        try
        {
            var result = await next(context, cancellationToken).ConfigureAwait(false);
            CommitCount++;
            await scope.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            RollbackCount++;
            await scope.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }
}

public static class TransactionScopeExecutionPolicyFactory
{
    public static TransactionScopeExecutionPolicy Default => new();

    public static TransactionScopeExecutionPolicy FromOptions(XtraqTransactionOptions options)
        => new();

    public static TransactionScopeExecutionPolicy FromSelector(Func<IProcedureExecutionContext, XtraqTransactionOptions?> selector)
        => new();

    public static TransactionScopeExecutionPolicy FromSelector<TInput>(TransactionOptionsSelector<TInput> selector)
        => new();
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
    public static async Task<(int Total, string? Label, int PolicyInvocations, int PolicyBegins, int PolicyCommits, int PolicyRollbacks, int OrchestratorBegins, int OrchestratorCommits, int OrchestratorRollbacks)> RunCallPipelineAsync()
    {
        TransactionScopeExecutionPolicy.Reset();
        var ctx = new FakeContext();
        var policy = new CapturePolicy();

        var execution = ctx.ConfigureProcedure(3)
            .WithLabel(""call-pipeline"")
            .WithTransaction()
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

        var orchestrator = ctx.Orchestrator;
        return (
            result + observed + asyncObserved,
            policy.Label,
            policy.Invocations,
            TransactionScopeExecutionPolicy.BeginCount,
            TransactionScopeExecutionPolicy.CommitCount,
            TransactionScopeExecutionPolicy.RollbackCount,
            orchestrator.BeginCount,
            orchestrator.CommitCount,
            orchestrator.RollbackCount);
    }

    public static async Task<(int RowCount, int Sum, int Output, int ObservedOutput, string? Label, int PolicyInvocations, int PolicyBegins, int PolicyCommits, int PolicyRollbacks, int OrchestratorBegins, int OrchestratorCommits, int OrchestratorRollbacks)> RunStreamPipelineAsync()
    {
        TransactionScopeExecutionPolicy.Reset();
        var ctx = new FakeContext();
        var policy = new CapturePolicy();

        var execution = ctx.ConfigureProcedureStream<int, int>(0)
            .WithLabel(""stream-pipeline"")
            .WithTransaction()
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

        var orchestrator = ctx.Orchestrator;
        return (
            outcome.RowCount,
            outcome.Sum,
            outcome.Output,
            observedOutput,
            policy.Label,
            policy.Invocations,
            TransactionScopeExecutionPolicy.BeginCount,
            TransactionScopeExecutionPolicy.CommitCount,
            TransactionScopeExecutionPolicy.RollbackCount,
            orchestrator.BeginCount,
            orchestrator.CommitCount,
            orchestrator.RollbackCount);
    }

    public static async Task<(int BufferCount, int AggregatedCount, int Output)> RunBufferAndAggregateAsync()
    {
        var ctx = new FakeContext();

        var execution = ctx.ConfigureProcedureStream<int, int>(0)
            .WithTransaction()
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

        var callResult = await InvokeAsync<(int Total, string? Label, int PolicyInvocations, int PolicyBegins, int PolicyCommits, int PolicyRollbacks, int OrchestratorBegins, int OrchestratorCommits, int OrchestratorRollbacks)>(harnessType, "RunCallPipelineAsync");
        Assert.Equal((34, "call-pipeline", 1, 1, 1, 0, 1, 1, 0), callResult);

        var streamResult = await InvokeAsync<(int RowCount, int Sum, int Output, int ObservedOutput, string? Label, int PolicyInvocations, int PolicyBegins, int PolicyCommits, int PolicyRollbacks, int OrchestratorBegins, int OrchestratorCommits, int OrchestratorRollbacks)>(harnessType, "RunStreamPipelineAsync");
        Assert.Equal((3, 6, 42, 42, "stream-pipeline", 1, 1, 1, 0, 1, 1, 0), streamResult);

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

        var loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(static asm => !asm.IsDynamic && !string.IsNullOrWhiteSpace(asm.Location))
            .Select(static asm => asm.Location);

        var paths = assemblies
            .Select(static asm => asm.Location)
            .Concat(loadedAssemblies)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(static path => !string.IsNullOrWhiteSpace(path) && File.Exists(path))
            .ToList();

        if (!paths.Any(static path => path.EndsWith("Xtraq.dll", StringComparison.OrdinalIgnoreCase)))
        {
            var solutionRoot = GetSolutionRoot();
            var fallback = Directory.GetFiles(solutionRoot, "Xtraq.dll", SearchOption.AllDirectories)
                .FirstOrDefault(static candidate => candidate.Contains(Path.Combine("bin", "Release"), StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(fallback) && File.Exists(fallback))
            {
                paths.Add(fallback);
            }
        }

        if (!paths.Any(static path => path.EndsWith("Xtraq.dll", StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("Unable to resolve Xtraq assembly path for template compilation.");
        }

        return paths.Select(static path => MetadataReference.CreateFromFile(path));
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
