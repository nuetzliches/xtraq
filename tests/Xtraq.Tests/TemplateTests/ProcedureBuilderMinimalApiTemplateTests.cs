using System.Runtime.Loader;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xtraq.Cli;
using Xtraq.Engine;
using Xunit;

namespace Xtraq.Tests.TemplateTests;

public sealed class ProcedureBuilderMinimalApiTemplateTests
{
    [Fact]
    public void ProcedureBuildersTemplate_CompilesMinimalApiExtensions()
    {
        var root = GetSolutionRoot();
        var templatePath = System.IO.Path.Combine(root, "src", "Templates", "ProcedureBuilders.xqt");
        Assert.True(System.IO.File.Exists(templatePath), $"Template not found: {templatePath}");

        var template = System.IO.File.ReadAllText(templatePath);
        var globalUsingsPath = System.IO.Path.Combine(root, "src", "GlobalUsings.cs");
        Assert.True(System.IO.File.Exists(globalUsingsPath), $"Global usings not found: {globalUsingsPath}");
        var globalUsingsSource = System.IO.File.ReadAllText(globalUsingsPath);

        var engine = new SimpleTemplateEngine();
        var builderModel = new
        {
            HEADER = "// generated for tests",
            Namespace = "TestNamespace",
            Procedures = new[]
            {
                new
                {
                    DisplayName = "sample.SampleProc",
                    MethodName = "WithSampleSampleProcProcedure",
                    InputTypeName = "global::TestNamespace.Sample.SampleProcInput",
                    ResultTypeName = "global::TestNamespace.Sample.SampleProcResult",
                    ExtensionTypeName = "global::TestNamespace.Sample.SampleProcExtensions",
                    ProcedureMethodName = "SampleProcAsync"
                }
            }
        };
        var builderSource = "#define XTRAQ_MINIMAL_API\n#define NET8_0_OR_GREATER\n" + engine.Render(template, builderModel);

        var harnessSource = """
// harness
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace TestNamespace
{
    public interface IXtraqDbContext { }

    public delegate XtraqTransactionOptions? TransactionOptionsSelector<TInput>(ProcedureExecutionContext<TInput> context);

    public sealed class XtraqTransactionOptions { }

    public interface IXtraqTransactionOrchestrator
    {
        ValueTask<XtraqTransactionScope> BeginAsync(XtraqTransactionOptions? options = null, CancellationToken cancellationToken = default);
    }

    public interface IXtraqTransactionOrchestratorAccessor
    {
        IXtraqTransactionOrchestrator TransactionOrchestrator { get; }
    }

    public sealed class XtraqTransactionScope : IAsyncDisposable
    {
        public ValueTask CommitAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask RollbackAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    public sealed class FakeOrchestrator : IXtraqTransactionOrchestrator
    {
        public ValueTask<XtraqTransactionScope> BeginAsync(XtraqTransactionOptions? options = null, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(new XtraqTransactionScope());
    }

    public sealed class FakeContext : IXtraqDbContext, IXtraqTransactionOrchestratorAccessor
    {
        private readonly FakeOrchestrator _orchestrator = new();
        IXtraqTransactionOrchestrator IXtraqTransactionOrchestratorAccessor.TransactionOrchestrator => _orchestrator;
    }

    public sealed class TransactionScopeExecutionPolicy : IProcedureExecutionPolicy
    {
        public ValueTask<TResult> ExecuteAsync<TInput, TResult>(
            ProcedureExecutionContext<TInput> context,
            ProcedureCallDelegate<TInput, TResult> next,
            CancellationToken cancellationToken)
            => next(context, cancellationToken);
    }

    public static class TransactionScopeExecutionPolicyFactory
    {
        public static TransactionScopeExecutionPolicy Default => new();
        public static TransactionScopeExecutionPolicy FromOptions(XtraqTransactionOptions options) => new();
        public static TransactionScopeExecutionPolicy FromSelector(Func<IProcedureExecutionContext, XtraqTransactionOptions?> selector) => new();
        public static TransactionScopeExecutionPolicy FromSelector<TInput>(TransactionOptionsSelector<TInput> selector) => new();
    }

    public static class MinimalApiHarness
    {
        public static RouteHandlerBuilder Attach(RouteHandlerBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);
            return builder.WithProcedure<int, int>(pipeline => pipeline.WithExecutor(static (db, value, ct) => ValueTask.FromResult(value + 1)));
        }

        public static RouteHandlerBuilder AttachStream(RouteHandlerBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);
            return builder.WithProcedureStream<int, int, int, int>(
                pipeline => pipeline.WithExecutor(static (db, input, onRow, ct) =>
                {
                    _ = onRow;
                    return ValueTask.FromResult(0);
                }));
        }
    }

    namespace Sample
    {
        public readonly record struct SampleProcInput(int Value);

        public sealed class SampleProcResult
        {
            public static SampleProcResult Empty { get; } = new();
        }

        public static class SampleProcExtensions
        {
            public static Task<SampleProcResult> SampleProcAsync(this global::TestNamespace.IXtraqDbContext db, SampleProcInput input, CancellationToken cancellationToken = default)
            {
                _ = db;
                _ = input;
                _ = cancellationToken;
                return Task.FromResult(SampleProcResult.Empty);
            }
        }
    }
}
""";

        var syntaxTrees = new[]
        {
            CSharpSyntaxTree.ParseText(globalUsingsSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
            CSharpSyntaxTree.ParseText(builderSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
            CSharpSyntaxTree.ParseText(harnessSource, new CSharpParseOptions(LanguageVersion.CSharp12))
        };

        var references = GetMetadataReferences();
        var compilation = CSharpCompilation.Create(
            assemblyName: "ProcedureBuilderMinimalApi.Dynamic",
            syntaxTrees: syntaxTrees,
            references: references,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        using var peStream = new System.IO.MemoryStream();
        var emitResult = compilation.Emit(peStream);
        if (!emitResult.Success)
        {
            var diagnostics = string.Join(System.Environment.NewLine, emitResult.Diagnostics);
            throw new System.InvalidOperationException($"Compilation failed:{System.Environment.NewLine}{diagnostics}");
        }

        peStream.Position = 0;
        var assembly = AssemblyLoadContext.Default.LoadFromStream(peStream);
        Assert.NotNull(assembly);
    }

    private static System.Collections.Generic.IEnumerable<MetadataReference> GetMetadataReferences()
    {
        var trusted = (string?)System.AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES");
        if (string.IsNullOrEmpty(trusted))
        {
            throw new System.InvalidOperationException("Trusted platform assemblies could not be resolved.");
        }

        var entries = trusted.Split(System.IO.Path.PathSeparator, System.StringSplitOptions.RemoveEmptyEntries);
        var references = new System.Collections.Generic.List<MetadataReference>(entries.Length);
        foreach (var path in entries)
        {
            references.Add(MetadataReference.CreateFromFile(path));
        }

        var xtraqAssemblyPath = typeof(CommandOptions).Assembly.Location;
        if (!string.IsNullOrWhiteSpace(xtraqAssemblyPath))
        {
            references.Add(MetadataReference.CreateFromFile(xtraqAssemblyPath));
        }

        return references;
    }

    private static string GetSolutionRoot()
    {
        var directory = System.AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(directory))
        {
            if (System.IO.File.Exists(System.IO.Path.Combine(directory, "Xtraq.sln")))
            {
                return directory;
            }

            directory = System.IO.Directory.GetParent(directory)?.FullName;
        }

        throw new System.InvalidOperationException("Could not locate solution root.");
    }
}
