using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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

public sealed class TransactionOrchestratorTemplateTests
{
    [Fact]
    public async Task TransactionOrchestratorTemplate_SupportsNestedCommitAndRollback()
    {
        var root = GetSolutionRoot();
        var templatePath = Path.Combine(root, "src", "Templates", "TransactionOrchestrator.spt");
        Assert.True(File.Exists(templatePath), $"Template not found: {templatePath}");

        var template = File.ReadAllText(templatePath);
        var globalUsingsPath = Path.Combine(root, "src", "GlobalUsings.cs");
        Assert.True(File.Exists(globalUsingsPath), $"Global usings not found: {globalUsingsPath}");
        var globalUsingsSource = File.ReadAllText(globalUsingsPath);

        var orchestratorSource = template
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");

        var harnessSource = """
// harness
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace TestNamespace;

public interface IXtraqDbContext
{
    DbConnection OpenConnection();
    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);
    int CommandTimeout { get; }
}

public sealed class FakeDbContext : IXtraqDbContext
{
    private readonly FakeDbConnection _connection;

    public FakeDbContext(FakeDbConnection connection)
    {
        _connection = connection;
    }

    public DbConnection OpenConnection()
    {
        _connection.Open();
        return _connection;
    }

    public async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return _connection;
    }

    public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(true);

    public int CommandTimeout => 30;
}

public sealed class FakeDbConnection : DbConnection
{
    private readonly List<FakeDbTransaction> _transactions = new();
    private ConnectionState _state = ConnectionState.Closed;

    public bool DisposeCalled { get; private set; }
    public override string ConnectionString { get; set; } = string.Empty;
    public override string Database => "Fake";
    public override string DataSource => "Fake";
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        var tx = new FakeDbTransaction(this, isolationLevel);
        _transactions.Add(tx);
        return tx;
    }

    public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

    public override void Close()
    {
        _state = ConnectionState.Closed;
    }

    public override void Open()
    {
        _state = ConnectionState.Open;
    }

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Open();
        return Task.CompletedTask;
    }

    protected override DbCommand CreateDbCommand() => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeCalled = true;
            _state = ConnectionState.Closed;
        }
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        DisposeCalled = true;
        _state = ConnectionState.Closed;
        return ValueTask.CompletedTask;
    }
}

public sealed class FakeDbTransaction : DbTransaction
{
    private readonly FakeDbConnection _connection;

    public FakeDbTransaction(FakeDbConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        IsolationLevel = isolationLevel;
    }

    public bool CommitCalled { get; private set; }
    public bool RollbackCalled { get; private set; }
    public bool Disposed { get; private set; }
    public List<string> Savepoints { get; } = new();
    public List<string> RolledBackSavepoints { get; } = new();

    public override IsolationLevel IsolationLevel { get; }
    protected override DbConnection DbConnection => _connection;

    public override void Commit()
    {
        CommitCalled = true;
    }

    public override void Rollback()
    {
        RollbackCalled = true;
    }

    public override void Rollback(string savepointName)
    {
        RolledBackSavepoints.Add(savepointName);
    }

    public override void Save(string savepointName)
    {
        Savepoints.Add(savepointName);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Disposed = true;
        }
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

public static class OrchestratorHarness
{
    public static async Task<(bool RootCommitted, bool SavepointCreated, bool ConnectionDisposed, bool TransactionDisposed, bool HasActiveAfter)> RunCommitFlowAsync()
    {
        var connection = new FakeDbConnection();
        var context = new FakeDbContext(connection);
        var orchestrator = new XtraqTransactionOrchestrator(context);

        var root = await orchestrator.BeginAsync().ConfigureAwait(false);
        var nested = await orchestrator.BeginAsync().ConfigureAwait(false);
        var transaction = (FakeDbTransaction)nested.Transaction;

        var savepointCreated = transaction.Savepoints.Count == 1;

        await nested.CommitAsync().ConfigureAwait(false);
        await root.CommitAsync().ConfigureAwait(false);

        return (transaction.CommitCalled, savepointCreated, connection.DisposeCalled, transaction.Disposed, orchestrator.HasActiveTransaction);
    }

    public static async Task<(int SavepointRollbacks, bool RootRolledBack, bool ConnectionDisposed, bool TransactionDisposed, bool HasActiveAfter)> RunDisposeRollbackAsync()
    {
        var connection = new FakeDbConnection();
        var context = new FakeDbContext(connection);
        var orchestrator = new XtraqTransactionOrchestrator(context);

        var root = await orchestrator.BeginAsync().ConfigureAwait(false);
        var nested = await orchestrator.BeginAsync().ConfigureAwait(false);
        var transaction = (FakeDbTransaction)nested.Transaction;

        await nested.DisposeAsync().ConfigureAwait(false);
        await root.DisposeAsync().ConfigureAwait(false);

        return (transaction.RolledBackSavepoints.Count, transaction.RollbackCalled, connection.DisposeCalled, transaction.Disposed, orchestrator.HasActiveTransaction);
    }

    public static async Task<bool> RunRequiresNewThrowsAsync()
    {
        var connection = new FakeDbConnection();
        var context = new FakeDbContext(connection);
        var orchestrator = new XtraqTransactionOrchestrator(context);

        var root = await orchestrator.BeginAsync().ConfigureAwait(false);
        try
        {
            var options = new XtraqTransactionOptions { RequiresNew = true };
            await orchestrator.BeginAsync(options).ConfigureAwait(false);
            return false;
        }
        catch (NotSupportedException)
        {
            return true;
        }
        finally
        {
            await root.RollbackAsync().ConfigureAwait(false);
        }
    }
}
""";

        var syntaxTrees = new[]
        {
            CSharpSyntaxTree.ParseText(globalUsingsSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
            CSharpSyntaxTree.ParseText(orchestratorSource, new CSharpParseOptions(LanguageVersion.CSharp12)),
            CSharpSyntaxTree.ParseText(harnessSource, new CSharpParseOptions(LanguageVersion.CSharp12))
        };

        var references = GetMetadataReferences();
        var compilation = CSharpCompilation.Create(
            assemblyName: "TransactionOrchestratorTests.Dynamic",
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
        var harnessType = assembly.GetType("TestNamespace.OrchestratorHarness")!;

        var commitResult = await InvokeAsync<(bool RootCommitted, bool SavepointCreated, bool ConnectionDisposed, bool TransactionDisposed, bool HasActiveAfter)>(harnessType, "RunCommitFlowAsync");
        Assert.True(commitResult.RootCommitted);
        Assert.True(commitResult.SavepointCreated);
        Assert.True(commitResult.ConnectionDisposed);
        Assert.True(commitResult.TransactionDisposed);
        Assert.False(commitResult.HasActiveAfter);

        var rollbackResult = await InvokeAsync<(int SavepointRollbacks, bool RootRolledBack, bool ConnectionDisposed, bool TransactionDisposed, bool HasActiveAfter)>(harnessType, "RunDisposeRollbackAsync");
        Assert.Equal(1, rollbackResult.SavepointRollbacks);
        Assert.True(rollbackResult.RootRolledBack);
        Assert.True(rollbackResult.ConnectionDisposed);
        Assert.True(rollbackResult.TransactionDisposed);
        Assert.False(rollbackResult.HasActiveAfter);

        var requiresNew = await InvokeAsync<bool>(harnessType, "RunRequiresNewThrowsAsync");
        Assert.True(requiresNew);
    }

    private static async Task<T> InvokeAsync<T>(Type harnessType, string method)
    {
        var mi = harnessType.GetMethod(method, BindingFlags.Public | BindingFlags.Static)!;
        var task = (Task<T>)mi.Invoke(null, Array.Empty<object>())!;
        return await task.ConfigureAwait(false);
    }

    private static IEnumerable<MetadataReference> GetMetadataReferences()
    {
        var assemblies = new[]
        {
            typeof(object).Assembly,
            typeof(ValueTask).Assembly,
            typeof(Enumerable).Assembly,
            typeof(List<>).Assembly,
            typeof(Stack<>).Assembly,
            typeof(CancellationToken).Assembly,
            typeof(DbConnection).Assembly,
            typeof(MarshalByRefObject).Assembly,
            typeof(System.ComponentModel.Component).Assembly,
            typeof(System.Text.Json.JsonSerializer).Assembly,
            typeof(System.Text.RegularExpressions.Regex).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection).Assembly
        };

        var runtimeDirectory = Path.GetDirectoryName(typeof(object).Assembly.Location)!;
        var runtimeAssemblies = new[]
        {
            Path.Combine(runtimeDirectory, "System.Runtime.dll"),
            Path.Combine(runtimeDirectory, "System.ComponentModel.Primitives.dll"),
            Path.Combine(runtimeDirectory, "System.ComponentModel.TypeConverter.dll")
        };

        return assemblies
            .Select(asm => asm.Location)
            .Concat(runtimeAssemblies.Where(File.Exists))
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
