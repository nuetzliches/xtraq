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
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
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
        var templatePath = Path.Combine(root, "src", "Templates", "TransactionOrchestrator.xqt");
        Assert.True(File.Exists(templatePath), $"Template not found: {templatePath}");

        var template = File.ReadAllText(templatePath);
        var globalUsingsPath = Path.Combine(root, "src", "GlobalUsings.cs");
        Assert.True(File.Exists(globalUsingsPath), $"Global usings not found: {globalUsingsPath}");
        var globalUsingsSource = File.ReadAllText(globalUsingsPath);

        var orchestratorSource = template
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");

        var optionsTemplatePath = Path.Combine(root, "src", "Templates", "DbContext", "XtraqDbContextOptions.xqt");
        Assert.True(File.Exists(optionsTemplatePath), $"Options template not found: {optionsTemplatePath}");
        var optionsTemplate = File.ReadAllText(optionsTemplatePath);
        Assert.Contains("TransactionOrchestratorFactory", optionsTemplate, StringComparison.Ordinal);
        var optionsSource = optionsTemplate
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");

        var serviceTemplatePath = Path.Combine(root, "src", "Templates", "DbContext", "XtraqDbContextServiceCollectionExtensions.xqt");
        Assert.True(File.Exists(serviceTemplatePath), $"Service extensions template not found: {serviceTemplatePath}");
        var serviceTemplate = File.ReadAllText(serviceTemplatePath);
        Assert.Contains("AddScoped<IXtraqTransactionOrchestrator>", serviceTemplate, StringComparison.Ordinal);
        var serviceSource = serviceTemplate
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");
        var serviceSourceForHarness = serviceSource.Replace("new XtraqDbContext(sp.GetRequiredService<XtraqDbContextOptions>(), sp)", "new FakeDbContext(sp.GetRequiredService<XtraqDbContextOptions>())");

        var adapterTemplatePath = Path.Combine(root, "src", "Templates", "ProcedureResultEntityAdapter.xqt");
        Assert.True(File.Exists(adapterTemplatePath), $"Adapter template not found: {adapterTemplatePath}");
        var adapterTemplate = File.ReadAllText(adapterTemplatePath);
        var adapterSource = adapterTemplate
            .Replace("{{ HEADER }}", "// generated for tests")
            .Replace("{{ Namespace }}", "TestNamespace");

        var policyTemplatePath = Path.Combine(root, "src", "Templates", "Policies", "TransactionExecutionPolicy.xqt");
        Assert.True(File.Exists(policyTemplatePath), $"Transaction policy template not found: {policyTemplatePath}");
        var policyTemplate = File.ReadAllText(policyTemplatePath);
        var policySource = policyTemplate
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
        using Microsoft.EntityFrameworkCore;
        using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace TestNamespace;

public interface IProcedureExecutionContext
{
    IXtraqDbContext DbContext { get; }
    object? InputValue { get; }
    string? Label { get; }
    IXtraqTransactionOrchestrator TransactionOrchestrator { get; }
}

public readonly record struct ProcedureExecutionContext<TInput>(
    IXtraqDbContext DbContext,
    TInput Input,
    string? Label,
    IXtraqTransactionOrchestrator TransactionOrchestrator) : IProcedureExecutionContext
{
    object? IProcedureExecutionContext.InputValue => Input;
}

public delegate ValueTask<TResult> ProcedureCallDelegate<TInput, TResult>(
    ProcedureExecutionContext<TInput> context,
    CancellationToken cancellationToken);

public interface IProcedureExecutionPolicy
{
    ValueTask<TResult> ExecuteAsync<TInput, TResult>(
        ProcedureExecutionContext<TInput> context,
        ProcedureCallDelegate<TInput, TResult> next,
        CancellationToken cancellationToken);
}

public interface IXtraqDbContext
{
    DbConnection OpenConnection();
    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);
    int CommandTimeout { get; }
}

public sealed class EfHarnessDbContext : DbContext
{
    public EfHarnessDbContext(DbContextOptions<EfHarnessDbContext> options)
        : base(options)
    {
    }

    public DbSet<Banner> Banners => Set<Banner>();
    public DbSet<ProcedureMetric> ProcedureMetrics => Set<ProcedureMetric>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Banner>(builder =>
        {
            builder.HasKey(x => x.BannerId);
            builder.Property(x => x.DisplayName);
        });

        modelBuilder.Entity<ProcedureMetric>(builder =>
        {
            builder.HasNoKey();
            builder.Property(x => x.Scope);
            builder.Property(x => x.Count);
        });
    }
}

public sealed class Banner
{
    public int BannerId { get; set; }
    public string? DisplayName { get; set; }
}

public sealed class ProcedureMetric
{
    public string Scope { get; set; } = string.Empty;
    public int Count { get; set; }
}

public readonly record struct BannerRow(int bannerId, string? displayName);

public readonly record struct ProcedureMetricRow(string scope, int count);

public sealed class FakeDbContext : IXtraqDbContext
{
    private readonly FakeDbConnection _connection;

    public FakeDbContext(FakeDbConnection connection)
    {
        _connection = connection;
    }

    public FakeDbContext(XtraqDbContextOptions options)
        : this(new FakeDbConnection())
    {
        options.ConnectionString ??= "fake-connection";
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

public sealed class FreshConnectionDbContext : IXtraqDbContext
{
    private readonly List<FakeDbConnection> _connections = new();

    public IReadOnlyList<FakeDbConnection> Connections => _connections;

    private FakeDbConnection CreateConnection()
    {
        var connection = new FakeDbConnection();
        _connections.Add(connection);
        return connection;
    }

    public DbConnection OpenConnection()
    {
        var connection = CreateConnection();
        connection.Open();
        return connection;
    }

    public async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(true);

    public int CommandTimeout => 30;
}

public sealed class AmbientDbContext : IXtraqDbContext, IXtraqAmbientTransactionAccessor
{
    private readonly FakeDbConnection _connection;
    private readonly FakeDbTransaction _transaction;

    public AmbientDbContext()
    {
        _connection = new FakeDbConnection();
        _connection.Open();
        _transaction = (FakeDbTransaction)_connection.BeginTransaction(IsolationLevel.ReadCommitted);
    }

    public FakeDbConnection Connection => _connection;
    public FakeDbTransaction Transaction => _transaction;

    public DbTransaction? AmbientTransaction => _transaction;
    public DbConnection? AmbientConnection => _connection;

    public DbConnection OpenConnection()
    {
        if (_connection.State != ConnectionState.Open)
        {
            _connection.Open();
        }

        return _connection;
    }

    public Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection.State != ConnectionState.Open)
        {
            _connection.Open();
        }

        return Task.FromResult<DbConnection>(_connection);
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
    public IReadOnlyList<FakeDbTransaction> Transactions => _transactions;
    public FakeDbTransaction? LastTransaction => _transactions.Count > 0 ? _transactions[^1] : null;
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

public sealed class DelegatingOrchestrator : IXtraqTransactionOrchestrator
{
    private readonly IXtraqTransactionOrchestrator _inner;

    public DelegatingOrchestrator(IXtraqTransactionOrchestrator inner)
    {
        _inner = inner;
    }

    public bool HasActiveTransaction => _inner.HasActiveTransaction;
    public DbConnection? CurrentConnection => _inner.CurrentConnection;
    public DbTransaction? CurrentTransaction => _inner.CurrentTransaction;
    public ValueTask<XtraqTransactionScope> BeginAsync(XtraqTransactionOptions? options = null, CancellationToken cancellationToken = default)
        => _inner.BeginAsync(options, cancellationToken);
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

    public static async Task<(bool DistinctTransactions, bool NestedCommitted, bool NestedRolledBack, bool RootCommitted, bool RootRolledBack, bool RootConnectionDisposed, bool NestedConnectionDisposed, bool NestedTransactionDisposed, bool HasActiveAfter, int ConnectionCount)> RunRequiresNewScopeAsync()
    {
        var context = new FreshConnectionDbContext();
        var orchestrator = new XtraqTransactionOrchestrator(context);

        var root = await orchestrator.BeginAsync().ConfigureAwait(false);
        var rootTransaction = (FakeDbTransaction)root.Transaction;

        var nestedOptions = new XtraqTransactionOptions { RequiresNew = true };
        var nested = await orchestrator.BeginAsync(nestedOptions).ConfigureAwait(false);
        var nestedTransaction = (FakeDbTransaction)nested.Transaction;
        var distinctTransactions = !ReferenceEquals(rootTransaction, nestedTransaction);

        await nested.CommitAsync().ConfigureAwait(false);
        await nested.DisposeAsync().ConfigureAwait(false);

        await root.RollbackAsync().ConfigureAwait(false);
        await root.DisposeAsync().ConfigureAwait(false);

        var connections = context.Connections;
        var rootConnectionDisposed = connections.Count > 0 && connections[0].DisposeCalled;
        var nestedConnectionDisposed = connections.Count > 1 && connections[1].DisposeCalled;

        return (
            distinctTransactions,
            nestedTransaction.CommitCalled,
            nestedTransaction.RollbackCalled,
            rootTransaction.CommitCalled,
            rootTransaction.RollbackCalled,
            rootConnectionDisposed,
            nestedConnectionDisposed,
            nestedTransaction.Disposed,
            orchestrator.HasActiveTransaction,
            connections.Count);
    }

    public static async Task<(bool CommitCalled, bool RollbackCalled, bool TransactionDisposed, bool ConnectionDisposed, bool HasActiveAfter)> RunAmbientCommitAsync()
    {
        var context = new AmbientDbContext();
        var orchestrator = new XtraqTransactionOrchestrator(context);

        await using (var scope = await orchestrator.BeginAsync().ConfigureAwait(false))
        {
            await scope.CommitAsync().ConfigureAwait(false);
        }

        return (
            context.Transaction.CommitCalled,
            context.Transaction.RollbackCalled,
            context.Transaction.Disposed,
            context.Connection.DisposeCalled,
            orchestrator.HasActiveTransaction);
    }

    public static async Task<(bool CommitCalled, bool RollbackCalled, bool TransactionDisposed, bool ConnectionDisposed, bool HasActiveAfter)> RunAmbientRollbackAsync()
    {
        var context = new AmbientDbContext();
        var orchestrator = new XtraqTransactionOrchestrator(context);

        await using (var scope = await orchestrator.BeginAsync().ConfigureAwait(false))
        {
            await scope.RollbackAsync().ConfigureAwait(false);
        }

        return (
            context.Transaction.CommitCalled,
            context.Transaction.RollbackCalled,
            context.Transaction.Disposed,
            context.Connection.DisposeCalled,
            orchestrator.HasActiveTransaction);
    }

    public static async Task<(bool DefaultResolved, bool FactoryResolved)> RunServiceRegistrationAsync()
    {
        var defaultServices = new ServiceCollection();
        defaultServices.AddXtraqDbContext(options =>
        {
            options.ConnectionString = "fake";
        });

        using var defaultProvider = defaultServices.BuildServiceProvider();
        using var defaultScope = defaultProvider.CreateScope();
        var defaultOrchestrator = defaultScope.ServiceProvider.GetRequiredService<IXtraqTransactionOrchestrator>();
        var defaultResolved = defaultOrchestrator is XtraqTransactionOrchestrator;
        await using (var scope = await defaultOrchestrator.BeginAsync().ConfigureAwait(false))
        {
            await scope.CommitAsync().ConfigureAwait(false);
        }

        var factoryServices = new ServiceCollection();
        factoryServices.AddXtraqDbContext(options =>
        {
            options.ConnectionString = "fake";
            options.TransactionOrchestratorFactory = sp => new DelegatingOrchestrator(new XtraqTransactionOrchestrator(sp.GetRequiredService<IXtraqDbContext>()));
        });

        using var factoryProvider = factoryServices.BuildServiceProvider();
        using var factoryScope = factoryProvider.CreateScope();
        var factoryOrchestrator = factoryScope.ServiceProvider.GetRequiredService<IXtraqTransactionOrchestrator>();
        var factoryResolved = factoryOrchestrator is DelegatingOrchestrator;
        await using (var scope = await factoryOrchestrator.BeginAsync().ConfigureAwait(false))
        {
            await scope.RollbackAsync().ConfigureAwait(false);
        }

        return (defaultResolved, factoryResolved);
    }

    public static async Task<(bool UsesAdapter, bool AmbientReused, bool DedicatedCreated)> RunUseXtraqProceduresAsync()
    {
        var services = new ServiceCollection();
        services.AddDbContext<EfHarnessDbContext>(options =>
        {
            options.UseSqlite("Data Source=:memory:");
        });

        services.AddXtraqDbContext(options =>
        {
            options.ConnectionString = "Data Source=:memory:";
        });

        services.UseXtraqProcedures<EfHarnessDbContext>();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var resolved = scope.ServiceProvider.GetRequiredService<IXtraqDbContext>();
        var usesAdapter = resolved.GetType().Name.Contains("EntityFrameworkXtraqContext", StringComparison.Ordinal);

        var efContext = scope.ServiceProvider.GetRequiredService<EfHarnessDbContext>();
        await efContext.Database.OpenConnectionAsync().ConfigureAwait(false);

        bool ambientReused;
        await using (var ambientTransaction = await efContext.Database.BeginTransactionAsync().ConfigureAwait(false))
        {
            var ambientConnection = await resolved.OpenConnectionAsync().ConfigureAwait(false);
            ambientReused = ReferenceEquals(ambientConnection, efContext.Database.GetDbConnection());

            await ambientTransaction.RollbackAsync().ConfigureAwait(false);
        }

        var standalone = await resolved.OpenConnectionAsync().ConfigureAwait(false);
        var dedicatedCreated = !ReferenceEquals(standalone, efContext.Database.GetDbConnection());
        await standalone.DisposeAsync().ConfigureAwait(false);

        return (usesAdapter, ambientReused, dedicatedCreated);
    }

    public static async Task<(bool AdapterRegistered, bool TracksTwo, bool ReusesTracked, bool KeylessProjected, bool KeylessTracked)> RunProcedureResultEntityAdapterAsync()
    {
        var services = new ServiceCollection();
        services.AddDbContext<EfHarnessDbContext>(options =>
        {
            options.UseSqlite("Data Source=:memory:");
        });

        services.AddXtraqDbContext(options =>
        {
            options.ConnectionString = "Data Source=:memory:";
        });

        services.UseXtraqProcedures<EfHarnessDbContext>();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var adapter = scope.ServiceProvider.GetService<ProcedureResultEntityAdapter<EfHarnessDbContext>>();
        var adapterRegistered = adapter is not null;
        if (adapter is null)
        {
            throw new InvalidOperationException("ProcedureResultEntityAdapter was not registered.");
        }

        var context = scope.ServiceProvider.GetRequiredService<EfHarnessDbContext>();
        await context.Database.EnsureCreatedAsync().ConfigureAwait(false);

        var initialRows = new[]
        {
            new BannerRow(1, "One"),
            new BannerRow(2, "Two")
        };

        var tracked = adapter.AttachEntities<Banner, BannerRow>(initialRows);
        var tracksTwo = tracked.Count == 2 && context.ChangeTracker.Entries<Banner>().Count() == 2;

        var updatedRows = new[]
        {
            new BannerRow(1, "One-updated")
        };

        var refreshed = adapter.AttachEntities<Banner, BannerRow>(updatedRows);
        var reusesTracked = ReferenceEquals(tracked[0], refreshed[0]) && refreshed[0].DisplayName == "One-updated";

        var metricRows = new[]
        {
            new ProcedureMetricRow("scope", 5)
        };

        var keyless = adapter.ProjectKeyless<ProcedureMetric, ProcedureMetricRow>(metricRows);
        var keylessProjected = keyless.Count == 1 && keyless[0].Scope == "scope" && keyless[0].Count == 5;
        var keylessTracked = context.ChangeTracker.Entries<ProcedureMetric>().Any();

        return (adapterRegistered, tracksTwo, reusesTracked, keylessProjected, keylessTracked);
    }

    public static async Task<(bool Committed, bool RolledBack)> RunPolicyIntegrationAsync()
    {
        var connection = new FakeDbConnection();
        var context = new FakeDbContext(connection);
        var orchestrator = new XtraqTransactionOrchestrator(context);
        var policy = new TransactionScopeExecutionPolicy();
        var execContext = new ProcedureExecutionContext<int>(context, 5, "policy-success", orchestrator);

        var result = await policy.ExecuteAsync(execContext, static async ValueTask<int> (ctx, ct) =>
        {
            await Task.Delay(1, ct).ConfigureAwait(false);
            return 123;
        }, CancellationToken.None).ConfigureAwait(false);

        _ = result;
        var transaction = connection.LastTransaction!;
        return (transaction.CommitCalled, transaction.RollbackCalled);
    }

    public static async Task<(bool Committed, bool RolledBack)> RunPolicyRollbackAsync()
    {
        var connection = new FakeDbConnection();
        var context = new FakeDbContext(connection);
        var orchestrator = new XtraqTransactionOrchestrator(context);
        var policy = new TransactionScopeExecutionPolicy();
        var execContext = new ProcedureExecutionContext<int>(context, 5, "policy-failure", orchestrator);

        try
        {
            await policy.ExecuteAsync(execContext, static async ValueTask<int> (ctx, ct) =>
            {
                await Task.Delay(1, ct).ConfigureAwait(false);
                throw new InvalidOperationException("policy failure");
            }, CancellationToken.None).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
        }

        var transaction = connection.LastTransaction!;
        return (transaction.CommitCalled, transaction.RollbackCalled || transaction.RolledBackSavepoints.Count > 0);
    }
}
""";

        var parseOptions = new CSharpParseOptions(LanguageVersion.CSharp12, preprocessorSymbols: new[] { "XTRAQ_ENTITY_FRAMEWORK" });

        var syntaxTrees = new[]
        {
            CSharpSyntaxTree.ParseText(globalUsingsSource, parseOptions),
            CSharpSyntaxTree.ParseText(orchestratorSource, parseOptions),
            CSharpSyntaxTree.ParseText(optionsSource, parseOptions),
            CSharpSyntaxTree.ParseText(serviceSourceForHarness, parseOptions),
            CSharpSyntaxTree.ParseText(adapterSource, parseOptions),
            CSharpSyntaxTree.ParseText(policySource, parseOptions),
            CSharpSyntaxTree.ParseText(harnessSource, parseOptions)
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

        var requiresNew = await InvokeAsync<(bool DistinctTransactions, bool NestedCommitted, bool NestedRolledBack, bool RootCommitted, bool RootRolledBack, bool RootConnectionDisposed, bool NestedConnectionDisposed, bool NestedTransactionDisposed, bool HasActiveAfter, int ConnectionCount)>(harnessType, "RunRequiresNewScopeAsync");
        Assert.True(requiresNew.DistinctTransactions);
        Assert.True(requiresNew.NestedCommitted);
        Assert.False(requiresNew.NestedRolledBack);
        Assert.False(requiresNew.RootCommitted);
        Assert.True(requiresNew.RootRolledBack);
        Assert.True(requiresNew.RootConnectionDisposed);
        Assert.True(requiresNew.NestedConnectionDisposed);
        Assert.True(requiresNew.NestedTransactionDisposed);
        Assert.False(requiresNew.HasActiveAfter);
        Assert.Equal(2, requiresNew.ConnectionCount);

        var ambientCommit = await InvokeAsync<(bool CommitCalled, bool RollbackCalled, bool TransactionDisposed, bool ConnectionDisposed, bool HasActiveAfter)>(harnessType, "RunAmbientCommitAsync");
        Assert.False(ambientCommit.CommitCalled);
        Assert.False(ambientCommit.RollbackCalled);
        Assert.False(ambientCommit.TransactionDisposed);
        Assert.False(ambientCommit.ConnectionDisposed);
        Assert.False(ambientCommit.HasActiveAfter);

        var ambientRollback = await InvokeAsync<(bool CommitCalled, bool RollbackCalled, bool TransactionDisposed, bool ConnectionDisposed, bool HasActiveAfter)>(harnessType, "RunAmbientRollbackAsync");
        Assert.False(ambientRollback.CommitCalled);
        Assert.True(ambientRollback.RollbackCalled);
        Assert.False(ambientRollback.TransactionDisposed);
        Assert.False(ambientRollback.ConnectionDisposed);
        Assert.False(ambientRollback.HasActiveAfter);

        var diResult = await InvokeAsync<(bool DefaultResolved, bool FactoryResolved)>(harnessType, "RunServiceRegistrationAsync");
        Assert.True(diResult.DefaultResolved);
        Assert.True(diResult.FactoryResolved);

        var efResult = await InvokeAsync<(bool UsesAdapter, bool AmbientReused, bool DedicatedCreated)>(harnessType, "RunUseXtraqProceduresAsync");
        Assert.True(efResult.UsesAdapter);
        Assert.True(efResult.AmbientReused);
        Assert.True(efResult.DedicatedCreated);

        var adapterResult = await InvokeAsync<(bool AdapterRegistered, bool TracksTwo, bool ReusesTracked, bool KeylessProjected, bool KeylessTracked)>(harnessType, "RunProcedureResultEntityAdapterAsync");
        Assert.True(adapterResult.AdapterRegistered);
        Assert.True(adapterResult.TracksTwo);
        Assert.True(adapterResult.ReusesTracked);
        Assert.True(adapterResult.KeylessProjected);
        Assert.False(adapterResult.KeylessTracked);

        var policyResult = await InvokeAsync<(bool Committed, bool RolledBack)>(harnessType, "RunPolicyIntegrationAsync");
        Assert.Equal((true, false), policyResult);

        var policyRollbackResult = await InvokeAsync<(bool Committed, bool RolledBack)>(harnessType, "RunPolicyRollbackAsync");
        Assert.Equal((false, true), policyRollbackResult);
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
            typeof(IServiceProvider).Assembly,
            typeof(Microsoft.Extensions.Configuration.IConfiguration).Assembly,
            typeof(MarshalByRefObject).Assembly,
            typeof(System.ComponentModel.Component).Assembly,
            typeof(System.Text.Json.JsonSerializer).Assembly,
            typeof(System.Text.RegularExpressions.Regex).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollection).Assembly,
            typeof(Microsoft.Extensions.DependencyInjection.ServiceCollectionContainerBuilderExtensions).Assembly,
            typeof(DbContext).Assembly,
            typeof(SqlConnection).Assembly,
            typeof(Microsoft.EntityFrameworkCore.SqliteDbContextOptionsBuilderExtensions).Assembly,
            typeof(Microsoft.Data.Sqlite.SqliteConnection).Assembly,
            typeof(Microsoft.EntityFrameworkCore.RelationalDatabaseFacadeExtensions).Assembly,
            typeof(Microsoft.Extensions.Configuration.ConfigurationExtensions).Assembly,
            typeof(System.Linq.Expressions.Expression).Assembly
        };

        var runtimeDirectory = Path.GetDirectoryName(typeof(object).Assembly.Location)!;
        var runtimeAssemblies = new[]
        {
            Path.Combine(runtimeDirectory, "System.Runtime.dll"),
            Path.Combine(runtimeDirectory, "System.ComponentModel.Primitives.dll"),
            Path.Combine(runtimeDirectory, "System.ComponentModel.TypeConverter.dll"),
            Path.Combine(runtimeDirectory, "System.ComponentModel.dll")
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
