using System;
using Xtraq.Execution;
using Xunit;

namespace Xtraq.Tests;

/// <summary>
/// Ensures tests mutating <see cref="ProcedureExecutor"/> global interceptors run in isolation.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ProcedureExecutorCollection : ICollectionFixture<ProcedureExecutorFixture>
{
    public const string Name = "ProcedureExecutor";
}

/// <summary>
/// Clears global interceptors before and after coordinated tests execute.
/// </summary>
public sealed class ProcedureExecutorFixture : IDisposable
{
    public ProcedureExecutorFixture()
    {
        ProcedureExecutor.ClearInterceptors();
    }

    public void Dispose()
    {
        ProcedureExecutor.ClearInterceptors();
    }
}
