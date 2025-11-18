using System;
using Xtraq.Utils;
using Xunit;

namespace Xtraq.Tests;

/// <summary>
/// Coordinates tests that mutate <see cref="DirectoryUtils"/> so they never run in parallel.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class DirectoryWorkspaceCollection : ICollectionFixture<DirectoryWorkspaceFixture>
{
    public const string Name = "DirectoryWorkspace";
}

/// <summary>
/// Resets <see cref="DirectoryUtils"/> before and after coordinated tests run.
/// </summary>
public sealed class DirectoryWorkspaceFixture : IDisposable
{
    public DirectoryWorkspaceFixture()
    {
        DirectoryUtils.ResetBasePath();
    }

    public void Dispose()
    {
        DirectoryUtils.ResetBasePath();
    }
}
