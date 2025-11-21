using System;
using System.IO;
using System.Threading.Tasks;
using Xtraq.Cache;
using Xtraq.Services;
using Xunit;

namespace Xtraq.Tests.Cache;

[Collection(DirectoryWorkspaceCollection.Name)]
public sealed class SchemaObjectCacheManagerIntegrationTests : IDisposable
{
    private readonly string _originalProjectPath;
    private readonly string _workspace;

    public SchemaObjectCacheManagerIntegrationTests()
    {
        _originalProjectPath = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH") ?? string.Empty;
        _workspace = Path.Combine(Path.GetTempPath(), "xtraq-cache-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_workspace);
        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", _workspace);
    }

    [Fact]
    public async Task RemoveAsync_RemovesEntryAndDependents_PersistsAcrossReload()
    {
        var console = new TestConsoleService();
        var tableRef = new SchemaObjectRef(SchemaObjectType.Table, "dbo", "Customers");
        var procRef = new SchemaObjectRef(SchemaObjectType.StoredProcedure, "dbo", "CustomerList");

        // First manager: seed entries and dependency, then flush.
        var manager = new SchemaObjectCacheManager(console);
        await manager.InitializeAsync();
        await manager.UpdateLastModifiedAsync(tableRef.Type, tableRef.Schema, tableRef.Name, DateTime.UtcNow);
        await manager.UpdateLastModifiedAsync(procRef.Type, procRef.Schema, procRef.Name, DateTime.UtcNow);
        await manager.RecordDependencyAsync(procRef, tableRef);
        await manager.FlushAsync();

        // Reload and verify dependency is present.
        var reloaded = new SchemaObjectCacheManager(console);
        await reloaded.InitializeAsync();
        Assert.Contains(procRef, reloaded.GetDependents(tableRef));
        Assert.NotNull(reloaded.GetLastModified(tableRef.Type, tableRef.Schema, tableRef.Name));

        // Remove base object, flush, reload again.
        await reloaded.RemoveAsync(tableRef);
        await reloaded.FlushAsync();

        var afterRemoval = new SchemaObjectCacheManager(console);
        await afterRemoval.InitializeAsync();

        Assert.Empty(afterRemoval.GetDependents(tableRef));
        Assert.Null(afterRemoval.GetLastModified(tableRef.Type, tableRef.Schema, tableRef.Name));
        Assert.NotNull(afterRemoval.GetLastModified(procRef.Type, procRef.Schema, procRef.Name));
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", _originalProjectPath.Length == 0 ? null : _originalProjectPath);
        try
        {
            if (Directory.Exists(_workspace))
            {
                Directory.Delete(_workspace, recursive: true);
            }
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
