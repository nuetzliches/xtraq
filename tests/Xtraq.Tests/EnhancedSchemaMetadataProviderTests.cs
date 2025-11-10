namespace Xtraq.Tests;

/// <summary>
/// Validates that the enhanced schema metadata provider can operate in offline mode using the snapshot index.
/// </summary>
public sealed class EnhancedSchemaMetadataProviderTests
{
    /// <summary>
    /// Ensures that table column metadata is resolved from the snapshot index when database connectivity is unavailable.
    /// </summary>
    [Xunit.Fact]
    public async System.Threading.Tasks.Task ResolveTableColumnAsync_UsesSnapshotIndex_WhenDatabaseUnavailable()
    {
        var workspace = CreateWorkspace();
        Xtraq.Utils.DirectoryUtils.SetBasePath(workspace);

        try
        {
            await CreateSnapshotIndexAsync(workspace);

            var snapshotProvider = new Xtraq.Services.SnapshotIndexMetadataProvider();
            var console = new TestConsoleService();
            var provider = new Xtraq.Schema.EnhancedSchemaMetadataProvider(snapshotProvider, dbContext: null, console);

            Xunit.Assert.True(await provider.IsOfflineModeAvailableAsync());

            var column = await provider.ResolveTableColumnAsync("dbo", "Users", "Id");
            Xunit.Assert.NotNull(column);
            Xunit.Assert.Equal("Id", column!.Name);
            Xunit.Assert.Equal("int", column.SqlTypeName);
            Xunit.Assert.False(column.IsNullable);
            Xunit.Assert.True(column.IsFromSnapshot);

            var columns = await provider.GetTableColumnsAsync("dbo", "Users");
            Xunit.Assert.Single(columns);
            Xunit.Assert.Equal("Id", columns[0].Name);
        }
        finally
        {
            Xtraq.Utils.DirectoryUtils.SetBasePath(string.Empty);
            CleanupWorkspace(workspace);
        }
    }

    private static async System.Threading.Tasks.Task CreateSnapshotIndexAsync(string workspace)
    {
    var schemaPath = System.IO.Path.Combine(workspace, ".xtraq", "snapshots");
        System.IO.Directory.CreateDirectory(schemaPath);

        var json = """
                {
                    "schemaVersion": 1,
                    "fingerprint": "offline-test",
                    "parser": { "toolVersion": "tests", "resultSetParserVersion": 1 },
                    "stats": {},
                    "procedures": [
                        {
                            "schema": "app",
                            "name": "Example",
                            "file": "procedures/app.Example.json",
                            "hash": "hash",
                            "resultSets": [
                                {
                                    "returnsJson": false,
                                    "returnsJsonArray": false,
                                    "columns": [
                                        {
                                            "name": "Result",
                                            "sqlTypeName": "int",
                                            "isNullable": false,
                                            "sourceSchema": "dbo",
                                            "sourceTable": "Users",
                                            "sourceColumn": "Id"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
                """;

        await System.IO.File.WriteAllTextAsync(System.IO.Path.Combine(schemaPath, "index.json"), json);
    }

    private static string CreateWorkspace()
    {
        var identifier = System.Guid.NewGuid().ToString("N");
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xtraq-tests", identifier);
        System.IO.Directory.CreateDirectory(path);
        return path;
    }

    private static void CleanupWorkspace(string workspace)
    {
        try
        {
            if (System.IO.Directory.Exists(workspace))
            {
                System.IO.Directory.Delete(workspace, recursive: true);
            }
        }
        catch
        {
            // Best-effort cleanup.
        }
    }
}
