using Xtraq.Metadata;
using Xunit;

namespace Xtraq.Tests;

public sealed class TableTypeMetadataProviderTests
{
    [Fact]
    public void GetAll_UsesIndexDocumentWhenAvailable()
    {
        var root = CreateWorkspace();
        try
        {
            var snapshots = EnsureSnapshotDirectory(root);
            var indexPath = System.IO.Path.Combine(snapshots, "index.json");
            System.IO.File.WriteAllText(indexPath, """
{
  "Version": 5,
  "UserDefinedTableTypes": [
    {
      "Catalog": "main",
      "Schema": "sample",
      "Name": "UserContactTableType",
      "Columns": [
        { "Name": "UserId", "TypeRef": "sys.int", "IsNullable": false },
        { "Name": "Email", "TypeRef": "sys.nvarchar", "MaxLength": 256, "IsNullable": true }
      ]
    },
    {
      "Schema": "shared",
      "Name": "AuditLogEntryTableType",
      "Columns": [
        { "Name": "EntryId", "TypeRef": "sys.uniqueidentifier", "IsNullable": false }
      ]
    }
  ]
}
""");

            var provider = new TableTypeMetadataProvider(root);
            var result = provider.GetAll();

            Assert.Equal(2, result.Count);

            var contacts = FindType(result, "UserContactTableType");
            Assert.Equal("sample", contacts.Schema);
            Assert.Equal("main", contacts.Catalog);
            Assert.Equal(2, contacts.Columns.Count);

            var email = FindColumn(contacts.Columns, "Email");
            Assert.Equal("nvarchar(256)", email.SqlType);
            Assert.True(email.IsNullable);

            var audit = FindType(result, "AuditLogEntryTableType");
            Assert.Equal("shared", audit.Schema);
            Assert.Null(audit.Catalog);
            Assert.Single(audit.Columns);
        }
        finally
        {
            TryDeleteDirectory(root);
        }
    }

    [Fact]
    public void GetAll_ReadsSplitTableTypeFilesAndMergesProcedureReferences()
    {
        var root = CreateWorkspace();
        try
        {
            var snapshots = EnsureSnapshotDirectory(root);
            var tableTypesDir = System.IO.Path.Combine(snapshots, "tabletypes");
            System.IO.Directory.CreateDirectory(tableTypesDir);
            System.IO.File.WriteAllText(System.IO.Path.Combine(tableTypesDir, "shared.AuditLogEntryTableType.json"), """
{
  "Schema": "shared",
  "Name": "AuditLogEntryTableType",
  "Columns": [
    { "Name": "EntryId", "TypeRef": "sys.bigint", "IsNullable": false },
    { "Name": "JsonBody", "TypeRef": "sys.nvarchar", "MaxLength": -1, "IsNullable": true }
  ]
}
""");

            var proceduresDir = System.IO.Path.Combine(snapshots, "procedures");
            System.IO.Directory.CreateDirectory(proceduresDir);
            System.IO.File.WriteAllText(System.IO.Path.Combine(proceduresDir, "dbo.SyncUsers.json"), """
{
  "Schema": "dbo",
  "Parameters": [
    { "Name": "@Entries", "IsTableType": true, "TableTypeSchema": "other", "TableTypeName": "MissingUdtt" }
  ]
}
""");

            var provider = new TableTypeMetadataProvider(root);
            var result = provider.GetAll();

            Assert.Equal(2, result.Count);

            var audit = FindType(result, "AuditLogEntryTableType");
            Assert.Equal(2, audit.Columns.Count);
            var jsonBody = FindColumn(audit.Columns, "JsonBody");
            Assert.Equal("nvarchar(max)", jsonBody.SqlType);
            Assert.True(jsonBody.IsNullable);

            var inferred = FindType(result, "MissingUdtt");
            Assert.Equal("other", inferred.Schema);
            Assert.Empty(inferred.Columns);
        }
        finally
        {
            TryDeleteDirectory(root);
        }
    }

    [Fact]
    public void GetAll_InferTableTypeFromTypeRefWhenFlagsAreMissing()
    {
        var root = CreateWorkspace();
        try
        {
            var snapshots = EnsureSnapshotDirectory(root);
            var proceduresDir = System.IO.Path.Combine(snapshots, "procedures");
            System.IO.Directory.CreateDirectory(proceduresDir);
            System.IO.File.WriteAllText(System.IO.Path.Combine(proceduresDir, "audit.Sync.json"), """
{
  "Schema": "audit",
  "Parameters": [
    { "Name": "@Payload", "TableTypeName": "", "TypeRef": "analytics.shared.SyncPayloadType" }
  ]
}
""");

            var provider = new TableTypeMetadataProvider(root);
            var result = provider.GetAll();

            Assert.Single(result);
            var inferred = result[0];
            Assert.Equal("shared", inferred.Schema);
            Assert.Equal("SyncPayloadType", inferred.Name);
            Assert.Equal("analytics", inferred.Catalog);
            Assert.Empty(inferred.Columns);
        }
        finally
        {
            TryDeleteDirectory(root);
        }
    }

    private static string CreateWorkspace()
    {
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "xtraq-udtt-tests", System.Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(path);
        return path;
    }

    private static string EnsureSnapshotDirectory(string root)
    {
        var dir = System.IO.Path.Combine(root, ".xtraq", "snapshots");
        System.IO.Directory.CreateDirectory(dir);
        return dir;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (System.IO.Directory.Exists(path))
            {
                System.IO.Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // best-effort cleanup
        }
    }

    private static TableTypeInfo FindType(System.Collections.Generic.IReadOnlyList<TableTypeInfo> types, string name)
    {
        foreach (var type in types)
        {
            if (string.Equals(type.Name, name, System.StringComparison.Ordinal))
            {
                return type;
            }
        }

        throw new System.InvalidOperationException($"Table type '{name}' not found.");
    }

    private static ColumnInfo FindColumn(System.Collections.Generic.IReadOnlyList<ColumnInfo> columns, string name)
    {
        foreach (var column in columns)
        {
            if (string.Equals(column.Name, name, System.StringComparison.Ordinal))
            {
                return column;
            }
        }

        throw new System.InvalidOperationException($"Column '{name}' not found.");
    }
}
