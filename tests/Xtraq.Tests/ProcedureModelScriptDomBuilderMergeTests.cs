using System;
using System.IO;
using System.Linq;
using Xtraq.SnapshotBuilder.Analyzers;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProcedureModelScriptDomBuilderMergeTests
{
    private const string TableMetadata = "{\n  \"Schema\": \"sample\",\n  \"Name\": \"UserContacts\",\n  \"Columns\": [\n    {\n      \"Name\": \"ContactId\",\n      \"TypeRef\": \"sys.int\",\n      \"IsIdentity\": true\n    },\n    {\n      \"Name\": \"UserId\",\n      \"TypeRef\": \"sys.int\"\n    },\n    {\n      \"Name\": \"Email\",\n      \"TypeRef\": \"sys.nvarchar\",\n      \"MaxLength\": 320\n    },\n    {\n      \"Name\": \"DisplayName\",\n      \"TypeRef\": \"sys.nvarchar\",\n      \"MaxLength\": 200\n    },\n    {\n      \"Name\": \"Preferred\",\n      \"TypeRef\": \"sys.bit\",\n      \"HasDefaultValue\": true\n    },\n    {\n      \"Name\": \"LastInteractionUtc\",\n      \"TypeRef\": \"sys.datetime2\",\n      \"IsNullable\": true,\n      \"Precision\": 23,\n      \"Scale\": 3\n    },\n    {\n      \"Name\": \"UpdatedAtUtc\",\n      \"TypeRef\": \"sys.datetime2\",\n      \"IsNullable\": true,\n      \"Precision\": 23,\n      \"Scale\": 3\n    }\n  ]\n}\n";

    private const string MergeProcedure = @"CREATE OR ALTER PROCEDURE sample.SyncUserContacts
AS
BEGIN
    MERGE sample.UserContacts AS target
    USING (
        SELECT
            1 AS UserId,
            N'a@example.com' AS Email,
            N'Alice' AS DisplayName,
            CAST(0 AS bit) AS Preferred,
            CAST(NULL AS datetime2(3)) AS LastInteractionUtc
    ) AS source
        ON target.UserId = source.UserId
       AND target.Email = source.Email
    WHEN MATCHED THEN
        UPDATE SET target.DisplayName = source.DisplayName
    WHEN NOT MATCHED THEN
        INSERT (UserId, Email, DisplayName, Preferred, LastInteractionUtc)
        VALUES (source.UserId, source.Email, source.DisplayName, source.Preferred, source.LastInteractionUtc)
    OUTPUT
        $action AS MergeAction,
        inserted.ContactId,
        inserted.UserId,
        inserted.Email,
        inserted.DisplayName,
        inserted.Preferred,
        inserted.LastInteractionUtc,
        inserted.UpdatedAtUtc;
END;";

    [Fact]
    public void MergeOutput_UsesTargetTableMetadata()
    {
        lock (SnapshotTestLock.Gate)
        {
            var tempRoot = Path.Combine(Path.GetTempPath(), "xtraq-tests", Guid.NewGuid().ToString("N"));
            var tablesDir = Path.Combine(tempRoot, ".xtraq", "snapshots", "tables");
            Directory.CreateDirectory(tablesDir);
            File.WriteAllText(Path.Combine(tablesDir, "sample.UserContacts.json"), TableMetadata);

            var previousSnapshotRoot = Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT");
            var previousProjectRoot = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");

            try
            {
                Environment.SetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT", tempRoot);
                Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", tempRoot);

                var builder = new ProcedureModelScriptDomBuilder();
                var request = new ProcedureAstBuildRequest(MergeProcedure, "sample", null, false);
                var model = builder.Build(request);

                Assert.NotNull(model);
                var resultSet = Assert.Single(model!.ResultSets);
                Assert.Equal(8, resultSet.Columns.Count);

                var contactId = resultSet.Columns.Single(c => string.Equals(c.Name, "ContactId", StringComparison.Ordinal));
                Assert.Equal("int", contactId.SqlTypeName);
                Assert.False(contactId.IsNullable ?? true);

                var userId = resultSet.Columns.Single(c => string.Equals(c.Name, "UserId", StringComparison.Ordinal));
                Assert.Equal("int", userId.SqlTypeName);
                Assert.False(userId.IsNullable ?? true);

                var email = resultSet.Columns.Single(c => string.Equals(c.Name, "Email", StringComparison.Ordinal));
                Assert.Equal("nvarchar(320)", email.SqlTypeName);
                Assert.False(email.IsNullable ?? true);
                Assert.Equal(320, email.MaxLength);

                var preferred = resultSet.Columns.Single(c => string.Equals(c.Name, "Preferred", StringComparison.Ordinal));
                Assert.Equal("bit", preferred.SqlTypeName);
                Assert.False(preferred.IsNullable ?? true);

                var lastInteraction = resultSet.Columns.Single(c => string.Equals(c.Name, "LastInteractionUtc", StringComparison.Ordinal));
                Assert.Equal("datetime2(3)", lastInteraction.SqlTypeName);
                Assert.True(lastInteraction.IsNullable ?? false);

                var updatedAt = resultSet.Columns.Single(c => string.Equals(c.Name, "UpdatedAtUtc", StringComparison.Ordinal));
                Assert.Equal("datetime2(3)", updatedAt.SqlTypeName);
                Assert.True(updatedAt.IsNullable ?? false);
            }
            finally
            {
                Environment.SetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT", previousSnapshotRoot);
                Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", previousProjectRoot);
                try
                {
                    if (Directory.Exists(tempRoot))
                    {
                        Directory.Delete(tempRoot, true);
                    }
                }
                catch
                {
                    // Best-effort cleanup only.
                }
            }
        }
    }
}
