using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Xunit;
using Xtraq.SnapshotBuilder.Writers;

namespace Xtraq.Tests;

public sealed class SchemaArtifactWriterPruneTests
{
    [Fact]
    public void PruneExtraneousFiles_WithSchemaFilterProtectsUnrelatedSchemas()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            var keepPath = Path.Combine(tempDir, "sample.keep.json");
            File.WriteAllText(keepPath, "{\n  \"Schema\": \"sample\",\n  \"Name\": \"keep\"\n}");

            var otherPath = Path.Combine(tempDir, "other.keep.json");
            File.WriteAllText(otherPath, "{\n  \"Schema\": \"other\",\n  \"Name\": \"keep\"\n}");

            var deletePath = Path.Combine(tempDir, "sample.delete.json");
            File.WriteAllText(deletePath, "{\n  \"Schema\": \"sample\",\n  \"Name\": \"delete\"\n}");

            var valid = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                Path.GetFileName(keepPath)
            };
            var schemaFilter = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sample" };

            InvokePrune(tempDir, valid, schemaFilter);

            Assert.True(File.Exists(keepPath));
            Assert.True(File.Exists(otherPath));
            Assert.False(File.Exists(deletePath));
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    [Fact]
    public void PruneExtraneousFiles_RemovesMatchingSchemasWhenNotValid()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            var victimPath = Path.Combine(tempDir, "sample.orphan.json");
            File.WriteAllText(victimPath, "{\n  \"Schema\": \"sample\",\n  \"Name\": \"orphan\"\n}");

            var valid = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var schemaFilter = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "sample" };

            InvokePrune(tempDir, valid, schemaFilter);

            Assert.False(File.Exists(victimPath));
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    private static void InvokePrune(string directory, HashSet<string> valid, ISet<string>? schemaFilter)
    {
        var method = typeof(SchemaArtifactWriter)
            .GetMethod("PruneExtraneousFiles", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        method!.Invoke(null, new object?[] { directory, valid, schemaFilter });
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "xtraq-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
