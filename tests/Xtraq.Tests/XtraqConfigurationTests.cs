using System;
using System.Collections.Generic;
using System.IO;

namespace Xtraq.Tests.Configuration;

/// <summary>
/// Verifies that environment configuration loading honours tracked config metadata.
/// </summary>
public sealed class XtraqConfigurationTests
{
    /// <summary>
    /// Ensures .xtraqconfig values fill in missing non-sensitive settings during configuration loading.
    /// </summary>
    [Xunit.Fact]
    public void Load_WhenTrackableConfigPresent_UsesPersistedValues()
    {
        var cleanupKeys = new[]
        {
            "XTRAQ_NAMESPACE",
            "XTRAQ_OUTPUT_DIR",
            "XTRAQ_TFM",
            "XTRAQ_BUILD_SCHEMAS"
        };
        var snapshot = new Dictionary<string, string?>(cleanupKeys.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var key in cleanupKeys)
        {
            snapshot[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        var projectRoot = Directory.CreateTempSubdirectory("xtraq-envcfg-").FullName;
        try
        {
            File.WriteAllText(Path.Combine(projectRoot, ".env"), "XTRAQ_GENERATOR_DB=Server=(local);\n");
            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig"),
                "{\n"
                + "  \"Namespace\": \"Tracked.Namespace\",\n"
                + "  \"OutputDir\": \"Artifacts\",\n"
                + "  \"TargetFramework\": \"net10\",\n"
                + "  \"BuildSchemas\": [\n"
                + "    \"core\",\n"
                + "    \"finance\",\n"
                + "    \"finance\"\n"
                + "  ]\n"
                + "}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(projectRoot);

            Xunit.Assert.Equal("Tracked.Namespace", configuration.NamespaceRoot);
            Xunit.Assert.Equal("Artifacts", configuration.OutputDir);
            Xunit.Assert.Equal("net10", Environment.GetEnvironmentVariable("XTRAQ_TFM"));
            Xunit.Assert.Equal(new[] { "core", "finance" }, configuration.BuildSchemas);
        }
        finally
        {
            foreach (var kvp in snapshot)
            {
                Environment.SetEnvironmentVariable(kvp.Key, kvp.Value);
            }

            try
            {
                Directory.Delete(projectRoot, recursive: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    /// <summary>
    /// Ensures the loader fails when the tracked configuration is missing.
    /// </summary>
    [Xunit.Fact]
    public void Load_WithoutTrackableConfig_ThrowsInvalidOperation()
    {
        var originalNamespace = Environment.GetEnvironmentVariable("XTRAQ_NAMESPACE");
        Environment.SetEnvironmentVariable("XTRAQ_NAMESPACE", null);

        var projectRoot = Directory.CreateTempSubdirectory("xtraq-envcfg-missing-").FullName;
        try
        {
            File.WriteAllText(Path.Combine(projectRoot, ".env"), "XTRAQ_GENERATOR_DB=Server=(local);\n");

            var ex = Xunit.Record.Exception(() => Xtraq.Configuration.XtraqConfiguration.Load(projectRoot));
            Xunit.Assert.IsType<InvalidOperationException>(ex);
            Xunit.Assert.Contains("not initialised", ex!.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Environment.SetEnvironmentVariable("XTRAQ_NAMESPACE", originalNamespace);
            try
            {
                Directory.Delete(projectRoot, recursive: true);
            }
            catch
            {
            }
        }
    }
}
