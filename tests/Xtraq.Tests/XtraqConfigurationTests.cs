using System;
using System.Collections.Generic;
using System.IO;

namespace Xtraq.Tests.Configuration;

/// <summary>
/// Verifies that environment configuration loading honours tracked redirect metadata.
/// </summary>
public sealed class XtraqConfigurationTests
{
    /// <summary>
    /// Ensures that a redirecting .xtraqconfig loads the project root and .env content from the referenced directory.
    /// </summary>
    [Xunit.Fact]
    public void Load_WhenRedirectConfigPresent_ResolvesProjectRootAndEnv()
    {
        var cleanupKeys = new[]
        {
            "XTRAQ_PROJECT_ROOT",
            "XTRAQ_NAMESPACE",
            "XTRAQ_GENERATOR_DB"
        };
        var snapshot = new Dictionary<string, string?>(cleanupKeys.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var key in cleanupKeys)
        {
            snapshot[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        var outer = Directory.CreateTempSubdirectory("xtraq-redirect-");
        var innerPath = Path.Combine(outer.FullName, "project-root");
        Directory.CreateDirectory(innerPath);

        try
        {
            File.WriteAllText(Path.Combine(innerPath, ".env"),
                "XTRAQ_GENERATOR_DB=Server=(local);Database=App;\n"
                + "XTRAQ_NAMESPACE=Redirect.Namespace\n");

            Xtraq.Configuration.TrackableConfigManager.WriteDefaultProjectPath(innerPath);

            File.WriteAllText(Path.Combine(outer.FullName, ".xtraqconfig"),
                "{\n  \"ProjectPath\": \"project-root\"\n}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(outer.FullName);

            var expectedRoot = Path.GetFullPath(innerPath);
            Xunit.Assert.Equal(expectedRoot, configuration.ProjectRoot);
            Xunit.Assert.Equal("Redirect.Namespace", configuration.NamespaceRoot);
            Xunit.Assert.Equal("Server=(local);Database=App;", configuration.GeneratorConnectionString);
            Xunit.Assert.Equal(expectedRoot, Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT"));
        }
        finally
        {
            foreach (var kvp in snapshot)
            {
                Environment.SetEnvironmentVariable(kvp.Key, kvp.Value);
            }

            TryDeleteDirectory(innerPath);
            TryDeleteDirectory(outer.FullName);
        }
    }

    /// <summary>
    /// Ensures that a colocated .xtraqconfig.local overrides tracked defaults from .xtraqconfig.
    /// </summary>
    [Xunit.Fact]
    public void Load_WhenLocalConfigPresent_UsesLocalOverrides()
    {
        var cleanupKeys = new[]
        {
            "XTRAQ_PROJECT_ROOT",
            "XTRAQ_NAMESPACE",
            "XTRAQ_OUTPUT_DIR",
            "XTRAQ_BUILD_SCHEMAS",
            "XTRAQ_GENERATOR_DB"
        };

        var snapshot = new Dictionary<string, string?>(cleanupKeys.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var key in cleanupKeys)
        {
            snapshot[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        var projectRoot = Directory.CreateTempSubdirectory("xtraq-localcfg-").FullName;

        try
        {
            File.WriteAllText(Path.Combine(projectRoot, ".env"),
                "XTRAQ_GENERATOR_DB=Server=(local);Database=App;TrustServerCertificate=True;\n");

            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig"),
                "{\n  \"Namespace\": \"Tracked.Namespace\",\n  \"OutputDir\": \"TrackedOutput\"\n}\n");

            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig.local"),
                "{\n  \"Namespace\": \"Local.Namespace\",\n  \"OutputDir\": \"LocalOutput\",\n  \"BuildSchemas\": [\"LocalOne\", \"LocalTwo\"]\n}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(projectRoot);

            Xunit.Assert.Equal("Local.Namespace", configuration.NamespaceRoot);
            Xunit.Assert.Equal("LocalOutput", configuration.OutputDir);
            Xunit.Assert.Equal(new[] { "LocalOne", "LocalTwo" }, configuration.BuildSchemas);
            Xunit.Assert.Equal(Path.Combine(projectRoot, ".xtraqconfig.local"), configuration.ConfigPath);
        }
        finally
        {
            foreach (var kvp in snapshot)
            {
                Environment.SetEnvironmentVariable(kvp.Key, kvp.Value);
            }

            TryDeleteDirectory(projectRoot);
        }
    }

    /// <summary>
    /// Ensures the loader fails when no tracked configuration is present alongside the .env.
    /// </summary>
    [Xunit.Fact]
    public void Load_WithoutTrackableConfig_ThrowsInvalidOperation()
    {
        var originalRoot = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", null);

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
            Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", originalRoot);
            TryDeleteDirectory(projectRoot);
        }
    }

    private static void TryDeleteDirectory(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // Best-effort cleanup for temporary directories.
        }
    }
}
