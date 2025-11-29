using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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
            "XTRAQ_PROJECT_PATH",
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
            Xunit.Assert.Equal(expectedRoot, Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH"));
            Xunit.Assert.Equal(expectedRoot, Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT"));
            Xunit.Assert.Equal(Xtraq.Configuration.ApiMode.None, configuration.ApiMode);
            Xunit.Assert.False(configuration.EntityFrameworkEnabled);
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
            "XTRAQ_PROJECT_PATH",
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
                "{\n  \"Namespace\": \"Tracked.Namespace\",\n  \"OutputDir\": \"TrackedOutput\",\n  \"Api\": { \"Mode\": \"None\" },\n  \"EntityFramework\": { \"Enabled\": false }\n}\n");

            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig.local"),
                "{\n  \"Namespace\": \"Local.Namespace\",\n  \"OutputDir\": \"LocalOutput\",\n  \"BuildSchemas\": [\"LocalOne\", \"LocalTwo\"],\n  \"Api\": { \"Mode\": \"Minimal\" },\n  \"EntityFramework\": { \"Enabled\": true }\n}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(projectRoot);

            Xunit.Assert.Equal("Local.Namespace", configuration.NamespaceRoot);
            Xunit.Assert.Equal("LocalOutput", configuration.OutputDir);
            Xunit.Assert.Equal(new[] { "LocalOne", "LocalTwo" }, configuration.BuildSchemas);
            Xunit.Assert.Equal(Xtraq.Configuration.ApiMode.Minimal, configuration.ApiMode);
            Xunit.Assert.True(configuration.EntityFrameworkEnabled);
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
        var originalPath = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH");
        var originalRoot = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", null);
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
            Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", originalPath);
            Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", originalRoot);
            TryDeleteDirectory(projectRoot);
        }
    }

    /// <summary>
    /// Ensures MinimalApi object config auto-binds per-procedure allow-lists.
    /// </summary>
    [Xunit.Fact]
    public void Load_WhenMinimalApiObjectSpecified_ResolvesAutoBindLists()
    {
        var cleanupKeys = new[]
        {
            "XTRAQ_PROJECT_PATH",
            "XTRAQ_PROJECT_ROOT",
            "XTRAQ_NAMESPACE",
            "XTRAQ_GENERATOR_DB",
            "XTRAQ_API_MODE",
            "XTRAQ_API_AUTOBIND",
            "XTRAQ_API_AUTOBIND_PROCEDURES"
        };

        var snapshot = new Dictionary<string, string?>(cleanupKeys.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var key in cleanupKeys)
        {
            snapshot[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        var projectRoot = Directory.CreateTempSubdirectory("xtraq-autobind-").FullName;

        try
        {
            File.WriteAllText(Path.Combine(projectRoot, ".env"),
                "XTRAQ_GENERATOR_DB=Server=(local);Database=App;TrustServerCertificate=True;\n");

            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig"),
                "{\n" +
                "  \"Namespace\": \"AutoBind.Namespace\",\n" +
                "  \"Api\": { \"Mode\": \"Minimal\", \"Requests\": { \"AutoBind\": [\"@UserId INT\", \"@Entries shared.AuditLogEntryTableType READONLY\"], \"AutoBindProcedures\": [\"sample.UserCompositeJsonSnapshot\", \"sample.WriteAuditLogEntries\"] } }\n" +
                "}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(projectRoot);

            Xunit.Assert.Equal(Xtraq.Configuration.ApiMode.Minimal, configuration.ApiMode);
            Xunit.Assert.Equal(new[] { "@UserId INT", "@Entries shared.AuditLogEntryTableType READONLY" }, configuration.ApiAutoBindParameters);
            Xunit.Assert.Equal(new[] { "sample.UserCompositeJsonSnapshot", "sample.WriteAuditLogEntries" }, configuration.ApiAutoBindProcedures);
            Xunit.Assert.Equal(Path.Combine(projectRoot, ".xtraqconfig"), configuration.ConfigPath);
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
    /// Allows offline build scenarios to load configuration without requiring XTRAQ_GENERATOR_DB.
    /// </summary>
    [Xunit.Fact]
    public void Load_WhenGeneratorConnectionOptional_SucceedsWithoutConnection()
    {
        var cleanupKeys = new[]
        {
            "XTRAQ_PROJECT_PATH",
            "XTRAQ_PROJECT_ROOT",
            "XTRAQ_GENERATOR_DB"
        };

        var snapshot = new Dictionary<string, string?>(cleanupKeys.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var key in cleanupKeys)
        {
            snapshot[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        var projectRoot = Directory.CreateTempSubdirectory("xtraq-offline-build-").FullName;

        try
        {
            File.WriteAllText(Path.Combine(projectRoot, ".xtraqconfig"),
                "{\n  \"Namespace\": \"Offline.Namespace\"\n}\n");

            var configuration = Xtraq.Configuration.XtraqConfiguration.Load(projectRoot, requireGeneratorConnection: false);

            Xunit.Assert.Null(configuration.GeneratorConnectionString);
            Xunit.Assert.Equal("Offline.Namespace", configuration.NamespaceRoot);
            Xunit.Assert.Equal(projectRoot, configuration.ProjectRoot);
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
