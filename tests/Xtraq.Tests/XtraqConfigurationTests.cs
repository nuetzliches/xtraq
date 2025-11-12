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
