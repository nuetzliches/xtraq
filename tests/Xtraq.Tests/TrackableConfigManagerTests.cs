using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Xtraq.Tests.Configuration;

/// <summary>
/// Covers behaviour of the TrackableConfigManager helper.
/// </summary>
public sealed class TrackableConfigManagerTests
{
    [Xunit.Fact]
    public void BuildEnvMap_WhenProvidedEnvLines_FiltersToXtraqKeys()
    {
        var lines = new[]
        {
            "XTRAQ_NAMESPACE=Acme.Core",
            "PATH=/usr/bin",
            "XTRAQ_OUTPUT_DIR=Artifacts",
            "  # comment",
            "XTRAQ_BUILD_SCHEMAS=core,identity"
        };

        var map = Xtraq.Configuration.TrackableConfigManager.BuildEnvMap(lines);

        Xunit.Assert.Equal("Acme.Core", map["XTRAQ_NAMESPACE"]);
        Xunit.Assert.Equal("Artifacts", map["XTRAQ_OUTPUT_DIR"]);
        Xunit.Assert.Equal("core,identity", map["XTRAQ_BUILD_SCHEMAS"]);
        Xunit.Assert.False(map.ContainsKey("PATH"));
    }

    [Xunit.Fact]
    public void Write_WhenEnvValuesProvided_WritesLegacyPayload()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-write-");
        try
        {
            var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["XTRAQ_NAMESPACE"] = "Acme.Product",
                ["XTRAQ_OUTPUT_DIR"] = "Artifacts",
                ["XTRAQ_BUILD_SCHEMAS"] = "core, identity; audit",
                ["XTRAQ_TARGET_FRAMEWORK"] = "net10.0"
            };

            Xtraq.Configuration.TrackableConfigManager.Write(directory.FullName, envValues);

            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            Xunit.Assert.True(File.Exists(configPath));

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Xunit.Assert.False(root.TryGetProperty("ProjectPath", out _));
            Xunit.Assert.Equal("Acme.Product", root.GetProperty("Namespace").GetString());
            Xunit.Assert.Equal("Artifacts", root.GetProperty("OutputDir").GetString());
            Xunit.Assert.Equal("net10.0", root.GetProperty("TargetFramework").GetString());
            var schemas = root.GetProperty("BuildSchemas")
                .EnumerateArray()
                .Select(static item => item.GetString())
                .ToArray();
            Xunit.Assert.Equal(new[] { "core", "identity", "audit" }, schemas);
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }

    [Xunit.Fact]
    public void Write_WhenRedirectPresent_DoesNotOverwrite()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-redirect-");
        try
        {
            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            File.WriteAllText(configPath, "{\n  \"ProjectPath\": \"..\\\\target\"\n}\n");

            var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["XTRAQ_NAMESPACE"] = "Should.Not.Apply"
            };

            Xtraq.Configuration.TrackableConfigManager.Write(directory.FullName, envValues);

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Xunit.Assert.True(root.TryGetProperty("ProjectPath", out var projectPathElement));
            Xunit.Assert.Equal("..\\target", projectPathElement.GetString());
            Xunit.Assert.False(root.TryGetProperty("Namespace", out _));
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }
}
