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
    private const string SchemaUrl = "https://nuetzliches.github.io/xtraq/xtraqconfig.schema.json";

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
    public void Write_WhenEnvValuesProvided_WritesOnlyNonDefaultValues()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-write-");
        try
        {
            var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["XTRAQ_NAMESPACE"] = "Acme.Product",
                ["XTRAQ_OUTPUT_DIR"] = "Artifacts",
                ["XTRAQ_BUILD_SCHEMAS"] = "core, identity; audit",
                ["XTRAQ_TARGET_FRAMEWORK"] = "net10.0",
                ["XTRAQ_API_MODE"] = "Minimal",
                ["XTRAQ_API_AUTOBIND"] = "@UserId INT",
                ["XTRAQ_API_AUTOBIND_PROCEDURES"] = "sample.UserCompositeJsonSnapshot",
                ["XTRAQ_ENTITY_FRAMEWORK_ENABLED"] = "1",
                ["XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES"] = "1"
            };

            Xtraq.Configuration.TrackableConfigManager.Write(directory.FullName, envValues);

            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            Xunit.Assert.True(File.Exists(configPath));

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Xunit.Assert.Equal(SchemaUrl, root.GetProperty("$schema").GetString());
            Xunit.Assert.False(root.TryGetProperty("ProjectPath", out _));
            Xunit.Assert.Equal("Acme.Product", root.GetProperty("Namespace").GetString());
            Xunit.Assert.Equal("Artifacts", root.GetProperty("OutputDir").GetString());
            Xunit.Assert.False(root.TryGetProperty("TargetFramework", out _));
            var schemas = root.GetProperty("BuildSchemas")
                .EnumerateArray()
                .Select(static item => item.GetString())
                .ToArray();
            Xunit.Assert.Equal(new[] { "core", "identity", "audit" }, schemas);
            var apiElement = root.GetProperty("Api");
            Xunit.Assert.Equal("Minimal", apiElement.GetProperty("Mode").GetString());
            var requests = apiElement.GetProperty("Requests");
            Xunit.Assert.Equal("@UserId INT", requests.GetProperty("AutoBind").EnumerateArray().First().GetString());
            Xunit.Assert.Equal("sample.UserCompositeJsonSnapshot", requests.GetProperty("AutoBindProcedures").EnumerateArray().First().GetString());
            var efElement = root.GetProperty("EntityFramework");
            Xunit.Assert.True(efElement.GetProperty("Enabled").GetBoolean());
            var resultSet = root.GetProperty("ResultSet");
            Xunit.Assert.True(resultSet.GetProperty("Json").GetProperty("IncludeNullValues").GetBoolean());
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }

    [Xunit.Fact]
    public void Write_WhenNonDefaultTargetFrameworkProvided_PersistsOverride()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-tf-");
        try
        {
            var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["XTRAQ_TARGET_FRAMEWORK"] = "net8.0"
            };

            Xtraq.Configuration.TrackableConfigManager.Write(directory.FullName, envValues);

            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            Xunit.Assert.True(File.Exists(configPath));

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Xunit.Assert.Equal(SchemaUrl, root.GetProperty("$schema").GetString());
            Xunit.Assert.True(root.TryGetProperty("TargetFramework", out var framework));
            Xunit.Assert.Equal("net8.0", framework.GetString());
            Xunit.Assert.False(root.TryGetProperty("OutputDir", out _));
            Xunit.Assert.False(root.TryGetProperty("Namespace", out _));
            Xunit.Assert.False(root.TryGetProperty("BuildSchemas", out _));
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }

    [Xunit.Fact]
    public void Write_WhenEnvIsDefault_WritesSchemaOnly()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-default-");
        try
        {
            var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["XTRAQ_GENERATOR_DB"] = "Server=(local);Database=App;"
            };

            Xtraq.Configuration.TrackableConfigManager.Write(directory.FullName, envValues);

            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            Xunit.Assert.True(File.Exists(configPath));

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Xunit.Assert.Equal(JsonValueKind.Object, root.ValueKind);
            var properties = root.EnumerateObject().ToArray();
            var schemaProperty = Xunit.Assert.Single(properties);
            Xunit.Assert.Equal("$schema", schemaProperty.Name);
            Xunit.Assert.Equal(SchemaUrl, schemaProperty.Value.GetString());
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

    [Xunit.Fact]
    public void ReadDefaults_WhenMinimalApiSet_ExposesEnvironmentFlag()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-config-defaults-");
        try
        {
            var configPath = Path.Combine(directory.FullName, ".xtraqconfig");
            File.WriteAllText(configPath, "{\n  \"Api\": { \"Mode\": \"Minimal\", \"Requests\": { \"AutoBind\": [\"@UserId INT\"], \"AutoBindProcedures\": [\"sample.UserCompositeJsonSnapshot\"] } },\n  \"EntityFramework\": { \"Enabled\": true },\n  \"ResultSet\": { \"Json\": { \"IncludeNullValues\": true } }\n}\n");

            var defaults = Xtraq.Configuration.TrackableConfigManager.ReadDefaults(directory.FullName);

            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_MODE", out var flag));
            Xunit.Assert.Equal("Minimal", flag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_AUTOBIND", out var autoBindFlag));
            Xunit.Assert.Equal("@UserId INT", autoBindFlag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_AUTOBIND_PROCEDURES", out var autoBindProcFlag));
            Xunit.Assert.Equal("sample.UserCompositeJsonSnapshot", autoBindProcFlag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_ENTITY_FRAMEWORK_ENABLED", out var efFlag));
            Xunit.Assert.Equal("1", efFlag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES", out var jsonFlag));
            Xunit.Assert.Equal("1", jsonFlag);
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }
}
