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
            "XTRAQ_GENERATOR_DB=Server=(local);Database=App;",
            "XTRAQ_LOG_LEVEL=debug",
            "  # comment",
            "XTRAQ_ALIAS_DEBUG=1"
        };

        var map = Xtraq.Configuration.TrackableConfigManager.BuildEnvMap(lines);

        Xunit.Assert.Equal("Server=(local);Database=App;", map["XTRAQ_GENERATOR_DB"]);
        Xunit.Assert.Equal("debug", map["XTRAQ_LOG_LEVEL"]);
        Xunit.Assert.Equal("1", map["XTRAQ_ALIAS_DEBUG"]);
        Xunit.Assert.False(map.ContainsKey("XTRAQ_NAMESPACE"));
        Xunit.Assert.False(map.ContainsKey("XTRAQ_OUTPUT_DIR"));
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
                ["XTRAQ_ENTITY_FRAMEWORK_ENABLED"] = "1"
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
            Xunit.Assert.False(root.TryGetProperty("ResultSet", out _));
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
            var expectedNamespace = ToPascalCase(new DirectoryInfo(directory.FullName).Name);
            Xunit.Assert.Equal(expectedNamespace, root.GetProperty("Namespace").GetString());
            Xunit.Assert.True(root.TryGetProperty("TargetFramework", out var framework));
            Xunit.Assert.Equal("net8.0", framework.GetString());
            Xunit.Assert.False(root.TryGetProperty("OutputDir", out _));
            Xunit.Assert.False(root.TryGetProperty("BuildSchemas", out _));
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }

    [Xunit.Fact]
    public void Write_WhenEnvIsDefault_WritesSchemaAndNamespaceOnly()
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
            Xunit.Assert.Equal(2, properties.Length);
            var schemaProperty = properties.First(p => p.Name == "$schema");
            Xunit.Assert.Equal(SchemaUrl, schemaProperty.Value.GetString());
            var namespaceProperty = properties.First(p => p.Name == "Namespace");
            var expectedNamespace = ToPascalCase(new DirectoryInfo(directory.FullName).Name);
            Xunit.Assert.Equal(expectedNamespace, namespaceProperty.Value.GetString());
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }

    private static string ToPascalCase(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return "Xtraq";
        }

        var parts = input
            .Split(new[] { '-', '_', ' ', '.', '/', '\\' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(static segment => segment.Trim())
            .Where(static segment => segment.Length > 0)
            .Select(static segment => char.ToUpperInvariant(segment[0]) + (segment.Length > 1 ? segment[1..].ToLowerInvariant() : string.Empty));

        var candidate = string.Concat(parts);
        candidate = new string(candidate.Where(static ch => char.IsLetterOrDigit(ch) || ch == '_').ToArray());
        if (string.IsNullOrEmpty(candidate))
        {
            candidate = "Xtraq";
        }

        if (char.IsDigit(candidate[0]))
        {
            candidate = "N" + candidate;
        }

        return candidate;
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
            File.WriteAllText(configPath, "{\n  \"Api\": { \"Mode\": \"Minimal\", \"Requests\": { \"AutoBind\": [\"@UserId INT\"], \"AutoBindProcedures\": [\"sample.UserCompositeJsonSnapshot\"] } },\n  \"EntityFramework\": { \"Enabled\": true }\n}\n");

            var defaults = Xtraq.Configuration.TrackableConfigManager.ReadDefaults(directory.FullName);

            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_MODE", out var flag));
            Xunit.Assert.Equal("Minimal", flag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_AUTOBIND", out var autoBindFlag));
            Xunit.Assert.Equal("@UserId INT", autoBindFlag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_API_AUTOBIND_PROCEDURES", out var autoBindProcFlag));
            Xunit.Assert.Equal("sample.UserCompositeJsonSnapshot", autoBindProcFlag);
            Xunit.Assert.True(defaults.TryGetValue("XTRAQ_ENTITY_FRAMEWORK_ENABLED", out var efFlag));
            Xunit.Assert.Equal("1", efFlag);
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }
}
