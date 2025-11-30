using System;
using System.IO;
using System.Text.Json;
using Xtraq.Metadata;
using Xunit;

namespace Xtraq.Tests;

public class UserDefinedTypeResolutionTests
{
    [Fact]
    public void UserFind_snapshot_uses_int_for_shared_pk_type()
    {
        const string json = """
                {
                    "Parameters": [
                        {
                            "Name": "@UserId",
                            "TypeRef": "sys.int",
                            "UserTypeRef": "shared.pkInt"
                        }
                    ]
                }
                """;

        using var doc = JsonDocument.Parse(json);
        var param = doc.RootElement.GetProperty("Parameters")[0];

        Assert.Equal("sys.int", param.GetProperty("TypeRef").GetString());
        Assert.Equal("shared.pkInt", param.GetProperty("UserTypeRef").GetString());
        Assert.False(param.TryGetProperty("IsNullable", out _));
    }

    [Fact]
    public void Resolver_maps_shared_pkint_to_int()
    {
        var directory = Directory.CreateTempSubdirectory("xtraq-type-resolver-");
        try
        {
            var typesDir = Path.Combine(directory.FullName, ".xtraq", "snapshots", "types");
            Directory.CreateDirectory(typesDir);
            var typePath = Path.Combine(typesDir, "shared.pkInt.json");
            File.WriteAllText(typePath, """
            {
              "Schema": "shared",
              "Name": "pkInt",
              "BaseSqlTypeName": "int",
              "IsNullable": false
            }
            """);

            var resolver = new TypeMetadataResolver(directory.FullName);
            var resolved = resolver.Resolve("shared.pkInt", null, null, null);

            Assert.True(resolved.HasValue);
            Assert.Equal("int", resolved.Value.SqlType);
            Assert.False(resolved.Value.IsNullable.GetValueOrDefault());
        }
        finally
        {
            Directory.Delete(directory.FullName, true);
        }
    }
}
