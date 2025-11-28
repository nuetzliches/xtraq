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
        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "samples", "restapi", ".xtraq", "snapshots", "procedures", "sample.UserFind.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var param = doc.RootElement.GetProperty("Parameters")[0];

        Assert.Equal("sys.int", param.GetProperty("TypeRef").GetString());
        Assert.Equal("shared.pkInt", param.GetProperty("UserTypeRef").GetString());
        Assert.False(param.TryGetProperty("IsNullable", out _));
    }

    [Fact]
    public void Resolver_maps_shared_pkint_to_int()
    {
        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        var resolver = new TypeMetadataResolver(Path.Combine(repoRoot, "samples", "restapi"));
        var resolved = resolver.Resolve("shared.pkInt", null, null, null);

        Assert.True(resolved.HasValue);
        Assert.Equal("int", resolved.Value.SqlType);
        Assert.False(resolved.Value.IsNullable.GetValueOrDefault());
    }
}
