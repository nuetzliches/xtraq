using System;
using Xtraq.SnapshotBuilder.Analyzers;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProcedureModelScriptDomBuilderJsonQueryTests
{
    [Fact]
    public void JsonQueryScalarSubquery_ProducesStructuredColumns()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE sample.JsonProjection
AS
BEGIN
    SELECT
        Payload = JSON_QUERY((
            SELECT
                CAST(42 AS int) AS TypeId,
                CAST(N'CODE' AS nvarchar(32)) AS Code
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )),
        Collection = JSON_QUERY((
            SELECT
                CAST(1 AS int) AS ItemId,
                CAST(N'VALUE' AS nvarchar(16)) AS DisplayName
            FOR JSON PATH
        ))
    FOR JSON PATH;
END;";

        var builder = new ProcedureModelScriptDomBuilder();
        var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: false);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model.ResultSets);
        Assert.Equal(2, resultSet.Columns.Count);

        var payload = Assert.Single(resultSet.Columns, c => c.Name == "Payload");
        Assert.True(payload.ReturnsJson);
        Assert.False(payload.ReturnsJsonArray ?? true);
        Assert.False(payload.ReturnsUnknownJson ?? true);
        Assert.Equal(2, payload.Columns.Count);
        var typeId = Assert.Single(payload.Columns, c => c.Name == "TypeId");
        Assert.Equal("int", typeId.SqlTypeName);
        var code = Assert.Single(payload.Columns, c => c.Name == "Code");
        Assert.NotNull(code.SqlTypeName);
        Assert.StartsWith("nvarchar", code.SqlTypeName!, StringComparison.OrdinalIgnoreCase);

        var collection = Assert.Single(resultSet.Columns, c => c.Name == "Collection");
        Assert.True(collection.ReturnsJson);
        Assert.True(collection.ReturnsJsonArray ?? false);
        Assert.False(collection.ReturnsUnknownJson ?? true);
        Assert.Equal(2, collection.Columns.Count);
        var itemId = Assert.Single(collection.Columns, c => c.Name == "ItemId");
        Assert.Equal("int", itemId.SqlTypeName);
        var displayName = Assert.Single(collection.Columns, c => c.Name == "DisplayName");
        Assert.NotNull(displayName.SqlTypeName);
        Assert.StartsWith("nvarchar", displayName.SqlTypeName!, StringComparison.OrdinalIgnoreCase);
    }
}
