using System.Text.Json;
using Xtraq.Data.Models;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.SnapshotBuilder.Writers;

namespace Xtraq.Tests;

/// <summary>
/// Validates that procedure snapshots retain both the exposed alias and the raw source column metadata.
/// </summary>
public sealed class ProcedureSnapshotDocumentBuilderTests
{
    [Xunit.Fact]
    public void BuildProcedureJson_PersistsSourceColumnAlongsideAlias()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "AliasProbe" };
        var procedure = new ProcedureModel();
        var resultSet = new ProcedureResultSet();
        var column = new ProcedureResultColumn
        {
            Name = "DisplayNameLabel",
            Alias = "DisplayNameLabel",
            SourceSchema = "dbo",
            SourceTable = "Users",
            SourceColumn = "DisplayName",
            SqlTypeName = "nvarchar",
            MaxLength = 200
        };

        resultSet.Columns.Add(column);
        procedure.ResultSets.Add(resultSet);

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            System.Array.Empty<StoredProcedureInput>(),
            procedure,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var document = JsonDocument.Parse(payload);
        var persistedColumn = document.RootElement
            .GetProperty("ResultSets")[0]
            .GetProperty("Columns")[0];

        Xunit.Assert.Equal("DisplayNameLabel", persistedColumn.GetProperty("Name").GetString());
        Xunit.Assert.Equal("DisplayNameLabel", persistedColumn.GetProperty("Alias").GetString());
        Xunit.Assert.Equal("DisplayName", persistedColumn.GetProperty("SourceColumn").GetString());
    }

    [Xunit.Fact]
    public void BuildProcedureJson_PersistsForcedNullableFlag()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "ForcedNullableProbe" };
        var procedure = new ProcedureModel();
        var resultSet = new ProcedureResultSet();
        var column = new ProcedureResultColumn
        {
            Name = "PreferredEmail",
            SqlTypeName = "nvarchar(320)",
            IsNullable = true,
            ForcedNullable = true
        };

        resultSet.Columns.Add(column);
        procedure.ResultSets.Add(resultSet);

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            System.Array.Empty<StoredProcedureInput>(),
            procedure,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var document = JsonDocument.Parse(payload);
        var persistedColumn = document.RootElement
            .GetProperty("ResultSets")[0]
            .GetProperty("Columns")[0];

        Xunit.Assert.True(persistedColumn.GetProperty("IsNullable").GetBoolean());
        Xunit.Assert.True(persistedColumn.GetProperty("ForcedNullable").GetBoolean());
    }

    [Xunit.Fact]
    public void BuildProcedureJson_DoesNotFlipIsNullableWhenOnlyForcedNullable()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "ForcedNullableProbe" };
        var procedure = new ProcedureModel();
        var resultSet = new ProcedureResultSet();
        var column = new ProcedureResultColumn
        {
            Name = "PreferredEmail",
            SqlTypeName = "nvarchar(320)",
            IsNullable = false,
            ForcedNullable = true
        };

        resultSet.Columns.Add(column);
        procedure.ResultSets.Add(resultSet);

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            System.Array.Empty<StoredProcedureInput>(),
            procedure,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var document = JsonDocument.Parse(payload);
        var persistedColumn = document.RootElement
            .GetProperty("ResultSets")[0]
            .GetProperty("Columns")[0];

        Xunit.Assert.False(persistedColumn.TryGetProperty("IsNullable", out _));
        Xunit.Assert.True(persistedColumn.GetProperty("ForcedNullable").GetBoolean());
    }
}
