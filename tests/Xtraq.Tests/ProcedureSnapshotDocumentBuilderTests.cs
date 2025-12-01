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
    public void BuildProcedureJson_MarksIsNullableWhenOnlyForcedNullable()
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

        Xunit.Assert.True(persistedColumn.GetProperty("IsNullable").GetBoolean());
        Xunit.Assert.True(persistedColumn.GetProperty("ForcedNullable").GetBoolean());
    }

    [Xunit.Fact]
    public void BuildProcedureJson_SkipsIsNullableForNestedForcedOnlyColumns()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "NestedForcedProbe" };
        var procedure = new ProcedureModel();
        var resultSet = new ProcedureResultSet();

        var nestedColumn = new ProcedureResultColumn
        {
            Name = "InnerValue",
            SqlTypeName = "int",
            ForcedNullable = true,
            IsNullable = false
        };

        var container = new ProcedureResultColumn
        {
            Name = "Envelope",
            ReturnsJson = true
        };
        container.Columns.Add(nestedColumn);

        resultSet.Columns.Add(container);
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
        var persistedChild = document.RootElement
            .GetProperty("ResultSets")[0]
            .GetProperty("Columns")[0]
            .GetProperty("Columns")[0];

        Xunit.Assert.True(persistedChild.GetProperty("ForcedNullable").GetBoolean());
        Xunit.Assert.False(persistedChild.TryGetProperty("IsNullable", out _));
    }

    [Xunit.Fact]
    public void BuildProcedureJson_MarksDefaultedScalarParameterAsNullable()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "DefaultsProbe" };
        var parameters = new[]
        {
            new StoredProcedureInput
            {
                Name = "@RecentPaymentCount",
                SqlTypeName = "int",
                IsNullable = false,
                HasDefaultValue = true
            }
        };

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            parameters,
            procedure: null,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var document = JsonDocument.Parse(payload);
        var param = document.RootElement.GetProperty("Parameters")[0];

        Xunit.Assert.True(param.GetProperty("IsNullable").GetBoolean());
        Xunit.Assert.True(param.GetProperty("HasDefaultValue").GetBoolean());
    }

    [Xunit.Fact]
    public void BuildProcedureJson_PreservesNullableMetadataEvenWithNonNullableUserType()
    {
        var descriptor = new ProcedureDescriptor { Schema = "sample", Name = "NullableProbe" };
        var parameters = new[]
        {
            new StoredProcedureInput
            {
                Name = "@RecentPaymentCount",
                SqlTypeName = "int",
                IsNullable = true,
                UserTypeName = "_id",
                UserTypeSchemaName = "core",
                UserTypeIsNullable = null // SQL metadata says nullable, but UDT default might be non-nullable
            }
        };

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            parameters,
            procedure: null,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var document = JsonDocument.Parse(payload);
        var param = document.RootElement.GetProperty("Parameters")[0];

        Xunit.Assert.True(param.GetProperty("IsNullable").GetBoolean());
    }
}
