using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Schema;
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
        var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: true);
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

    [Fact]
    public void JsonQueryCorrelatedScalarSubquery_SetsSingleRowGuarantee()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE sample.JsonProjection
AS
BEGIN
    SELECT
        DebitorWorkflow = JSON_QUERY((
            SELECT s.StatusId
            FROM [testwf].[Status] AS s
            WHERE s.StatusId = j.Workflow_Mandate_StatusId
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
    FROM [testjrnl].[Journal] AS j;
END;";

        var provider = new StubEnhancedSchemaMetadataProvider(new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase)
        {
            ["testjrnl.Journal"] = new[]
            {
                CreateColumn("Workflow_Mandate_StatusId", isNullable: false)
            },
            ["testwf.Status"] = new[]
            {
                CreateColumn("StatusId", isNullable: false, isIdentity: true),
                CreateColumn("IsDeleted", isNullable: false)
            }
        });

        var builder = new ProcedureModelScriptDomBuilder(provider);
        var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: true);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model!.ResultSets);
        var column = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "DebitorWorkflow", StringComparison.OrdinalIgnoreCase));
        Assert.True(column.ReturnsJson, "Expected nested JSON metadata to be detected");
        Assert.False(column.ReturnsJsonArray ?? true, "WITHOUT_ARRAY_WRAPPER should force a scalar JSON shape");
        var nestedStatus = Assert.Single(column.Columns);
        Assert.Equal("Status", nestedStatus.SourceTable);
        Assert.Equal("StatusId", nestedStatus.SourceColumn);
        Assert.True(column.JsonSingleRowGuaranteed, "Expected correlated single-row JSON subquery to be guaranteed");
    }

    [Fact]
    public void JsonQueryCorrelatedScalarSubquery_WithAdditionalJoin_RemainsUnknown()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE sample.JsonProjection
AS
BEGIN
    SELECT
        DebitorWorkflow = JSON_QUERY((
            SELECT s.StatusId
            FROM [testwf].[Status] AS s
                INNER JOIN [testwf].[Node] AS n
                    ON n.StatusId = s.StatusId
            WHERE s.StatusId = j.Workflow_Mandate_StatusId
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
    FROM [testjrnl].[Journal] AS j;
END;";

        var provider = new StubEnhancedSchemaMetadataProvider(new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase)
        {
            ["testjrnl.Journal"] = new[]
            {
                CreateColumn("Workflow_Mandate_StatusId", isNullable: false)
            },
            ["testwf.Status"] = new[]
            {
                CreateColumn("StatusId", isNullable: false, isIdentity: true),
                CreateColumn("IsDeleted", isNullable: false)
            },
            ["testwf.Node"] = new[]
            {
                CreateColumn("StatusId", isNullable: false)
            }
        });

        var builder = new ProcedureModelScriptDomBuilder(provider);
        var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: true);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model!.ResultSets);
        var column = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "DebitorWorkflow", StringComparison.OrdinalIgnoreCase));
        Assert.True(column.ReturnsJson);
        Assert.False(column.ReturnsJsonArray ?? true);
        Assert.Null(column.JsonSingleRowGuaranteed);
    }

    [Fact]
    public void JsonQueryCorrelatedScalarSubquery_WithExtraFilter_RemainsUnknown()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE sample.JsonProjection
AS
BEGIN
    SELECT
        DebitorWorkflow = JSON_QUERY((
            SELECT s.StatusId
            FROM [testwf].[Status] AS s
                INNER JOIN [testwf].[Node] AS n
                    ON n.StatusId = s.StatusId
            WHERE s.StatusId = j.Workflow_Mandate_StatusId
                AND s.IsDeleted = 0
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
    FROM [testjrnl].[Journal] AS j;
END;";

        var provider = new StubEnhancedSchemaMetadataProvider(new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase)
        {
            ["testjrnl.Journal"] = new[]
            {
                CreateColumn("Workflow_Mandate_StatusId", isNullable: false)
            },
            ["testwf.Status"] = new[]
            {
                CreateColumn("StatusId", isNullable: false, isIdentity: true),
                CreateColumn("IsDeleted", isNullable: false)
            },
            ["testwf.Node"] = new[]
            {
                CreateColumn("StatusId", isNullable: false)
            }
        });

        var builder = new ProcedureModelScriptDomBuilder(provider);
        var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: false);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model!.ResultSets);
        var column = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "DebitorWorkflow", StringComparison.OrdinalIgnoreCase));
        Assert.True(column.ReturnsJson, "Expected nested JSON metadata to be detected");
        Assert.False(column.ReturnsJsonArray ?? true, "WITHOUT_ARRAY_WRAPPER should force a scalar JSON shape");
        Assert.Null(column.JsonSingleRowGuaranteed);
    }

    [Fact]
    public void JournalListDebitorWorkflow_RemainsNullableDueToAdditionalJoin()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE elastic.JournalList
AS
BEGIN
    SELECT
        DebitorWorkflow = JSON_QUERY((
            SELECT [Status] = JSON_QUERY((
                SELECT w_m_s.StatusId,
                    w_m_n.DisplayName
                FROM [workflow].[Status] AS w_m_s
                    INNER JOIN [workflow].[Node] AS w_m_n
                        ON w_m_n.StatusId = w_m_s.StatusId
                WHERE w_m_s.StatusId = j.Workflow_Mandate_StatusId
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ))
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
    FROM [journal].[Journal] AS j;
END;";

        var provider = new StubEnhancedSchemaMetadataProvider(new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase)
        {
            ["journal.Journal"] = new[]
            {
                CreateColumn("Workflow_Mandate_StatusId", isNullable: false)
            },
            ["workflow.Status"] = new[]
            {
                CreateColumn("StatusId", isNullable: false, isIdentity: true)
            },
            ["workflow.Node"] = new[]
            {
                CreateColumn("StatusId", isNullable: false)
            }
        });

        var builder = new ProcedureModelScriptDomBuilder(provider);
        var request = new ProcedureAstBuildRequest(procedure, "elastic", null, VerboseParsing: false);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model!.ResultSets);
        var column = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "DebitorWorkflow", StringComparison.OrdinalIgnoreCase));
        Assert.True(column.ReturnsJson);
        Assert.Null(column.JsonSingleRowGuaranteed);
        Assert.True(column.IsNullable == true, "DebitorWorkflow should remain nullable without single-row guarantees");
        var nestedEnvelope = Assert.Single(column.Columns);
        Assert.True((nestedEnvelope.Columns?.Count ?? 0) > 0);
    }

    [Fact]
    public void JournalListDebitorWorkflow_SingleRowGuaranteeWithoutAdditionalJoin()
    {
        const string procedure = @"CREATE OR ALTER PROCEDURE elastic.JournalList
AS
BEGIN
    SELECT
        DebitorWorkflow = JSON_QUERY((
            SELECT [Status] = JSON_QUERY((
                SELECT w_m_s.StatusId,
                    w_m_s.DisplayName
                FROM [workflow].[Status] AS w_m_s
                WHERE w_m_s.StatusId = j.Workflow_Mandate_StatusId
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ))
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
    FROM [journal].[Journal] AS j;
END;";

        var provider = new StubEnhancedSchemaMetadataProvider(new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase)
        {
            ["journal.Journal"] = new[]
            {
                CreateColumn("Workflow_Mandate_StatusId", isNullable: false)
            },
            ["workflow.Status"] = new[]
            {
                CreateColumn("StatusId", isNullable: false, isIdentity: true),
                CreateColumn("DisplayName", isNullable: true)
            }
        });

        var builder = new ProcedureModelScriptDomBuilder(provider);
        var request = new ProcedureAstBuildRequest(procedure, "elastic", null, VerboseParsing: false);
        var model = builder.Build(request);

        Assert.NotNull(model);
        var resultSet = Assert.Single(model!.ResultSets);
        var column = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "DebitorWorkflow", StringComparison.OrdinalIgnoreCase));
        Assert.True(column.ReturnsJson);
        Assert.False(column.ReturnsJsonArray ?? true);
        Assert.True(column.JsonSingleRowGuaranteed);
        Assert.False(column.IsNullable ?? true);
    }

    private static ColumnMetadata CreateColumn(string name, bool isNullable, bool? isIdentity = null)
    {
        return new ColumnMetadata
        {
            Name = name,
            SqlTypeName = "int",
            IsNullable = isNullable,
            IsIdentity = isIdentity
        };
    }

        private sealed class StubEnhancedSchemaMetadataProvider : IEnhancedSchemaMetadataProvider
        {
            private readonly Dictionary<string, IReadOnlyList<ColumnMetadata>> _tables;

            public StubEnhancedSchemaMetadataProvider(Dictionary<string, IReadOnlyList<ColumnMetadata>> tables)
            {
                _tables = tables != null
                    ? new Dictionary<string, IReadOnlyList<ColumnMetadata>>(tables, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.OrdinalIgnoreCase);
            }

        public Task<ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            var columns = GetTableColumns(schema, tableName);
            var match = columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
            return Task.FromResult(match);
        }

        public Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            var columns = GetTableColumns(schema, tableName);
            return Task.FromResult(columns);
        }

        public Task<FunctionReturnMetadata?> ResolveFunctionReturnAsync(string schema, string functionName, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<FunctionReturnMetadata?>(null);
        }

        public Task<bool> IsOfflineModeAvailableAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        private IReadOnlyList<ColumnMetadata> GetTableColumns(string? schema, string tableName)
        {
            var key = BuildKey(schema, tableName);
            if (_tables.TryGetValue(key, out var columns))
            {
                return columns;
            }

            return Array.Empty<ColumnMetadata>();
        }

        private static string BuildKey(string? schema, string tableName)
        {
            var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
            return string.Concat(effectiveSchema, ".", tableName?.Trim());
        }
    }
}
