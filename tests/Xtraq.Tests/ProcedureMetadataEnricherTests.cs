namespace Xtraq.Tests;

/// <summary>
/// Verifies the metadata enrichment stage can run offline using in-memory providers.
/// </summary>
public sealed class ProcedureMetadataEnricherTests
{
    /// <summary>
    /// Ensures column metadata and JSON flags are enriched when providers supply offline data.
    /// </summary>
    [Xunit.Fact]
    public async System.Threading.Tasks.Task EnrichAsync_PopulatesColumnMetadata_FromOfflineProviders()
    {
        var console = new TestConsoleService();
        var functionProvider = new FakeFunctionJsonMetadataProvider();
        var schemaProvider = new FakeEnhancedSchemaMetadataProvider();

        var stringColumnMetadata = new Xtraq.Schema.ColumnMetadata
        {
            Name = "DisplayName",
            SqlTypeName = "nvarchar",
            IsNullable = true,
            MaxLength = 200
        };

        var idColumnMetadata = new Xtraq.Schema.ColumnMetadata
        {
            Name = "Id",
            SqlTypeName = "int",
            IsNullable = false
        };

        schemaProvider.AddColumn("dbo", "Users", "DisplayName", stringColumnMetadata);
        schemaProvider.AddColumn("dbo", "Users", "Id", idColumnMetadata);
        functionProvider.Register("meta", "FetchCreatedUser", returnsJson: true, returnsJsonArray: false, rootProperty: "user");

        var procedure = new Xtraq.SnapshotBuilder.Models.ProcedureModel();
        var resultSet = new Xtraq.SnapshotBuilder.Models.ProcedureResultSet();
        var jsonColumn = new Xtraq.SnapshotBuilder.Models.ProcedureResultColumn
        {
            Name = "created.user.displayName",
            SourceSchema = "dbo",
            SourceTable = "Users",
            SourceColumn = "DisplayName",
            Reference = new Xtraq.SnapshotBuilder.Models.ProcedureReference
            {
                Kind = Xtraq.SnapshotBuilder.Models.ProcedureReferenceKind.Function,
                Schema = "meta",
                Name = "FetchCreatedUser"
            }
        };

        var scalarColumn = new Xtraq.SnapshotBuilder.Models.ProcedureResultColumn
        {
            Name = "created.user.userId",
            SourceSchema = "dbo",
            SourceTable = "Users",
            SourceColumn = "Id"
        };

        resultSet.Columns.Add(jsonColumn);
        resultSet.Columns.Add(scalarColumn);
        procedure.ResultSets.Add(resultSet);

        var enricher = new Xtraq.SnapshotBuilder.Metadata.ProcedureMetadataEnricher(console, functionProvider, schemaProvider);
        var descriptor = new Xtraq.SnapshotBuilder.Models.ProcedureDescriptor { Schema = "identity", Name = "RecordAsJson" };
    var request = new Xtraq.SnapshotBuilder.Analyzers.ProcedureMetadataEnrichmentRequest(descriptor, procedure, SnapshotFile: null);

        await enricher.EnrichAsync(request, System.Threading.CancellationToken.None);

        Xunit.Assert.True(jsonColumn.ReturnsJson);
        Xunit.Assert.True(jsonColumn.IsNestedJson);
        Xunit.Assert.False(jsonColumn.ReturnsJsonArray);
        Xunit.Assert.Equal("user", jsonColumn.JsonRootProperty);

        Xunit.Assert.Equal("int", scalarColumn.SqlTypeName);
        Xunit.Assert.False(scalarColumn.IsNullable);
    }

    private sealed class FakeFunctionJsonMetadataProvider : Xtraq.SnapshotBuilder.Metadata.IFunctionJsonMetadataProvider
    {
        private readonly System.Collections.Generic.Dictionary<string, Xtraq.SnapshotBuilder.Metadata.FunctionJsonMetadata> _entries = new(System.StringComparer.OrdinalIgnoreCase);

        public void Register(string? schema, string name, bool returnsJson, bool returnsJsonArray, string? rootProperty)
        {
            var key = BuildKey(schema, name);
            _entries[key] = new Xtraq.SnapshotBuilder.Metadata.FunctionJsonMetadata(returnsJson, returnsJsonArray, rootProperty);
        }

        public System.Threading.Tasks.Task<Xtraq.SnapshotBuilder.Metadata.FunctionJsonMetadata?> ResolveAsync(string? schema, string name, System.Threading.CancellationToken cancellationToken)
        {
            var key = BuildKey(schema, name);
            _entries.TryGetValue(key, out var metadata);
            return System.Threading.Tasks.Task.FromResult(metadata);
        }

        private static string BuildKey(string? schema, string name)
        {
            var normalizedSchema = string.IsNullOrWhiteSpace(schema) ? string.Empty : schema;
            return string.Concat(normalizedSchema, "::", name);
        }
    }

    private sealed class FakeEnhancedSchemaMetadataProvider : Xtraq.Schema.IEnhancedSchemaMetadataProvider
    {
        private readonly System.Collections.Generic.Dictionary<string, Xtraq.Schema.ColumnMetadata> _columns = new(System.StringComparer.OrdinalIgnoreCase);
        private readonly System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<Xtraq.Schema.ColumnMetadata>> _tables = new(System.StringComparer.OrdinalIgnoreCase);
        private readonly System.Collections.Generic.Dictionary<string, Xtraq.Schema.FunctionReturnMetadata> _functions = new(System.StringComparer.OrdinalIgnoreCase);

        public System.Threading.Tasks.Task<Xtraq.Schema.ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, System.Threading.CancellationToken cancellationToken = default)
        {
            var key = BuildColumnKey(schema, tableName, columnName, catalog);
            _columns.TryGetValue(key, out var metadata);
            return System.Threading.Tasks.Task.FromResult(metadata);
        }

        public System.Threading.Tasks.Task<System.Collections.Generic.IReadOnlyList<Xtraq.Schema.ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, System.Threading.CancellationToken cancellationToken = default)
        {
            var tableKey = BuildTableKey(schema, tableName, catalog);
            if (_tables.TryGetValue(tableKey, out var list))
            {
                return System.Threading.Tasks.Task.FromResult<System.Collections.Generic.IReadOnlyList<Xtraq.Schema.ColumnMetadata>>(list);
            }

            return System.Threading.Tasks.Task.FromResult<System.Collections.Generic.IReadOnlyList<Xtraq.Schema.ColumnMetadata>>(System.Array.Empty<Xtraq.Schema.ColumnMetadata>());
        }

        public System.Threading.Tasks.Task<Xtraq.Schema.FunctionReturnMetadata?> ResolveFunctionReturnAsync(string schema, string functionName, System.Threading.CancellationToken cancellationToken = default)
        {
            var key = BuildFunctionKey(schema, functionName);
            _functions.TryGetValue(key, out var metadata);
            return System.Threading.Tasks.Task.FromResult(metadata);
        }

        public System.Threading.Tasks.Task<bool> IsOfflineModeAvailableAsync(System.Threading.CancellationToken cancellationToken = default)
        {
            return System.Threading.Tasks.Task.FromResult(true);
        }

        public void AddColumn(string schema, string table, string column, Xtraq.Schema.ColumnMetadata metadata, string? catalog = null)
        {
            var key = BuildColumnKey(schema, table, column, catalog);
            _columns[key] = metadata;

            var tableKey = BuildTableKey(schema, table, catalog);
            if (!_tables.TryGetValue(tableKey, out var list))
            {
                list = new System.Collections.Generic.List<Xtraq.Schema.ColumnMetadata>();
                _tables[tableKey] = list;
            }

            list.Add(metadata);
        }

        public void AddFunction(string schema, string name, Xtraq.Schema.FunctionReturnMetadata metadata)
        {
            var key = BuildFunctionKey(schema, name);
            _functions[key] = metadata;
        }

        private static string BuildColumnKey(string schema, string table, string column, string? catalog)
        {
            var normalizedCatalog = string.IsNullOrWhiteSpace(catalog) ? string.Empty : catalog;
            return string.Concat(normalizedCatalog, "::", schema, "::", table, "::", column);
        }

        private static string BuildTableKey(string schema, string table, string? catalog)
        {
            var normalizedCatalog = string.IsNullOrWhiteSpace(catalog) ? string.Empty : catalog;
            return string.Concat(normalizedCatalog, "::", schema, "::", table);
        }

        private static string BuildFunctionKey(string schema, string name)
        {
            return string.Concat(schema, "::", name);
        }
    }
}
