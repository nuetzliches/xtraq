using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Schema;
using Xtraq.SnapshotBuilder.Analyzers;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProcedureModelScriptDomBuilderApplyTests
{
    [Fact]
    public void Build_WithOuterApply_ForcedNullableColumns()
    {
        lock (SnapshotTestLock.Gate)
        {
            var provider = new StubSchemaMetadataProvider();
            provider.RegisterFunction(
                "sample",
                "fnPreferredContact",
                new ColumnMetadata
                {
                    Name = "Email",
                    SqlTypeName = "nvarchar(320)",
                    MaxLength = 320,
                    IsNullable = false
                },
                new ColumnMetadata
                {
                    Name = "DisplayName",
                    SqlTypeName = "nvarchar(200)",
                    MaxLength = 200,
                    IsNullable = false
                });

            var sql = @"CREATE OR ALTER PROCEDURE sample.UserContactApply
    @UserId INT
AS
BEGIN
    SELECT pc.Email AS PreferredEmail, pc.DisplayName AS PreferredDisplayName
    FROM sample.Users AS u
    OUTER APPLY sample.fnPreferredContact(@UserId) AS pc;
END;";

            var builder = new ProcedureModelScriptDomBuilder(provider);
            var request = new ProcedureAstBuildRequest(sql, "sample", null, VerboseParsing: false);
            var model = builder.Build(request);

            Assert.NotNull(model);
            var resultSet = Assert.Single(model!.ResultSets);
            Assert.Equal(2, resultSet.Columns.Count);

            var emailColumn = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "PreferredEmail", StringComparison.Ordinal));
            Assert.False(emailColumn.IsNullable ?? true);
            Assert.True(emailColumn.ForcedNullable ?? false);

            var displayColumn = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "PreferredDisplayName", StringComparison.Ordinal));
            Assert.False(displayColumn.IsNullable ?? true);
            Assert.True(displayColumn.ForcedNullable ?? false);
        }
    }

    private sealed class StubSchemaMetadataProvider : IEnhancedSchemaMetadataProvider
    {
        private readonly Dictionary<string, IReadOnlyList<ColumnMetadata>> _functionColumns = new(StringComparer.OrdinalIgnoreCase);

        public void RegisterFunction(string schema, string name, params ColumnMetadata[] columns)
        {
            _functionColumns[BuildKey(schema, name)] = columns;
        }

        public Task<ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            if (_functionColumns.TryGetValue(BuildKey(schema, tableName), out var columns))
            {
                var match = columns.FirstOrDefault(column => string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase));
                return Task.FromResult(match);
            }

            return Task.FromResult<ColumnMetadata?>(null);
        }

        public Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            if (_functionColumns.TryGetValue(BuildKey(schema, tableName), out var columns))
            {
                return Task.FromResult(columns);
            }

            return Task.FromResult<IReadOnlyList<ColumnMetadata>>(Array.Empty<ColumnMetadata>());
        }

        public Task<FunctionReturnMetadata?> ResolveFunctionReturnAsync(string schema, string functionName, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<FunctionReturnMetadata?>(null);
        }

        public Task<bool> IsOfflineModeAvailableAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        private static string BuildKey(string schema, string name)
        {
            return string.Concat(schema?.Trim() ?? string.Empty, ".", name?.Trim() ?? string.Empty);
        }
    }
}
