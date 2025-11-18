using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Schema;
using Xtraq.SnapshotBuilder.Analyzers;
using Xtraq.SnapshotBuilder.Models;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProcedureModelScriptDomBuilderFunctionFallbackTests
{
    [Fact]
    public void Build_WhenFunctionMetadataMissing_UsesProviderFallback()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "xtraq-function-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        var snapshot = Capture(new[] { "XTRAQ_PROJECT_ROOT", "XTRAQ_SNAPSHOT_ROOT" });
        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", tempRoot);
        Environment.SetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT", tempRoot);

        var provider = new StubSchemaMetadataProvider();
        provider.RegisterFunction(
            "sample",
            "fnUserContactFallback",
            new ColumnMetadata
            {
                Name = "PreferredContactEmail",
                SqlTypeName = "nvarchar(320)",
                IsNullable = true,
                MaxLength = 320
            },
            new ColumnMetadata
            {
                Name = "PreferredContactDisplayName",
                SqlTypeName = "nvarchar(200)",
                IsNullable = true,
                MaxLength = 200
            });

        try
        {
            var procedure = @"CREATE OR ALTER PROCEDURE sample.UserContactLookup
    @UserId INT
AS
BEGIN
    SELECT pc.PreferredContactEmail, pc.PreferredContactDisplayName
    FROM sample.fnUserContactFallback(@UserId) AS pc;
END;";

            var builder = new ProcedureModelScriptDomBuilder(provider);
            var request = new ProcedureAstBuildRequest(procedure, "sample", null, VerboseParsing: false);
            var model = builder.Build(request);

            Assert.NotNull(model);
            var resultSet = Assert.Single(model!.ResultSets);
            Assert.Equal(2, resultSet.Columns.Count);

            var email = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "PreferredContactEmail", StringComparison.Ordinal));
            Assert.Equal("nvarchar(320)", email.SqlTypeName);
            Assert.True(email.IsNullable ?? false);
            Assert.NotNull(email.Reference);
            Assert.Equal(ProcedureReferenceKind.Function, email.Reference!.Kind);
            Assert.Equal("sample", email.Reference.Schema);
            Assert.Equal("fnUserContactFallback", email.Reference.Name);

            var displayName = Assert.Single(resultSet.Columns, c => string.Equals(c.Name, "PreferredContactDisplayName", StringComparison.Ordinal));
            Assert.Equal("nvarchar(200)", displayName.SqlTypeName);
            Assert.True(displayName.IsNullable ?? false);
            Assert.NotNull(displayName.Reference);
            Assert.Equal(ProcedureReferenceKind.Function, displayName.Reference!.Kind);

            Assert.Contains(("sample", "fnUserContactFallback"), provider.FunctionRequests);
        }
        finally
        {
            Restore(snapshot);
            TryDeleteDirectory(tempRoot);
        }
    }

    private static Dictionary<string, string?> Capture(IEnumerable<string> keys)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in keys)
        {
            map[key] = Environment.GetEnvironmentVariable(key);
        }

        return map;
    }

    private static void Restore(Dictionary<string, string?> snapshot)
    {
        foreach (var pair in snapshot)
        {
            Environment.SetEnvironmentVariable(pair.Key, pair.Value);
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
        }
    }

    private sealed class StubSchemaMetadataProvider : IEnhancedSchemaMetadataProvider
    {
        private readonly Dictionary<string, IReadOnlyList<ColumnMetadata>> _functionColumns = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<(string Schema, string Name)> _functionRequests = new();

        public IReadOnlyList<(string Schema, string Name)> FunctionRequests => _functionRequests;

        public void RegisterFunction(string schema, string name, params ColumnMetadata[] columns)
        {
            _functionColumns[BuildKey(schema, name)] = columns;
        }

        public Task<ColumnMetadata?> ResolveTableColumnAsync(string schema, string tableName, string columnName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            var columns = _functionColumns.TryGetValue(BuildKey(schema, tableName), out var map)
                ? map
                : Array.Empty<ColumnMetadata>();

            var match = columns.FirstOrDefault(column => string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase));
            return Task.FromResult<ColumnMetadata?>(match);
        }

        public Task<IReadOnlyList<ColumnMetadata>> GetTableColumnsAsync(string schema, string tableName, string? catalog = null, CancellationToken cancellationToken = default)
        {
            _functionRequests.Add((schema, tableName));
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
