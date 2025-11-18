using Xtraq.SnapshotBuilder.Writers;
using Xtraq.Utils;

namespace Xtraq.Services;

/// <summary>
/// Provides access to result set metadata from the snapshot index for offline build scenarios.
/// Enables table/column mapping resolution without database connectivity.
/// </summary>
internal interface ISnapshotIndexMetadataProvider
{
    /// <summary>
    /// Gets result set metadata for a specific procedure from the snapshot index.
    /// </summary>
    /// <param name="schema">Procedure schema</param>
    /// <param name="name">Procedure name</param>
    /// <param name="cancellationToken">Cancellation token that cancels the lookup.</param>
    /// <returns>Result set metadata or null if not found</returns>
    Task<IReadOnlyList<IndexResultSetEntry>?> GetResultSetMetadataAsync(string schema, string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets column metadata for table/column mapping resolution.
    /// </summary>
    /// <param name="schema">Schema name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="columnName">Column name (optional, returns all columns if null)</param>
    /// <param name="cancellationToken">Cancellation token that cancels the lookup.</param>
    /// <returns>Column metadata or null if not found</returns>
    Task<IndexColumnEntry?> GetColumnMetadataAsync(string schema, string tableName, string? columnName = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all column metadata entries that reference the specified table within the snapshot index.
    /// </summary>
    /// <param name="schema">Schema name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="cancellationToken">Cancellation token that cancels the lookup.</param>
    /// <returns>List of matching column metadata entries</returns>
    Task<IReadOnlyList<IndexColumnEntry>> GetTableColumnsMetadataAsync(string schema, string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if the snapshot index is available for offline operations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token that cancels the availability check.</param>
    /// <returns>True if index is available and readable</returns>
    Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// File-based implementation that reads metadata from the snapshot index.json.
/// </summary>
internal sealed class SnapshotIndexMetadataProvider : ISnapshotIndexMetadataProvider
{
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private IndexDocument? _cachedIndex;
    private DateTime _lastIndexRead = DateTime.MinValue;
    private readonly TimeSpan _cacheValidityDuration = TimeSpan.FromMinutes(5);

    public async Task<IReadOnlyList<IndexResultSetEntry>?> GetResultSetMetadataAsync(string schema, string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var index = await LoadIndexAsync(cancellationToken).ConfigureAwait(false);
        if (index?.Procedures == null)
        {
            return null;
        }

        var procedure = index.Procedures.FirstOrDefault(p =>
            string.Equals(p.Schema, schema, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));

        return procedure?.ResultSets;
    }

    public async Task<IndexColumnEntry?> GetColumnMetadataAsync(string schema, string tableName, string? columnName = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(tableName))
        {
            return null;
        }

        var index = await LoadIndexAsync(cancellationToken).ConfigureAwait(false);
        if (index?.Procedures == null)
        {
            return null;
        }

        // Search through all procedures for columns that reference the specified table
        foreach (var procedure in index.Procedures)
        {
            if (procedure.ResultSets == null) continue;

            foreach (var resultSet in procedure.ResultSets)
            {
                if (resultSet.Columns == null) continue;

                var matchingColumn = FindColumnInHierarchy(resultSet.Columns, schema, tableName, columnName);
                if (matchingColumn != null)
                {
                    return matchingColumn;
                }
            }
        }

        return null;
    }

    public async Task<IReadOnlyList<IndexColumnEntry>> GetTableColumnsMetadataAsync(string schema, string tableName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(tableName))
        {
            return Array.Empty<IndexColumnEntry>();
        }

        var index = await LoadIndexAsync(cancellationToken).ConfigureAwait(false);
        if (index?.Procedures == null)
        {
            return Array.Empty<IndexColumnEntry>();
        }

        var result = new Dictionary<string, IndexColumnEntry>(StringComparer.OrdinalIgnoreCase);

        foreach (var procedure in index.Procedures)
        {
            if (procedure.ResultSets == null)
            {
                continue;
            }

            foreach (var resultSet in procedure.ResultSets)
            {
                if (resultSet.Columns == null)
                {
                    continue;
                }

                CollectColumns(resultSet.Columns, schema, tableName, result);
            }
        }

        if (result.Count == 0)
        {
            return Array.Empty<IndexColumnEntry>();
        }

        return result.Values.ToList();
    }

    public async Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var index = await LoadIndexAsync(cancellationToken).ConfigureAwait(false);
            return index != null;
        }
        catch
        {
            return false;
        }
    }

    private async Task<IndexDocument?> LoadIndexAsync(CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;

        // Use cached index if still valid
        if (_cachedIndex != null && (now - _lastIndexRead) < _cacheValidityDuration)
        {
            return _cachedIndex;
        }

        var working = DirectoryUtils.GetWorkingDirectory();
        if (string.IsNullOrEmpty(working))
        {
            return null;
        }

        var indexPath = Path.Combine(working, ".xtraq", "snapshots", "index.json");
        if (!File.Exists(indexPath))
        {
            return null;
        }

        try
        {
            await using var stream = File.OpenRead(indexPath);
            var index = await JsonSerializer.DeserializeAsync<IndexDocument>(stream, _jsonOptions, cancellationToken).ConfigureAwait(false);

            _cachedIndex = index;
            _lastIndexRead = now;

            return index;
        }
        catch
        {
            _cachedIndex = null;
            return null;
        }
    }

    private static IndexColumnEntry? FindColumnInHierarchy(IReadOnlyList<IndexColumnEntry> columns, string schema, string tableName, string? columnName)
    {
        foreach (var column in columns)
        {
            // Check if this column matches the search criteria
            if (string.Equals(column.SourceSchema, schema, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(column.SourceTable, tableName, StringComparison.OrdinalIgnoreCase))
            {
                if (string.IsNullOrWhiteSpace(columnName) ||
                    string.Equals(column.SourceColumn, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return column;
                }
            }

            // Recursively search nested columns
            if (column.Columns != null && column.Columns.Count > 0)
            {
                var nestedMatch = FindColumnInHierarchy(column.Columns, schema, tableName, columnName);
                if (nestedMatch != null)
                {
                    return nestedMatch;
                }
            }
        }

        return null;
    }

    private static void CollectColumns(IReadOnlyList<IndexColumnEntry> columns, string schema, string tableName, Dictionary<string, IndexColumnEntry> result)
    {
        foreach (var column in columns)
        {
            if (column == null)
            {
                continue;
            }

            if (string.Equals(column.SourceSchema, schema, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(column.SourceTable, tableName, StringComparison.OrdinalIgnoreCase))
            {
                var key = !string.IsNullOrWhiteSpace(column.SourceColumn)
                    ? column.SourceColumn!
                    : column.Name;

                if (!string.IsNullOrWhiteSpace(key))
                {
                    var normalizedKey = key!;
                    if (!result.TryGetValue(normalizedKey, out var existing))
                    {
                        result[normalizedKey] = column;
                    }
                    else if (ShouldReplace(existing, column))
                    {
                        result[normalizedKey] = column;
                    }
                }
            }

            if (column.Columns != null && column.Columns.Count > 0)
            {
                CollectColumns(column.Columns, schema, tableName, result);
            }
        }
    }

    private static bool ShouldReplace(IndexColumnEntry existing, IndexColumnEntry candidate)
    {
        if (string.IsNullOrWhiteSpace(existing.SqlTypeName) && !string.IsNullOrWhiteSpace(candidate.SqlTypeName))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(existing.UserTypeRef) && !string.IsNullOrWhiteSpace(candidate.UserTypeRef))
        {
            return true;
        }

        if (!existing.MaxLength.HasValue && candidate.MaxLength.HasValue)
        {
            return true;
        }

        if (!existing.Precision.HasValue && candidate.Precision.HasValue)
        {
            return true;
        }

        if (!existing.Scale.HasValue && candidate.Scale.HasValue)
        {
            return true;
        }

        return false;
    }
}
