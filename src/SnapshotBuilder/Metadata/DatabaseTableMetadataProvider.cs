using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Services;

namespace Xtraq.SnapshotBuilder.Metadata;

internal sealed class DatabaseTableMetadataProvider : ITableMetadataProvider
{
    private readonly DbContext _dbContext;
    private readonly IConsoleService _console;

    public DatabaseTableMetadataProvider(DbContext dbContext, IConsoleService console)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    public async Task<IReadOnlyList<TableMetadata>> GetTablesAsync(ISet<string> schemas, CancellationToken cancellationToken)
    {
        if (schemas == null || schemas.Count == 0)
        {
            return Array.Empty<TableMetadata>();
        }

        var schemaList = schemas
            .Where(static s => !string.IsNullOrWhiteSpace(s))
            .Select(static s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (schemaList.Count == 0)
        {
            return Array.Empty<TableMetadata>();
        }

        var results = new List<TableMetadata>();
        var columnLookup = await BuildColumnLookupAsync(schemaList, cancellationToken).ConfigureAwait(false);

        foreach (var schema in schemaList)
        {
            cancellationToken.ThrowIfCancellationRequested();
            List<Table> tables;
            try
            {
                var list = await _dbContext.TableListAsync(schema, cancellationToken).ConfigureAwait(false);
                tables = list ?? new List<Table>();
                _console.Verbose($"[snapshot-table] enumerated {tables.Count} table(s) in schema '{schema}'.");
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-table] failed to enumerate tables for schema '{schema}': {ex.Message}");
                continue;
            }

            foreach (var table in tables)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (table == null || string.IsNullOrWhiteSpace(table.SchemaName) || string.IsNullOrWhiteSpace(table.Name))
                {
                    continue;
                }

                var key = BuildTableColumnKey(table.SchemaName, table.Name);
                List<Column> columns;
                if (columnLookup.TryGetValue(key, out var cachedColumns))
                {
                    columns = cachedColumns;
                    _console.Verbose($"[snapshot-table] reused cached columns for {table.SchemaName}.{table.Name} ({columns.Count}).");
                }
                else
                {
                    columns = new List<Column>();
                    try
                    {
                        var list = await _dbContext.TableColumnsListAsync(table.SchemaName, table.Name, cancellationToken).ConfigureAwait(false);
                        if (list != null)
                        {
                            columns = list;
                            columnLookup[key] = columns;
                            _console.Verbose($"[snapshot-table] loaded {columns.Count} column(s) for {table.SchemaName}.{table.Name} (fallback).");
                        }
                    }
                    catch (Exception ex)
                    {
                        _console.Verbose($"[snapshot-table] failed to load columns for {table.SchemaName}.{table.Name}: {ex.Message}");
                    }
                }

                results.Add(new TableMetadata(table, columns));
            }
        }

        return results;
    }

    private async Task<Dictionary<string, List<Column>>> BuildColumnLookupAsync(IReadOnlyList<string> schemas, CancellationToken cancellationToken)
    {
        var lookup = new Dictionary<string, List<Column>>(StringComparer.OrdinalIgnoreCase);
        if (schemas == null || schemas.Count == 0)
        {
            return lookup;
        }

        try
        {
            var catalogColumns = await _dbContext.TableColumnsCatalogAsync(schemas, cancellationToken).ConfigureAwait(false);
            if (catalogColumns == null || catalogColumns.Count == 0)
            {
                return lookup;
            }

            foreach (var column in catalogColumns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.SchemaName) || string.IsNullOrWhiteSpace(column.TableName))
                {
                    continue;
                }

                var key = BuildTableColumnKey(column.SchemaName, column.TableName);
                if (!lookup.TryGetValue(key, out var list))
                {
                    list = new List<Column>();
                    lookup[key] = list;
                }

                list.Add(column);
            }

            _console.Verbose($"[snapshot-table] hydrated catalog column cache for {lookup.Count} table(s).");
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-table] catalog column preload failed: {ex.Message}");
        }

        return lookup;
    }

    private static string BuildTableColumnKey(string schema, string table)
    {
        return string.Concat(schema, "|", table);
    }
}
