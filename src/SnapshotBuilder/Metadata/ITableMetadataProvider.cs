using Xtraq.Data.Models;

namespace Xtraq.SnapshotBuilder.Metadata;

internal interface ITableMetadataProvider
{
    Task<IReadOnlyList<TableMetadata>> GetTablesAsync(ISet<string> schemas, CancellationToken cancellationToken);
}

internal sealed record TableMetadata(Table Table, IReadOnlyList<Column> Columns);
