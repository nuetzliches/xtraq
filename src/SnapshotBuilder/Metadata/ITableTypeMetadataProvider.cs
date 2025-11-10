using Xtraq.Data.Models;

namespace Xtraq.SnapshotBuilder.Metadata;

internal interface ITableTypeMetadataProvider
{
    Task<IReadOnlyList<TableTypeMetadata>> GetTableTypesAsync(ISet<string> schemas, CancellationToken cancellationToken);
}

internal sealed record TableTypeMetadata(TableType TableType, IReadOnlyList<Column> Columns);
