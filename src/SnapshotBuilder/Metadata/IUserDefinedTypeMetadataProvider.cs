using Xtraq.Data.Queries;

namespace Xtraq.SnapshotBuilder.Metadata;

internal interface IUserDefinedTypeMetadataProvider
{
    Task<IReadOnlyList<UserDefinedTypeRow>> GetUserDefinedTypesAsync(ISet<string> schemas, CancellationToken cancellationToken);
    Task<UserDefinedTypeRow?> GetUserDefinedTypeAsync(string? catalog, string schema, string name, CancellationToken cancellationToken);
}
