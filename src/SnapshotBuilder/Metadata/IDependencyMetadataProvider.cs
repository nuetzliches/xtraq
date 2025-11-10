using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Metadata;

internal interface IDependencyMetadataProvider
{
    Task<IReadOnlyList<ProcedureDependency>> ResolveAsync(IEnumerable<ProcedureDependency> dependencies, CancellationToken cancellationToken);
    Task<ProcedureDependency?> ResolveAsync(ProcedureDependency dependency, CancellationToken cancellationToken);
}
