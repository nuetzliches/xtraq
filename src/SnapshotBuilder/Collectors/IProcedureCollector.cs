using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Collectors;

internal interface IProcedureCollector
{
    Task<ProcedureCollectionResult> CollectAsync(SnapshotBuildOptions options, CancellationToken cancellationToken);
}
