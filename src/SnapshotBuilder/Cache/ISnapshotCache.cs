using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Cache;

internal interface ISnapshotCache
{
    Task InitializeAsync(SnapshotBuildOptions options, CancellationToken cancellationToken);
    ProcedureCacheEntry? TryGetProcedure(ProcedureDescriptor descriptor);
    Task RecordReuseAsync(ProcedureCollectionItem item, CancellationToken cancellationToken);
    Task RecordAnalysisAsync(ProcedureAnalysisResult result, CancellationToken cancellationToken);
    Task FlushAsync(CancellationToken cancellationToken);
}
