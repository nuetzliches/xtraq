using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Writers;

internal interface ISnapshotWriter
{
    Task<SnapshotWriteResult> WriteAsync(IReadOnlyList<ProcedureAnalysisResult> analyzedProcedures, SnapshotBuildOptions options, CancellationToken cancellationToken);
}
