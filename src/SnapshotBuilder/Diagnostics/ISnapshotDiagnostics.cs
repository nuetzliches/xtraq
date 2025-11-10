using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Diagnostics;

/// <summary>
/// Lightweight hook for observing orchestration progress without hard-coding console output.
/// </summary>
internal interface ISnapshotDiagnostics
{
    ValueTask OnCollectionCompletedAsync(ProcedureCollectionResult result, CancellationToken cancellationToken);
    ValueTask OnAnalysisCompletedAsync(int analyzedCount, CancellationToken cancellationToken);
    ValueTask OnWriteCompletedAsync(Models.SnapshotWriteResult result, CancellationToken cancellationToken);
    ValueTask OnTelemetryAsync(SnapshotPhaseTelemetry telemetry, CancellationToken cancellationToken);
}
