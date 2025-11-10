using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

internal interface IProcedureAnalyzer
{
    Task<IReadOnlyList<ProcedureAnalysisResult>> AnalyzeAsync(
        IReadOnlyList<ProcedureCollectionItem> items,
        SnapshotBuildOptions options,
        CancellationToken cancellationToken);
}
