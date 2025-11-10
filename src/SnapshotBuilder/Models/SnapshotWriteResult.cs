
namespace Xtraq.SnapshotBuilder.Models;

internal sealed class SnapshotWriteResult
{
    public int FilesWritten { get; init; }
    public int FilesUnchanged { get; init; }
    public IReadOnlyList<ProcedureAnalysisResult> UpdatedProcedures { get; init; } = Array.Empty<ProcedureAnalysisResult>();
}
