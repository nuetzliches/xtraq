
namespace Xtraq.SnapshotBuilder.Models;

internal sealed class ProcedureCollectionResult
{
    public IReadOnlyList<ProcedureCollectionItem> Items { get; init; } = new List<ProcedureCollectionItem>();
}
