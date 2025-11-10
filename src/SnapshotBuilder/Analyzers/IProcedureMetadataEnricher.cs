using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Enriches <see cref="ProcedureModel"/> instances with metadata derived from caches and database lookups.
/// </summary>
internal interface IProcedureMetadataEnricher
{
    /// <summary>
    /// Applies metadata enrichment for the supplied procedure.
    /// </summary>
    /// <param name="request">The enrichment request.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    Task EnrichAsync(ProcedureMetadataEnrichmentRequest request, CancellationToken cancellationToken);
}

/// <summary>
/// Defines the context required for procedure metadata enrichment.
/// </summary>
/// <param name="Descriptor">Descriptor of the procedure being enriched.</param>
/// <param name="Procedure">The mutable procedure model.</param>
/// <param name="SnapshotFile">Optional snapshot file name used for metadata reuse.</param>
internal sealed record ProcedureMetadataEnrichmentRequest(
    ProcedureDescriptor Descriptor,
    ProcedureModel Procedure,
    string? SnapshotFile);
