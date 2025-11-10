using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Builds <see cref="ProcedureModel"/> instances from SQL procedure definitions.
/// </summary>
internal interface IProcedureAstBuilder
{
    /// <summary>
    /// Builds a <see cref="ProcedureModel"/> using the supplied request parameters.
    /// </summary>
    /// <param name="request">The AST build request.</param>
    /// <returns>The constructed model or <c>null</c> when the definition cannot be parsed.</returns>
    ProcedureModel? Build(ProcedureAstBuildRequest request);
}

/// <summary>
/// Defines the inputs required to build a procedure AST.
/// </summary>
/// <param name="Definition">The raw SQL text of the procedure.</param>
/// <param name="DefaultSchema">Default schema used for unqualified identifiers.</param>
/// <param name="DefaultCatalog">Default catalog used for unqualified identifiers.</param>
/// <param name="VerboseParsing">Enables verbose parsing diagnostics.</param>
internal sealed record ProcedureAstBuildRequest(
    string? Definition,
    string? DefaultSchema,
    string? DefaultCatalog,
    bool VerboseParsing);
