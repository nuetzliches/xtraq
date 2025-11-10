using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Builds <see cref="ProcedureModel"/> instances from SQL definitions.
/// </summary>
internal interface IProcedureModelBuilder
{
    ProcedureModel? Build(string? definition, string? defaultSchema, string? defaultCatalog, bool verboseParsing);
}
