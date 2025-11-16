
namespace Xtraq.Metadata;

/// <summary>
/// Aggregates the metadata describing a stored procedure exposed by the generator.
/// </summary>
/// <param name="ProcedureName">The procedure name without schema qualification.</param>
/// <param name="Schema">The owning schema for the procedure.</param>
/// <param name="OperationName">The logical operation name generated for the procedure.</param>
/// <param name="InputParameters">The input parameters accepted by the procedure.</param>
/// <param name="OutputFields">The output fields exposed through output parameters.</param>
/// <param name="ResultSets">The result set descriptors produced by the procedure.</param>
/// <param name="TableTypeParameters">Table type parameter descriptors referenced by the procedure.</param>
/// <param name="Summary">Optional summary documentation.</param>
/// <param name="Remarks">Optional extended remarks.</param>
public sealed record ProcedureDescriptor(
    string ProcedureName,
    string Schema,
    string OperationName,
    IReadOnlyList<FieldDescriptor> InputParameters,
    IReadOnlyList<FieldDescriptor> OutputFields,
    IReadOnlyList<ResultSetDescriptor> ResultSets,
    IReadOnlyList<TableTypeParameterDescriptor> TableTypeParameters,
    string? Summary = null,
    string? Remarks = null
);
