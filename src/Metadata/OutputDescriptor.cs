
namespace Xtraq.Metadata;

/// <summary>
/// Represents the output contract for a stored procedure operation.
/// </summary>
/// <param name="OperationName">The logical operation name associated with the output.</param>
/// <param name="Fields">The fields included in the output record.</param>
/// <param name="Summary">Optional summary describing the output.</param>
/// <param name="Remarks">Optional extended documentation for the output payload.</param>
public sealed record OutputDescriptor(
    string OperationName,
    IReadOnlyList<FieldDescriptor> Fields,
    string? Summary = null,
    string? Remarks = null
);
