
namespace Xtraq.Metadata;

/// <summary>
/// Represents the input contract for a stored procedure operation.
/// </summary>
/// <param name="OperationName">The logical operation name describing the input.</param>
/// <param name="Fields">The collection of fields that compose the input record.</param>
/// <param name="Summary">Optional user-supplied summary describing the input.</param>
/// <param name="Remarks">Optional extended documentation for the input.</param>
public sealed record InputDescriptor(
    string OperationName,
    IReadOnlyList<FieldDescriptor> Fields,
    string? Summary = null,
    string? Remarks = null
);
