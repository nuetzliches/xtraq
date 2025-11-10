namespace Xtraq.Metadata;

/// <summary>
/// Represents a high-level result type emitted by the generator for a procedure execution.
/// </summary>
/// <param name="OperationName">The logical operation associated with the result.</param>
/// <param name="PayloadType">The CLR type that captures the aggregated payload.</param>
/// <param name="HasErrorField">Indicates whether the payload includes an error field.</param>
/// <param name="Summary">Optional summary documentation.</param>
/// <param name="Remarks">Optional additional remarks.</param>
public sealed record ResultDescriptor(
    string OperationName,
    string PayloadType,
    bool HasErrorField = true,
    string? Summary = null,
    string? Remarks = null
);
