
namespace Xtraq.Metadata;

/// <summary>
/// Describes a single result set produced by a stored procedure execution.
/// </summary>
/// <param name="Index">The ordinal position of the result set within the procedure results.</param>
/// <param name="Name">The assigned name used for generated types.</param>
/// <param name="Fields">The fields present in the result set.</param>
/// <param name="IsScalar">Indicates whether the result set represents a single value.</param>
/// <param name="Optional">Indicates whether the result set is optional.</param>
/// <param name="HasSelectStar">Indicates whether the result set originated from a <c>SELECT *</c> projection.</param>
/// <param name="ExecSourceSchemaName">Optional schema name when the result originates from EXEC statements.</param>
/// <param name="ExecSourceProcedureName">Optional procedure name when the result originates from EXEC statements.</param>
/// <param name="ProcedureRef">Optional procedure reference used for dependency tracking.</param>
/// <param name="JsonPayload">Optional JSON payload descriptor when the result emits JSON.</param>
/// <param name="JsonStructure">Optional JSON structure description for strongly typed projections.</param>
public sealed record ResultSetDescriptor(
    int Index,
    string Name,
    IReadOnlyList<FieldDescriptor> Fields,
    bool IsScalar = false,
    bool Optional = true,
    bool HasSelectStar = false,
    string? ExecSourceSchemaName = null,
    string? ExecSourceProcedureName = null,
    string? ProcedureRef = null,
    JsonPayloadDescriptor? JsonPayload = null,
    IReadOnlyList<JsonFieldNode>? JsonStructure = null
);
