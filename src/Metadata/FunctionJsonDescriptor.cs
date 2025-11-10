namespace Xtraq.Metadata;

/// <summary>
/// Describes the JSON payload emitted by a scalar function (e.g., identity.RecordAsJson).
/// </summary>
public sealed record FunctionJsonDescriptor(
    string SchemaName,
    string FunctionName,
    string RootTypeName,
    bool ReturnsJsonArray,
    bool IncludeNullValues,
    IReadOnlyList<FunctionJsonFieldDescriptor> Fields
);

/// <summary>
/// Represents a single property within a function JSON payload.
/// Container nodes have <see cref="ClrType"/> set to <c>null</c> and a non-empty <see cref="Children"/> collection.
/// </summary>
public sealed record FunctionJsonFieldDescriptor(
    string Name,
    string? ClrType,
    bool IsNullable,
    bool IsArray,
    bool IncludeNullValues,
    IReadOnlyList<FunctionJsonFieldDescriptor> Children
)
{
    internal bool IsContainer => Children.Count > 0;
}
