
namespace Xtraq.Metadata;

/// <summary>
/// Minimal function descriptor: reflects lean snapshot without definition or hash.
/// For table-valued functions <see cref="ReturnSqlType"/> remains null while the columns are exposed via <see cref="Columns"/>.
/// </summary>
public sealed record FunctionDescriptor(
    string SchemaName,
    string FunctionName,
    bool IsTableValued,
    string? ReturnSqlType,
    int? ReturnMaxLength,
    bool? ReturnIsNullable,
    JsonPayloadDescriptor? JsonPayload,
    bool IsEncrypted,
    IReadOnlyList<string> Dependencies,
    IReadOnlyList<FunctionParameterDescriptor> Parameters,
    IReadOnlyList<TableValuedFunctionColumnDescriptor> Columns
);

/// <summary>
/// Describes a function parameter including CLR mapping information.
/// </summary>
public sealed record FunctionParameterDescriptor(
    string Name,
    string SqlType,
    string ClrType,
    bool IsNullable,
    int? MaxLength,
    bool IsOutput
);

/// <summary>
/// Describes a single column of the row returned by a table-valued function.
/// </summary>
public sealed record TableValuedFunctionColumnDescriptor(
    string Name,
    string SqlType,
    string ClrType,
    bool IsNullable,
    int? MaxLength
);

