
namespace Xtraq.Metadata;

/// <summary>
/// Describes a single field emitted by stored procedure metadata, including JSON expansion hints.
/// </summary>
/// <param name="Name">The raw field name as supplied by SQL Server.</param>
/// <param name="PropertyName">The CLR-safe property name generated for the field.</param>
/// <param name="ClrType">The CLR type projected from the SQL type.</param>
/// <param name="IsNullable">Indicates whether the field can produce <c>null</c> values.</param>
/// <param name="SqlTypeName">The database type name returned by metadata discovery (may be null when unavailable).</param>
/// <param name="MaxLength">The optional maximum length for variable sized fields.</param>
/// <param name="Documentation">Human-readable documentation extracted from extended properties.</param>
/// <param name="Attributes">Optional additional attributes applied during code generation.</param>
/// <param name="FunctionRef">Optional linked function reference when the field derives from a function call.</param>
/// <param name="DeferredJsonExpansion">Indicates whether JSON payload expansion should be deferred.</param>
/// <param name="ReturnsJson">Indicates whether the field returns JSON content.</param>
/// <param name="ReturnsJsonArray">Indicates whether the JSON content is an array.</param>
/// <param name="JsonRootProperty">Optional root property name for JSON payloads.</param>
/// <param name="ReturnsUnknownJson">Indicates whether the JSON shape is unknown at build time.</param>
/// <param name="JsonElementClrType">The CLR type used for JSON element projections.</param>
/// <param name="JsonElementSqlType">The SQL type used when JSON columns are materialized as JSON elements.</param>
/// <param name="JsonIncludeNullValues">Indicates whether JSON serialization should include null values.</param>
public sealed record FieldDescriptor(
    string Name,
    string PropertyName,
    string ClrType,
    bool IsNullable,
    string? SqlTypeName,
    int? MaxLength = null,
    string? Documentation = null,
    IReadOnlyList<string>? Attributes = null,
    string? FunctionRef = null,
    bool? DeferredJsonExpansion = null,
    bool? ReturnsJson = null,
    bool? ReturnsJsonArray = null,
    string? JsonRootProperty = null,
    bool? ReturnsUnknownJson = null,
    string? JsonElementClrType = null,
    string? JsonElementSqlType = null,
    bool? JsonIncludeNullValues = null
);

