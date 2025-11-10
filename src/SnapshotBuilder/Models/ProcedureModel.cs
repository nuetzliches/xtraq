namespace Xtraq.SnapshotBuilder.Models;

/// <summary>
/// Represents the parsed structure of a stored procedure relevant for snapshot generation.
/// </summary>
internal sealed class ProcedureModel
{
    public List<ProcedureExecutedProcedureCall> ExecutedProcedures { get; } = new();
    public List<ProcedureResultSet> ResultSets { get; } = new();
}

/// <summary>
/// Represents a procedure call executed within another stored procedure (EXEC statements).
/// </summary>
internal sealed class ProcedureExecutedProcedureCall
{
    /// <summary>
    /// Catalog name of the executed procedure (null for default catalog).
    /// </summary>
    public string? Catalog { get; set; }

    /// <summary>
    /// Schema name of the executed procedure (null for default schema).
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// Name of the executed procedure.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Indicates whether this procedure call was successfully captured during analysis.
    /// </summary>
    public bool IsCaptured { get; set; }
}

/// <summary>
/// Represents a result set returned by a stored procedure (SELECT statements or forwarded EXEC results).
/// </summary>
internal sealed class ProcedureResultSet
{
    /// <summary>
    /// Indicates whether this result set returns JSON data (FOR JSON detected).
    /// </summary>
    public bool ReturnsJson { get; set; }

    /// <summary>
    /// Indicates whether this result set returns a JSON array (FOR JSON without WITHOUT_ARRAY_WRAPPER).
    /// </summary>
    public bool ReturnsJsonArray { get; set; }

    /// <summary>
    /// The root property name for JSON output (e.g., ROOT('propertyName') in FOR JSON ROOT('propertyName')).
    /// Used to wrap JSON results in a named property instead of default array structure.
    /// Example: FOR JSON ROOT('data') produces {"data": [...]} instead of [...]
    /// </summary>
    public string? JsonRootProperty { get; set; }

    /// <summary>
    /// Indicates whether the JSON projection uses INCLUDE_NULL_VALUES to emit explicit null properties.
    /// </summary>
    public bool JsonIncludeNullValues { get; set; }

    /// <summary>
    /// Indicates whether this result set uses SELECT * (wildcard selection).
    /// </summary>
    public bool HasSelectStar { get; set; }

    /// <summary>
    /// Indicates whether FOR JSON WITHOUT_ARRAY_WRAPPER is backed by a single-row guarantee (e.g., TOP 1 or aggregate projection).
    /// A value of <c>true</c> means the analyzer found a guarantee, <c>false</c> means it determined multiple rows may be produced, and <c>null</c> means the condition is unknown.
    /// </summary>
    public bool? JsonSingleRowGuaranteed { get; set; }

    /// <summary>
    /// Reference to another procedure whose results are forwarded through this result set.
    /// Used for procedure forwarding scenarios where one procedure EXECs another and returns its results.
    /// </summary>
    public ProcedureReference? Reference { get; set; }

    /// <summary>
    /// List of columns in this result set.
    /// </summary>
    public List<ProcedureResultColumn> Columns { get; } = new();
}

/// <summary>
/// Represents a single column in a procedure result set with detailed metadata for code generation.
/// </summary>
internal sealed class ProcedureResultColumn
{
    /// <summary>
    /// Column name as it appears in the result set.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Type of expression that produces this column (column reference, cast, function call, etc.).
    /// </summary>
    public ProcedureResultColumnExpressionKind ExpressionKind { get; set; }

    /// <summary>
    /// Source schema name when this column originates from a table column.
    /// </summary>
    public string? SourceSchema { get; set; }

    /// <summary>
    /// Source catalog name when this column originates from a table column.
    /// </summary>
    public string? SourceCatalog { get; set; }

    /// <summary>
    /// Source table name when this column originates from a table column.
    /// </summary>
    public string? SourceTable { get; set; }

    /// <summary>
    /// Source column name when this column originates from a table column.
    /// </summary>
    public string? SourceColumn { get; set; }

    /// <summary>
    /// Table alias used in the query when this column originates from a table.
    /// </summary>
    public string? SourceAlias { get; set; }

    /// <summary>
    /// SQL data type name (e.g., 'nvarchar', 'int', 'datetime2').
    /// </summary>
    public string? SqlTypeName { get; set; }

    /// <summary>
    /// Target type when this column is explicitly cast (e.g., CAST(column AS varchar)).
    /// </summary>
    public string? CastTargetType { get; set; }

    /// <summary>
    /// Length parameter for cast target type (e.g., 50 in CAST(column AS varchar(50))).
    /// </summary>
    public int? CastTargetLength { get; set; }

    /// <summary>
    /// Precision parameter for cast target type (e.g., 10 in CAST(column AS decimal(10,2))).
    /// </summary>
    public int? CastTargetPrecision { get; set; }

    /// <summary>
    /// Scale parameter for cast target type (e.g., 2 in CAST(column AS decimal(10,2))).
    /// </summary>
    public int? CastTargetScale { get; set; }

    /// <summary>
    /// Indicates whether this column contains an integer literal value.
    /// </summary>
    public bool HasIntegerLiteral { get; set; }

    /// <summary>
    /// Indicates whether this column contains a decimal literal value.
    /// </summary>
    public bool HasDecimalLiteral { get; set; }

    /// <summary>
    /// Indicates whether this column can contain NULL values.
    /// </summary>
    public bool? IsNullable { get; set; }

    /// <summary>
    /// Indicates whether this column originated from a literal NULL expression.
    /// </summary>
    public bool HasNullLiteral { get; set; }

    /// <summary>
    /// Indicates whether this column is explicitly marked as nullable regardless of source constraints.
    /// </summary>
    public bool? ForcedNullable { get; set; }

    /// <summary>
    /// Indicates whether this column contains nested JSON data (JSON within JSON).
    /// </summary>
    public bool? IsNestedJson { get; set; }

    /// <summary>
    /// Indicates whether this individual column returns JSON data.
    /// </summary>
    public bool? ReturnsJson { get; set; }

    /// <summary>
    /// Indicates whether this individual column returns a JSON array.
    /// </summary>
    public bool? ReturnsJsonArray { get; set; }

    /// <summary>
    /// The root property name for JSON output at the column level.
    /// Similar to result set level but applies to individual JSON columns.
    /// </summary>
    public string? JsonRootProperty { get; set; }

    /// <summary>
    /// Indicates whether the JSON projection for this column specifies INCLUDE_NULL_VALUES.
    /// </summary>
    public bool? JsonIncludeNullValues { get; set; }

    /// <summary>
    /// Indicates whether FOR JSON WITHOUT_ARRAY_WRAPPER on this column is backed by a single-row guarantee (e.g., TOP 1 or aggregate projection).
    /// A value of <c>true</c> means the analyzer found a guarantee, <c>false</c> means it determined multiple rows may be produced, and <c>null</c> means the condition is unknown.
    /// </summary>
    public bool? JsonSingleRowGuaranteed { get; set; }

    /// <summary>
    /// CLR type of the JSON array elements when <see cref="ReturnsJsonArray"/> is true.
    /// </summary>
    public string? JsonElementClrType { get; set; }

    /// <summary>
    /// SQL type of the JSON array elements when <see cref="ReturnsJsonArray"/> is true.
    /// </summary>
    public string? JsonElementSqlType { get; set; }

    /// <summary>
    /// Indicates whether this column returns JSON of unknown structure (fallback when analysis fails).
    /// </summary>
    public bool? ReturnsUnknownJson { get; set; }

    /// <summary>
    /// Nested columns when this column contains structured data (e.g., user-defined table types).
    /// </summary>
    public List<ProcedureResultColumn> Columns { get; } = new();

    /// <summary>
    /// Schema name of user-defined type when this column uses a custom type.
    /// </summary>
    public string? UserTypeSchemaName { get; set; }

    /// <summary>
    /// Name of user-defined type when this column uses a custom type.
    /// </summary>
    public string? UserTypeName { get; set; }

    /// <summary>
    /// Maximum length for variable-length data types (e.g., varchar(100)).
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// Indicates whether the column definition is ambiguous and may require manual review.
    /// </summary>
    public bool? IsAmbiguous { get; set; }

    /// <summary>
    /// Raw SQL expression text for this column (for debugging and advanced analysis).
    /// </summary>
    public string? RawExpression { get; set; }

    /// <summary>
    /// Indicates whether this column is produced by an aggregate function (COUNT, SUM, etc.).
    /// </summary>
    public bool IsAggregate { get; set; }

    /// <summary>
    /// Name of the aggregate function when IsAggregate is true (e.g., 'COUNT', 'SUM').
    /// </summary>
    public string? AggregateFunction { get; set; }

    /// <summary>
    /// Reference to another database object that this column depends on.
    /// </summary>
    public ProcedureReference? Reference { get; set; }

    /// <summary>
    /// Indicates whether JSON expansion for this column is deferred to a later analysis phase.
    /// </summary>
    public bool? DeferredJsonExpansion { get; set; }
}

/// <summary>
/// Represents a reference to another database object (procedure, function, view, etc.).
/// Used for tracking dependencies and forwarding scenarios.
/// </summary>
internal sealed class ProcedureReference
{
    /// <summary>
    /// Type of database object being referenced.
    /// </summary>
    public ProcedureReferenceKind Kind { get; set; }

    /// <summary>
    /// Schema name of the referenced object (null for default schema).
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// Catalog name of the referenced object (null for default catalog).
    /// </summary>
    public string? Catalog { get; set; }

    /// <summary>
    /// Name of the referenced object.
    /// </summary>
    public string? Name { get; set; }
}

/// <summary>
/// Types of database objects that can be referenced in procedure analysis.
/// </summary>
internal enum ProcedureReferenceKind
{
    /// <summary>
    /// Unknown or unspecified reference type.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Reference to a stored procedure.
    /// </summary>
    Procedure,

    /// <summary>
    /// Reference to a scalar or table-valued function.
    /// </summary>
    Function,

    /// <summary>
    /// Reference to a view.
    /// </summary>
    View,

    /// <summary>
    /// Reference to a table.
    /// </summary>
    Table,

    /// <summary>
    /// Reference to a user-defined table type.
    /// </summary>
    TableType
}

internal enum ProcedureResultColumnExpressionKind
{
    Unknown = 0,
    ColumnRef,
    Cast,
    FunctionCall,
    JsonQuery,
    Computed
}
