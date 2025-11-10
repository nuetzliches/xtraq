
namespace Xtraq.SnapshotBuilder.Writers;

internal sealed class IndexDocument
{
    public int SchemaVersion { get; set; } = 1;
    public string Fingerprint { get; set; } = string.Empty;
    public IndexParser Parser { get; set; } = new();
    public IndexStats Stats { get; set; } = new();
    public List<IndexProcedureEntry> Procedures { get; set; } = new();
    public List<IndexTableTypeEntry> TableTypes { get; set; } = new();
    public List<IndexUserDefinedTypeEntry> UserDefinedTypes { get; set; } = new();
    public int FunctionsVersion { get; set; }
    public List<IndexFunctionEntry> Functions { get; set; } = new();
    public List<IndexTableEntry> Tables { get; set; } = new();
}

internal sealed class IndexParser
{
    public string ToolVersion { get; set; } = string.Empty;
    public int ResultSetParserVersion { get; set; }
}

internal sealed class IndexStats
{
    public int ProcedureTotal { get; set; }
    public int ProcedureSkipped { get; set; }
    public int ProcedureLoaded { get; set; }
    public int UdttTotal { get; set; }
    public int TableTotal { get; set; }
    public int ViewTotal { get; set; }
    public int UserDefinedTypeTotal { get; set; }
    public int FunctionTotal { get; set; }
}

internal sealed class IndexProcedureEntry
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string File { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
    public List<IndexResultSetEntry>? ResultSets { get; set; }
}

internal sealed class IndexTableTypeEntry
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string File { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
}

internal sealed class IndexTableEntry
{
    public string? Catalog { get; set; }
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string File { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
}

internal sealed class IndexUserDefinedTypeEntry
{
    public string? Catalog { get; set; }
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string File { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
}

internal sealed class IndexFunctionEntry
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string File { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
}

/// <summary>
/// Metadata for result sets to enable offline build mode and table/column mapping persistence.
/// </summary>
internal sealed class IndexResultSetEntry
{
    public bool ReturnsJson { get; set; }
    public bool ReturnsJsonArray { get; set; }
    public string? JsonRootProperty { get; set; }
    public bool HasSelectStar { get; set; }
    public string? ProcedureRef { get; set; }
    public List<IndexColumnEntry>? Columns { get; set; }
}

/// <summary>
/// Column metadata with source table/column mapping for offline build support.
/// </summary>
internal sealed class IndexColumnEntry
{
    public string? Name { get; set; }
    public string? SqlTypeName { get; set; }
    public bool IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }

    // Source mapping for table/column relationships
    public string? SourceSchema { get; set; }
    public string? SourceTable { get; set; }
    public string? SourceColumn { get; set; }

    // JSON-specific metadata
    public bool ReturnsJson { get; set; }
    public bool ReturnsJsonArray { get; set; }
    public bool IsNestedJson { get; set; }
    public string? JsonRootProperty { get; set; }
    public string? JsonElementClrType { get; set; }
    public string? JsonElementSqlType { get; set; }
    public string? FunctionRef { get; set; }

    // User-defined type references
    public string? UserTypeSchema { get; set; }
    public string? UserTypeName { get; set; }

    // Nested columns for complex types
    public List<IndexColumnEntry>? Columns { get; set; }
}
