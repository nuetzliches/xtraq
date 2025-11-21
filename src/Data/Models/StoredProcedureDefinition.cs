using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class StoredProcedureDefinition
{
    [SqlFieldName("schema_name")]
    public string SchemaName { get; set; } = string.Empty;
    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;
    [SqlFieldName("id")]
    public int Id { get; set; }
    [SqlFieldName("definition")]
    public string Definition { get; set; } = string.Empty;
}

internal sealed class StoredProcedureInputBulk
{
    [SqlFieldName("schema_name")]
    public string SchemaName { get; set; } = string.Empty;
    [SqlFieldName("procedure_name")]
    public string StoredProcedureName { get; set; } = string.Empty;
    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;
    [SqlFieldName("is_nullable")]
    public bool IsNullable { get; set; }
    [SqlFieldName("system_type_name")]
    public string SqlTypeName { get; set; } = string.Empty;
    [SqlFieldName("max_length")]
    public int MaxLength { get; set; }
    [SqlFieldName("is_output")]
    public bool IsOutput { get; set; }
    [SqlFieldName("is_table_type")]
    public bool IsTableType { get; set; }
    [SqlFieldName("user_type_name")]
    public string? UserTypeName { get; set; }
    [SqlFieldName("user_type_id")]
    public int? UserTypeId { get; set; }
    [SqlFieldName("user_type_schema_name")]
    public string? UserTypeSchemaName { get; set; }
}
