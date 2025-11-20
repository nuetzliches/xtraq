using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal class Column
{
    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;

    [SqlFieldName("catalog_name")]
    public string? CatalogName { get; set; }

    [SqlFieldName("schema_name")]
    public string? SchemaName { get; set; }

    [SqlFieldName("table_name")]
    public string? TableName { get; set; }

    [SqlFieldName("is_nullable")]
    public bool IsNullable { get; set; }

    [SqlFieldName("system_type_name")]
    public string SqlTypeName { get; set; } = string.Empty;

    [SqlFieldName("max_length")]
    public int MaxLength { get; set; }

    [SqlFieldName("is_identity")]
    public int? IsIdentityRaw { get; set; }

    [SqlFieldName("user_type_name")]
    public string? UserTypeName { get; set; }

    [SqlFieldName("user_type_schema_name")]
    public string? UserTypeSchemaName { get; set; }

    [SqlFieldName("base_type_name")]
    public string? BaseSqlTypeName { get; set; }

    [SqlFieldName("precision")]
    public int? Precision { get; set; }

    [SqlFieldName("scale")]
    public int? Scale { get; set; }

    [SqlFieldName("has_default_value")]
    public bool HasDefaultValue { get; set; }

    [SqlFieldName("default_definition")]
    public string? DefaultDefinition { get; set; }

    [SqlFieldName("default_constraint_name")]
    public string? DefaultConstraintName { get; set; }

    [SqlFieldName("is_computed")]
    public bool IsComputed { get; set; }

    [SqlFieldName("computed_definition")]
    public string? ComputedDefinition { get; set; }

    [SqlFieldName("is_computed_persisted")]
    public bool IsComputedPersisted { get; set; }

    [SqlFieldName("is_rowguidcol")]
    public bool IsRowGuid { get; set; }

    [SqlFieldName("is_sparse")]
    public bool IsSparse { get; set; }

    [SqlFieldName("generated_always_type_desc")]
    public string? GeneratedAlwaysType { get; set; }

    [SqlFieldName("is_hidden")]
    public bool IsHidden { get; set; }

    [SqlFieldName("is_columnset")]
    public bool IsColumnSet { get; set; }
}
