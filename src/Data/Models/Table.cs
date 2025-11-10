using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class Table
{
    [SqlFieldName("object_id")]
    public int ObjectId { get; set; }

    [SqlFieldName("schema_name")]
    public string SchemaName { get; set; } = string.Empty;

    [SqlFieldName("table_name")]
    public string Name { get; set; } = string.Empty;

    [SqlFieldName("catalog_name")]
    public string? CatalogName { get; set; }

    [SqlFieldName("modify_date")]
    public DateTime ModifyDate { get; set; }
}
