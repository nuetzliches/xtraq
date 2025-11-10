using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class StoredProcedure
{
    [SqlFieldName("object_id")]
    public int Id { get; set; }

    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;

    [SqlFieldName("modify_date")]
    public DateTime Modified { get; set; }

    [SqlFieldName("schema_name")]
    public string SchemaName { get; set; } = string.Empty;
}
