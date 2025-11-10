using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

/// <summary>
/// Represents a SQL user-defined table type discovered from the metadata catalog.
/// </summary>
internal sealed class TableType
{
    /// <summary>
    /// Gets or sets the unique identifier assigned to the user-defined table type.
    /// </summary>
    [SqlFieldName("user_type_id")]
    public int? UserTypeId { get; set; }

    /// <summary>
    /// Gets or sets the name of the SQL table type.
    /// </summary>
    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the owning schema for the SQL table type.
    /// </summary>
    [SqlFieldName("schema_name")]
    public string SchemaName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the column definitions that belong to the table type.
    /// </summary>
    public List<Column> Columns { get; set; } = new();
}
