using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class DbObject
{
    [SqlFieldName("object_id")]
    public int Id { get; set; }
}
