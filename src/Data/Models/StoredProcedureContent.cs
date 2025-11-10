using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class StoredProcedureContent
{
    [SqlFieldName("definition")]
    public string Definition { get; set; } = string.Empty;
}
