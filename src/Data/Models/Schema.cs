using Xtraq.Data.Attributes;

namespace Xtraq.Data.Models;

internal sealed class Schema
{
    [SqlFieldName("name")]
    public string Name { get; set; } = string.Empty;

    public override string ToString()
    {
        return Name;
    }
}
