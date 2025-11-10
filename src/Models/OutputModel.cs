namespace Xtraq.Models;

internal sealed class OutputModel
{
    public string Namespace { get; set; } = string.Empty;
    public DataContextModel DataContext { get; set; } = new();
}
