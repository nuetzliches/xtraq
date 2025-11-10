using System.Text.Json.Serialization;
using Xtraq.Configuration;
using Xtraq.Infrastructure;

namespace Xtraq.Models;

internal sealed class ConfigurationModel : IVersioned
{
    [JsonConverter(typeof(StringVersionConverter))]
    public Version Version { get; set; } = new(1, 0);

    [JsonConverter(typeof(TargetFrameworkConverter))]
    public string TargetFramework { get; set; } = Constants.DefaultTargetFramework.ToFrameworkString(); // Allowed values: net8.0, net10.0

    public ProjectModel Project { get; set; } = new();
    public List<SchemaModel> Schema { get; set; } = new();
}
