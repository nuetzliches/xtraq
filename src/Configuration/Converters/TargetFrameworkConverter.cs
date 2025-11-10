using System.Text.Json.Serialization;
namespace Xtraq.Configuration;

internal sealed class TargetFrameworkConverter : JsonConverter<string?>
{
    public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return Constants.DefaultTargetFramework.ToFrameworkString();
        }

        var value = reader.GetString();
        if (string.IsNullOrWhiteSpace(value))
        {
            return Constants.DefaultTargetFramework.ToFrameworkString();
        }

        var framework = TargetFrameworkExtensions.FromString(value);
        return framework.ToFrameworkString();
    }

    public override void Write(Utf8JsonWriter writer, string? value, JsonSerializerOptions options)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            writer.WriteStringValue(Constants.DefaultTargetFramework.ToFrameworkString());
            return;
        }

        var framework = TargetFrameworkExtensions.FromString(value);
        writer.WriteStringValue(framework.ToFrameworkString());
    }
}
