
namespace Xtraq.Extensions;

internal static class VersionExtensions
{
    internal static string ToVersionString(this Version version)
    {
        return version.ToString(3);
    }
}
