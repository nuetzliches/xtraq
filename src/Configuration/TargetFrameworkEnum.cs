namespace Xtraq.Configuration;

internal enum TargetFrameworkEnum
{
    Net80,
    Net100
}

internal static class TargetFrameworkExtensions
{
    public static string ToDefaultTargetFramework(this TargetFrameworkEnum framework)
    {
        return Constants.DefaultTargetFramework.ToFrameworkString();
    }

    public static string ToFrameworkString(this TargetFrameworkEnum framework)
    {
        return framework switch
        {
            TargetFrameworkEnum.Net80 => "net8.0",
            TargetFrameworkEnum.Net100 => "net10.0",
            _ => framework.ToDefaultTargetFramework()
        };
    }

    public static TargetFrameworkEnum FromString(string frameworkString)
    {
        if (string.IsNullOrEmpty(frameworkString))
            return Constants.DefaultTargetFramework;

        return frameworkString.ToLowerInvariant() switch
        {
            "net8.0" => TargetFrameworkEnum.Net80,
            "net10.0" => TargetFrameworkEnum.Net100,
            _ => Constants.DefaultTargetFramework
        };
    }
}
