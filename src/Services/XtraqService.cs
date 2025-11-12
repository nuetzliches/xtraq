namespace Xtraq.Services;

internal sealed class XtraqService
{
    public Version Version { get; }

    public string InformationalVersion { get; }

    public XtraqService()
    {
        var assembly = GetType().Assembly;
        Version = assembly.GetName().Version ?? new Version(0, 0);
        InformationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion
            ?? Version.ToString();
    }
}
