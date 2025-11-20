using Microsoft.Extensions.Configuration;
using Xtraq.Cli.Commands;
using Xtraq.Data;
using Xtraq.Extensions;

namespace Xtraq.Cli.Hosting;

/// <summary>
/// Configures the CLI host's configuration sources and dependency injection container.
/// </summary>
internal sealed class CliHostBuilder
{
    private readonly string[] _arguments;
    private readonly IServiceCollection _services = new ServiceCollection();
    private readonly IConfigurationBuilder _configurationBuilder = new ConfigurationBuilder();
    private readonly string _environmentName;

    public CliHostBuilder(string[] arguments)
    {
        _arguments = arguments ?? Array.Empty<string>();
        _environmentName = ResolveEnvironmentName();

        ConfigureDefaultConfiguration();
        ConfigureDefaultServices();
    }

    private void ConfigureDefaultConfiguration()
    {
        _configurationBuilder
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddJsonFile($"appsettings.{_environmentName}.json", optional: true);
    }

    private void ConfigureDefaultServices()
    {
        _services.AddXtraq();
        _services.AddDbContext();

        _services.AddSingleton<BuildCommand>();
        _services.AddSingleton<SnapshotCommand>();

        TryConfigureTemplateServices();
    }

    private void TryConfigureTemplateServices()
    {
        try
        {
            var templateRoot = Path.Combine(Directory.GetCurrentDirectory(), "src", "Templates");
            if (Directory.Exists(templateRoot))
            {
                _services.AddSingleton<Xtraq.Engine.ITemplateRenderer, Xtraq.Engine.SimpleTemplateEngine>();
                _services.AddSingleton<Xtraq.Engine.ITemplateLoader>(_ => new Xtraq.Engine.FileSystemTemplateLoader(templateRoot));
            }
        }
        catch (Exception tex)
        {
            Console.Error.WriteLine($"[xtraq templates warn] {tex.Message}");
        }
    }

    public CliHostBuilder ConfigureServices(Action<IServiceCollection> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_services);
        return this;
    }

    public CliHostBuilder ConfigureAppConfiguration(Action<IConfigurationBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_configurationBuilder);
        return this;
    }

    public CliHostContext Build()
    {
        var configuration = _configurationBuilder.Build();
        _services.AddSingleton<IConfiguration>(configuration);

        var serviceProvider = _services.BuildServiceProvider();
        return new CliHostContext(configuration, serviceProvider, _environmentName, _arguments);
    }

    private static string ResolveEnvironmentName()
    {
        return Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ??
               Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ??
               "Production";
    }
}
