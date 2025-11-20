using Microsoft.Extensions.Configuration;

namespace Xtraq.Cli.Hosting;

/// <summary>
/// Represents the configured CLI host, exposing shared state such as configuration and services.
/// </summary>
internal sealed class CliHostContext : IAsyncDisposable, IDisposable
{
    private readonly ServiceProvider _serviceProvider;

    public CliHostContext(IConfiguration configuration, ServiceProvider serviceProvider, string environmentName, string[] arguments)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        EnvironmentName = string.IsNullOrWhiteSpace(environmentName) ? "Production" : environmentName;
        Arguments = arguments ?? Array.Empty<string>();
    }

    public IConfiguration Configuration { get; }

    public IServiceProvider Services => _serviceProvider;

    public string EnvironmentName { get; }

    public string[] Arguments { get; }

    public void Dispose()
    {
        _serviceProvider.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        return _serviceProvider.DisposeAsync();
    }
}
