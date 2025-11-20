using System.CommandLine;
using Xtraq.Cli.Hosting;

namespace Xtraq;

/// <summary>
/// Entry point wiring for the Xtraq command-line interface.
/// </summary>
public static class Program
{
    /// <summary>
    /// Executes the Xtraq CLI with the provided arguments.
    /// </summary>
    /// <param name="args">Incoming command-line arguments.</param>
    /// <returns>Exit code emitted by the invoked command.</returns>
    public static async Task<int> RunCliAsync(string[] args)
    {
        var normalizedArgs = CliHostUtilities.NormalizeInvocationArguments(args);
        CliEnvironmentBootstrapper.Initialize(normalizedArgs);

        await using var hostContext = new CliHostBuilder(normalizedArgs).Build();
        var rootCommand = new CliCommandAppBuilder(hostContext).Build();
        return await rootCommand.InvokeAsync(normalizedArgs).ConfigureAwait(false);
    }

    /// <summary>
    /// Default entry point for the CLI host.
    /// </summary>
    /// <param name="args">Command-line arguments forwarded by the runtime.</param>
    /// <returns>Exit code produced by <see cref="RunCliAsync(string[])"/>.</returns>
    public static Task<int> Main(string[] args) => RunCliAsync(args);
}

