namespace Xtraq.Cli.Commands;

/// <summary>
/// Represents a command that can be executed by the Xtraq CLI runtime.
/// Implementations encapsulate the behaviour for verbs such as snapshot or build.
/// </summary>
internal interface IXtraqCommand
{
    /// <summary>
    /// Executes the command using the supplied invocation context.
    /// </summary>
    /// <param name="context">The invocation context describing the current CLI run.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The process exit code to return to the host.</returns>
    ValueTask<int> ExecuteAsync(XtraqCommandContext context, CancellationToken cancellationToken = default);
}

/// <summary>
/// Provides shared invocation data that commands can rely on during execution.
/// </summary>
internal sealed class XtraqCommandContext
{
    /// <summary>
    /// Creates a new command context with the provided dependencies and request metadata.
    /// </summary>
    /// <param name="projectPath">Absolute or relative project root that the command should operate on.</param>
    /// <param name="options">The parsed command options that apply to the current invocation.</param>
    /// <param name="services">The service provider for resolving additional dependencies.</param>
    /// <param name="console">Console abstraction used to communicate with the user.</param>
    /// <param name="refreshSnapshot">Indicates whether the execution should refresh the snapshot before continuing.</param>
    public XtraqCommandContext(
        string projectPath,
        ICommandOptions options,
        IServiceProvider services,
        Xtraq.Services.IConsoleService console,
        bool refreshSnapshot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectPath);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(console);

        ProjectPath = projectPath;
        Options = options;
        Services = services;
        Console = console;
        RefreshSnapshot = refreshSnapshot;
    }

    /// <summary>
    /// Gets the project root path for the current invocation.
    /// </summary>
    public string ProjectPath { get; }

    /// <summary>
    /// Gets the parsed command options.
    /// </summary>
    public ICommandOptions Options { get; }

    /// <summary>
    /// Gets the service provider used to resolve scoped dependencies.
    /// </summary>
    public IServiceProvider Services { get; }

    /// <summary>
    /// Gets the console abstraction for user interaction and output.
    /// </summary>
    public Xtraq.Services.IConsoleService Console { get; }

    /// <summary>
    /// Gets a value indicating whether a snapshot refresh should be performed before the command's primary work.
    /// </summary>
    public bool RefreshSnapshot { get; }
}
