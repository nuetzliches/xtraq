namespace Xtraq.Cli;

internal interface ICommandOptions
{
    string Path { get; }
    bool Verbose { get; }
    bool Debug { get; }
    bool NoCache { get; }
    bool NoUpdate { get; }
    string Procedure { get; }
    bool Telemetry { get; }
    bool JsonIncludeNullValues { get; }
    bool HasJsonIncludeNullValuesOverride { get; }
    bool CiMode { get; }
}

internal class CommandOptions : ICommandOptions
{
    private readonly CliCommandOptions _current = new();

    public CommandOptions()
    {
    }

    public CommandOptions(ICommandOptions options)
    {
        Update(options);
    }

    /// <summary>
    /// Copies option values into the current instance, applying normalization for string inputs.
    /// </summary>
    /// <param name="options">Source options to copy from.</param>
    /// <exception cref="ArgumentNullException">Raised when <paramref name="options"/> is <c>null</c>.</exception>
    public void Update(ICommandOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _current.Path = Normalize(options.Path);
        _current.Verbose = options.Verbose;
        _current.Debug = options.Debug;
        _current.NoCache = options.NoCache;
        _current.NoUpdate = options.NoUpdate;
        _current.Procedure = Normalize(options.Procedure);
        _current.Telemetry = options.Telemetry;
        _current.JsonIncludeNullValues = options.JsonIncludeNullValues;
        _current.HasJsonIncludeNullValuesOverride = options.HasJsonIncludeNullValuesOverride;
        _current.CiMode = options.CiMode;
    }

    public string Path => Normalize(_current.Path);
    public bool Verbose => _current.Verbose;
    public bool Debug => _current.Debug;
    public bool NoCache => _current.NoCache;
    public bool NoUpdate => _current.NoUpdate;
    public string Procedure => Normalize(_current.Procedure);
    public bool Telemetry => _current.Telemetry;
    public bool JsonIncludeNullValues => _current.JsonIncludeNullValues;
    public bool HasJsonIncludeNullValuesOverride => _current.HasJsonIncludeNullValuesOverride;
    public bool CiMode => _current.CiMode;

    private static string Normalize(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim();
    }
}

/// <summary>
/// Mutable implementation used by the CLI to pass parsed options to the runtime.
/// </summary>
internal sealed class CliCommandOptions : ICommandOptions
{
    public string Path { get; set; } = string.Empty;
    public bool Verbose { get; set; }
    public bool Debug { get; set; }
    public bool NoCache { get; set; }
    public bool NoUpdate { get; set; }
    public string Procedure { get; set; } = string.Empty;
    public bool Telemetry { get; set; }
    public bool JsonIncludeNullValues { get; set; }
    public bool HasJsonIncludeNullValuesOverride { get; set; }
    public bool CiMode { get; set; }
}
