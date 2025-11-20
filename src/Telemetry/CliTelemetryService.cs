using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Telemetry;

/// <summary>
/// Provides an abstraction for capturing anonymised CLI telemetry events.
/// </summary>
internal interface ICliTelemetryService
{
    /// <summary>
    /// Captures a telemetry event when the CLI completes a command execution.
    /// </summary>
    /// <param name="telemetryEvent">Event payload describing the executed command.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>A task that completes when the telemetry operation finishes.</returns>
    Task CaptureAsync(CliTelemetryEvent telemetryEvent, CancellationToken cancellationToken = default);
}

/// <summary>
/// Immutable representation of a single CLI invocation that should be recorded for telemetry.
/// </summary>
/// <param name="CommandName">Logical command identifier such as <c>build</c> or <c>snapshot</c>.</param>
/// <param name="CliVersion">Semantic version of the running CLI.</param>
/// <param name="Success">Indicates whether the command completed successfully.</param>
/// <param name="Duration">Total execution time for the command.</param>
/// <param name="ProjectRoot">Resolved project root (will be hashed before persistence).</param>
/// <param name="CiMode">Indicates whether the user explicitly enabled CI mode via <c>--ci</c>.</param>
/// <param name="TelemetryOptionEnabled">Indicates whether the <c>--telemetry</c> switch was specified.</param>
/// <param name="VerboseOptionEnabled">Indicates whether verbose output was enabled.</param>
/// <param name="NoCacheOptionEnabled">Indicates whether cache usage was disabled via <c>--no-cache</c>.</param>
/// <param name="EntityFrameworkOptionEnabled">Indicates whether Entity Framework integration helpers were enabled.</param>
/// <param name="RefreshSnapshotRequested">Indicates whether snapshot refresh was requested.</param>
/// <param name="ProcedureFilter">Raw procedure filter (hashed before persistence).</param>
/// <param name="AdditionalMetadata">Optional metadata bag containing non-sensitive, pre-sanitised keys and values.</param>
internal sealed record CliTelemetryEvent(
    string CommandName,
    string CliVersion,
    bool Success,
    TimeSpan Duration,
    string? ProjectRoot,
    bool CiMode,
    bool TelemetryOptionEnabled,
    bool VerboseOptionEnabled,
    bool NoCacheOptionEnabled,
    bool EntityFrameworkOptionEnabled,
    bool RefreshSnapshotRequested,
    string? ProcedureFilter,
    IReadOnlyDictionary<string, string>? AdditionalMetadata);

/// <summary>
/// Default telemetry pipeline that persists anonymised CLI usage data locally while respecting opt-out semantics.
/// </summary>
internal sealed class CliTelemetryService : ICliTelemetryService
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true
    };

    private readonly IConsoleService _console;
    private readonly object _sync = new();
    private bool _initialized;
    private string _telemetryDirectory = string.Empty;

    /// <summary>
    /// Initialises a new instance of the <see cref="CliTelemetryService"/> class.
    /// </summary>
    /// <param name="console">Console abstraction used for disclosure messages and verbose logging.</param>
    public CliTelemetryService(IConsoleService console)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    /// <inheritdoc />
    public async Task CaptureAsync(CliTelemetryEvent telemetryEvent, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(telemetryEvent);

        if (!telemetryEvent.TelemetryOptionEnabled)
        {
            _console.Verbose("telemetry: cli usage capture skipped (requires --telemetry).");
            return;
        }

        Initialize();

        try
        {
            var envelope = BuildPayload(telemetryEvent);
            Directory.CreateDirectory(_telemetryDirectory);
            var fileName = $"cli-command-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json";
            var filePath = Path.Combine(_telemetryDirectory, fileName);
            var payload = JsonSerializer.Serialize(envelope, SerializerOptions);
            await File.WriteAllTextAsync(filePath, payload, cancellationToken).ConfigureAwait(false);
            var relativePath = Path.Combine(".xtraq", "telemetry", fileName).Replace('\\', '/');
            _console.Verbose($"telemetry: cli command recorded path={relativePath}");
        }
        catch (Exception ex)
        {
            _console.Verbose($"[telemetry] capture failed reason={ex.Message}");
        }
    }

    /// <summary>
    /// Performs one-time initialisation of directory paths and opt-out detection.
    /// </summary>
    private void Initialize()
    {
        if (_initialized)
        {
            return;
        }

        lock (_sync)
        {
            if (_initialized)
            {
                return;
            }

            var projectRoot = DirectoryUtils.GetWorkingDirectory();
            if (string.IsNullOrWhiteSpace(projectRoot))
            {
                projectRoot = ProjectRootResolver.ResolveCurrent();
            }

            string baseDirectory;
            if (string.IsNullOrWhiteSpace(projectRoot))
            {
                var userHome = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                baseDirectory = string.IsNullOrWhiteSpace(userHome)
                    ? Path.Combine(Environment.CurrentDirectory, ".xtraq")
                    : Path.Combine(userHome, ".xtraq");
            }
            else
            {
                baseDirectory = Path.Combine(projectRoot, ".xtraq");
            }

            _telemetryDirectory = Path.Combine(baseDirectory, "telemetry");
            _initialized = true;
        }
    }

    /// <summary>
    /// Builds the serialisable payload for a telemetry event.
    /// </summary>
    /// <param name="telemetryEvent">Event metadata supplied by the CLI pipeline.</param>
    /// <returns>Payload suitable for JSON serialisation.</returns>
    private CliTelemetryEnvelope BuildPayload(CliTelemetryEvent telemetryEvent)
    {
        var projectHash = string.IsNullOrWhiteSpace(telemetryEvent.ProjectRoot)
            ? null
            : HashString(telemetryEvent.ProjectRoot);
        var procedureFilterHash = string.IsNullOrWhiteSpace(telemetryEvent.ProcedureFilter)
            ? null
            : HashString(telemetryEvent.ProcedureFilter);

        return new CliTelemetryEnvelope
        {
            SchemaVersion = 2,
            TimestampUtc = DateTimeOffset.UtcNow,
            Command = telemetryEvent.CommandName,
            Version = telemetryEvent.CliVersion,
            Success = telemetryEvent.Success,
            DurationMs = Convert.ToInt64(Math.Round(telemetryEvent.Duration.TotalMilliseconds, MidpointRounding.AwayFromZero)),
            RuntimeIdentifier = System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier,
            OperatingSystem = Environment.OSVersion.VersionString,
            CiMode = telemetryEvent.CiMode,
            Verbose = telemetryEvent.VerboseOptionEnabled,
            NoCache = telemetryEvent.NoCacheOptionEnabled,
            EntityFramework = telemetryEvent.EntityFrameworkOptionEnabled,
            RefreshSnapshot = telemetryEvent.RefreshSnapshotRequested,
            ProjectRootHash = projectHash,
            ProcedureFilterHash = procedureFilterHash,
            Metadata = telemetryEvent.AdditionalMetadata
        };
    }

    private static string HashString(string value)
    {
        using var sha = SHA256.Create();
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = sha.ComputeHash(bytes);
        return Convert.ToHexString(hash);
    }

    private sealed class CliTelemetryEnvelope
    {
        public int SchemaVersion { get; init; }
        public DateTimeOffset TimestampUtc { get; init; }
        public string Command { get; init; } = string.Empty;
        public string Version { get; init; } = string.Empty;
        public bool Success { get; init; }
        public long DurationMs { get; init; }
        public string RuntimeIdentifier { get; init; } = string.Empty;
        public string OperatingSystem { get; init; } = string.Empty;
        public bool CiMode { get; init; }
        public bool Verbose { get; init; }
        public bool NoCache { get; init; }
        public bool EntityFramework { get; init; }
        public bool RefreshSnapshot { get; init; }
        public string? ProjectRootHash { get; init; }
        public string? ProcedureFilterHash { get; init; }
        public IReadOnlyDictionary<string, string>? Metadata { get; init; }
    }
}
