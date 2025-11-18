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
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IConsoleService _console;
    private readonly object _sync = new();
    private bool _initialized;
    private bool _enabled;
    private bool _disclosureRecorded;
    private string _telemetryDirectory = string.Empty;
    private string _usageLogPath = string.Empty;
    private string _consentMarkerPath = string.Empty;
    private string _machineIdPath = string.Empty;
    private string? _machineId;

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

        Initialize();

        if (!_enabled)
        {
            return;
        }

        try
        {
            await EnsureDisclosureAsync(cancellationToken).ConfigureAwait(false);
            var envelope = BuildPayload(telemetryEvent);
            Directory.CreateDirectory(_telemetryDirectory);
            var payload = JsonSerializer.Serialize(envelope, SerializerOptions);
            await AppendLineAsync(_usageLogPath, payload, cancellationToken).ConfigureAwait(false);
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
            _usageLogPath = Path.Combine(_telemetryDirectory, "cli-usage.jsonl");
            _consentMarkerPath = Path.Combine(_telemetryDirectory, "cli-telemetry-consent.json");
            _machineIdPath = Path.Combine(_telemetryDirectory, "machine-id");

            _enabled = EvaluateTelemetryEnabled();
            _initialized = true;
        }
    }

    /// <summary>
    /// Determines whether telemetry should remain enabled based on environment hints.
    /// </summary>
    /// <returns><c>true</c> when telemetry may execute, otherwise <c>false</c>.</returns>
    private static bool EvaluateTelemetryEnabled()
    {
        if (IsTelemetryOptOut(Environment.GetEnvironmentVariable("DOTNET_CLI_TELEMETRY_OPTOUT")))
        {
            return false;
        }

        if (IsTelemetryOptOut(Environment.GetEnvironmentVariable("XTRAQ_CLI_TELEMETRY_OPTOUT")))
        {
            return false;
        }

        if (IsTelemetryOptOut(Environment.GetEnvironmentVariable("XTRAQ_TELEMETRY_OPTOUT")))
        {
            return false;
        }

        if (DetectCiEnvironment())
        {
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks whether a supplied environment variable value requests telemetry opt-out.
    /// </summary>
    /// <param name="value">Raw environment variable value.</param>
    /// <returns><c>true</c> when telemetry should be disabled.</returns>
    private static bool IsTelemetryOptOut(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = value.Trim();
        return normalized.Equals("1", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("true", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("y", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("on", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Detects whether the CLI is executing inside a recognised CI environment.
    /// </summary>
    /// <returns><c>true</c> when a CI flag is detected.</returns>
    private static bool DetectCiEnvironment()
    {
        var ciVariables = new[]
        {
            "CI",
            "TF_BUILD",
            "GITHUB_ACTIONS",
            "APPVEYOR",
            "TRAVIS",
            "CIRCLECI",
            "TEAMCITY_VERSION",
            "CI_PIPELINE_ID",
            "JENKINS_URL",
            "GITLAB_CI",
            "CODEBUILD_BUILD_ID",
            "BITBUCKET_BUILD_NUMBER"
        };

        foreach (var variable in ciVariables)
        {
            if (IsEnvironmentFlagSet(Environment.GetEnvironmentVariable(variable)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Determines whether an environment variable should be treated as an affirmative flag.
    /// </summary>
    /// <param name="value">Raw environment value.</param>
    /// <returns><c>true</c> when the flag is set.</returns>
    private static bool IsEnvironmentFlagSet(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (bool.TryParse(value, out var parsed))
        {
            return parsed;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
            || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || value.Equals("y", StringComparison.OrdinalIgnoreCase)
            || value.Equals("on", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Ensures that the telemetry disclosure message is printed once per environment and persisted.
    /// </summary>
    /// <param name="cancellationToken">Token used to observe cancellation while persisting the consent marker.</param>
    private async Task EnsureDisclosureAsync(CancellationToken cancellationToken)
    {
        if (_disclosureRecorded)
        {
            return;
        }

        var shouldNotify = false;

        lock (_sync)
        {
            if (_disclosureRecorded)
            {
                return;
            }

            shouldNotify = !File.Exists(_consentMarkerPath);
            _disclosureRecorded = true;
        }

        if (shouldNotify && !ShouldSuppressDisclosureMessage())
        {
            _console.Output("Telemetry\n---------\nXtraq collects anonymised usage data to improve the CLI. Set XTRAQ_CLI_TELEMETRY_OPTOUT=1 or DOTNET_CLI_TELEMETRY_OPTOUT=1 to opt out. More details: https://xtraq.dev/meta/cli-telemetry");
        }

        try
        {
            Directory.CreateDirectory(_telemetryDirectory);
            var optOutMap = CollectOptOutVariables();
            var consent = new CliTelemetryConsent
            {
                Version = 1,
                NotifiedUtc = DateTimeOffset.UtcNow,
                TelemetryEnabled = _enabled,
                CiDetected = DetectCiEnvironment(),
                OptOutVariables = optOutMap
            };
            var content = JsonSerializer.Serialize(consent, SerializerOptions);
            await File.WriteAllTextAsync(_consentMarkerPath, content, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[telemetry] failed to persist consent marker reason={ex.Message}");
        }
    }

    /// <summary>
    /// Determines whether the disclosure banner should be suppressed based on environment configuration.
    /// </summary>
    /// <returns><c>true</c> when the banner should be hidden.</returns>
    private static bool ShouldSuppressDisclosureMessage()
    {
        if (IsEnvironmentFlagSet(Environment.GetEnvironmentVariable("DOTNET_NOLOGO")))
        {
            return true;
        }

        if (IsEnvironmentFlagSet(Environment.GetEnvironmentVariable("XTRAQ_NOLOGO")))
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Collects opt-out environment variables for auditing.
    /// </summary>
    /// <returns>Dictionary describing which opt-out variables were set.</returns>
    private static Dictionary<string, bool> CollectOptOutVariables()
    {
        return new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
        {
            ["DOTNET_CLI_TELEMETRY_OPTOUT"] = IsTelemetryOptOut(Environment.GetEnvironmentVariable("DOTNET_CLI_TELEMETRY_OPTOUT")),
            ["XTRAQ_CLI_TELEMETRY_OPTOUT"] = IsTelemetryOptOut(Environment.GetEnvironmentVariable("XTRAQ_CLI_TELEMETRY_OPTOUT")),
            ["XTRAQ_TELEMETRY_OPTOUT"] = IsTelemetryOptOut(Environment.GetEnvironmentVariable("XTRAQ_TELEMETRY_OPTOUT"))
        };
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
        var machineHash = HashString(EnsureMachineId());

        return new CliTelemetryEnvelope
        {
            SchemaVersion = 1,
            TimestampUtc = DateTimeOffset.UtcNow,
            Command = telemetryEvent.CommandName,
            Version = telemetryEvent.CliVersion,
            Success = telemetryEvent.Success,
            DurationMs = Convert.ToInt64(Math.Round(telemetryEvent.Duration.TotalMilliseconds, MidpointRounding.AwayFromZero)),
            MachineId = machineHash,
            RuntimeIdentifier = System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier,
            OperatingSystem = Environment.OSVersion.VersionString,
            CiDetected = telemetryEvent.CiMode || DetectCiEnvironment(),
            ProjectRootHash = projectHash,
            ProcedureFilterHash = procedureFilterHash,
            TelemetrySwitchEnabled = telemetryEvent.TelemetryOptionEnabled,
            CiMode = telemetryEvent.CiMode,
            Verbose = telemetryEvent.VerboseOptionEnabled,
            NoCache = telemetryEvent.NoCacheOptionEnabled,
            EntityFramework = telemetryEvent.EntityFrameworkOptionEnabled,
            RefreshSnapshot = telemetryEvent.RefreshSnapshotRequested,
            Metadata = telemetryEvent.AdditionalMetadata
        };
    }

    /// <summary>
    /// Ensures a persistent, non-identifying machine identifier exists.
    /// </summary>
    /// <returns>Stable machine identifier string.</returns>
    private string EnsureMachineId()
    {
        if (!string.IsNullOrEmpty(_machineId))
        {
            return _machineId!;
        }

        lock (_sync)
        {
            if (!string.IsNullOrEmpty(_machineId))
            {
                return _machineId!;
            }

            if (File.Exists(_machineIdPath))
            {
                var persisted = File.ReadAllText(_machineIdPath).Trim();
                if (!string.IsNullOrWhiteSpace(persisted))
                {
                    _machineId = persisted;
                    return _machineId!;
                }
            }

            var generated = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
            Directory.CreateDirectory(_telemetryDirectory);
            File.WriteAllText(_machineIdPath, generated);
            _machineId = generated;
            return _machineId!;
        }
    }

    /// <summary>
    /// Computes a SHA256 hash for the provided value and returns the hex representation.
    /// </summary>
    /// <param name="value">Value to hash.</param>
    /// <returns>Uppercase hexadecimal hash string.</returns>
    private static string HashString(string value)
    {
        using var sha = SHA256.Create();
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = sha.ComputeHash(bytes);
        return Convert.ToHexString(hash);
    }

    /// <summary>
    /// Appends a single line to a JSONL file asynchronously.
    /// </summary>
    /// <param name="filePath">Destination file path.</param>
    /// <param name="content">Content to append as a single line.</param>
    /// <param name="cancellationToken">Cancellation token to observe.</param>
    private static async Task AppendLineAsync(string filePath, string content, CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, true);
        await using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        cancellationToken.ThrowIfCancellationRequested();
        await writer.WriteLineAsync(content).ConfigureAwait(false);
    }

    /// <summary>
    /// Minimal schema used for persisted telemetry events.
    /// </summary>
    private sealed class CliTelemetryEnvelope
    {
        public int SchemaVersion { get; init; }
        public DateTimeOffset TimestampUtc { get; init; }
        public string Command { get; init; } = string.Empty;
        public string Version { get; init; } = string.Empty;
        public bool Success { get; init; }
        public long DurationMs { get; init; }
        public string MachineId { get; init; } = string.Empty;
        public string RuntimeIdentifier { get; init; } = string.Empty;
        public string OperatingSystem { get; init; } = string.Empty;
        public bool CiDetected { get; init; }
        public string? ProjectRootHash { get; init; }
        public string? ProcedureFilterHash { get; init; }
        public bool TelemetrySwitchEnabled { get; init; }
        public bool CiMode { get; init; }
        public bool Verbose { get; init; }
        public bool NoCache { get; init; }
        public bool EntityFramework { get; init; }
        public bool RefreshSnapshot { get; init; }
        public IReadOnlyDictionary<string, string>? Metadata { get; init; }
    }

    /// <summary>
    /// Document persisted when the disclosure banner has been shown.
    /// </summary>
    private sealed class CliTelemetryConsent
    {
        public int Version { get; init; }
        public DateTimeOffset NotifiedUtc { get; init; }
        public bool TelemetryEnabled { get; init; }
        public bool CiDetected { get; init; }
        public IReadOnlyDictionary<string, bool> OptOutVariables { get; init; } = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
    }
}
