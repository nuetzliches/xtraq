using Xtraq.Utils;

namespace Xtraq.Services;

/// <summary>
/// Service for checking and performing auto-updates of the Xtraq Global Tool.
/// Provides async update checking without blocking normal command execution.
/// </summary>
public class UpdateService
{
    private static readonly HttpClient HttpClient = new();
    private const string NuGetApiUrl = "https://api.nuget.org/v3-flatcontainer/xtraq/index.json";
    private const int UpdateCheckTimeoutMs = 5000; // 5 seconds timeout for update checks

    /// <summary>
    /// Checks if an update is available for the current Xtraq installation.
    /// This method is designed to be non-blocking and will timeout after 5 seconds.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Update information or null if check failed/no update available</returns>
    public async Task<UpdateInfo?> CheckForUpdateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(UpdateCheckTimeoutMs);

            var currentVersion = GetCurrentVersion();
            if (string.IsNullOrWhiteSpace(currentVersion))
            {
                return null;
            }

            var latestVersion = await GetLatestVersionFromNuGetAsync(cts.Token).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(latestVersion))
            {
                return null;
            }

            if (IsNewerVersion(latestVersion, currentVersion))
            {
                return new UpdateInfo
                {
                    CurrentVersion = currentVersion,
                    LatestVersion = latestVersion,
                    IsUpdateAvailable = true
                };
            }

            return new UpdateInfo
            {
                CurrentVersion = currentVersion,
                LatestVersion = latestVersion,
                IsUpdateAvailable = false
            };
        }
        catch (OperationCanceledException)
        {
            // Timeout or cancellation - this is expected and should not be treated as an error
            return null;
        }
        catch (Exception ex)
        {
            // Log error but don't throw - update check should never break normal operations
            DebugOutputHelper.WriteVerboseDebug($"[update] Check failed: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Performs the actual update of the Xtraq Global Tool using dotnet tool update.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>True if update was successful, false otherwise</returns>
    public async Task<bool> UpdateAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = "tool update -g xtraq",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            using var process = new Process { StartInfo = startInfo };
            process.Start();

            var outputTask = process.StandardOutput.ReadToEndAsync();
            var errorTask = process.StandardError.ReadToEndAsync();

            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

            var output = await outputTask.ConfigureAwait(false);
            var error = await errorTask.ConfigureAwait(false);

            if (process.ExitCode == 0)
            {
                DebugOutputHelper.WriteVerboseDebug($"[update] Success: {output}");
                return true;
            }
            else
            {
                DebugOutputHelper.WriteVerboseDebug($"[update] Failed (exit {process.ExitCode}): {error}");
                return false;
            }
        }
        catch (Exception ex)
        {
            DebugOutputHelper.WriteVerboseDebug($"[update] Update process failed: {ex.Message}");
            return false;
        }
    }

    private static string? GetCurrentVersion()
    {
        try
        {
            var assembly = typeof(UpdateService).Assembly;
            var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            return informational ?? assembly.GetName().Version?.ToString();
        }
        catch
        {
            return null;
        }
    }

    private static async Task<string?> GetLatestVersionFromNuGetAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await HttpClient.GetStringAsync(NuGetApiUrl, cancellationToken).ConfigureAwait(false);
            using var document = JsonDocument.Parse(response);

            if (document.RootElement.TryGetProperty("versions", out var versionsElement) &&
                versionsElement.ValueKind == JsonValueKind.Array)
            {
                var versions = versionsElement.EnumerateArray();
                string? latest = null;

                foreach (var version in versions)
                {
                    if (version.ValueKind == JsonValueKind.String)
                    {
                        latest = version.GetString();
                    }
                }

                return latest;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static bool IsNewerVersion(string latestVersion, string currentVersion)
    {
        if (TryParseSemanticVersion(latestVersion, out var latest) &&
            TryParseSemanticVersion(currentVersion, out var current))
        {
            return CompareSemanticVersion(latest, current) > 0;
        }

        return !string.Equals(latestVersion, currentVersion, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryParseSemanticVersion(string value, out SemanticVersion result)
    {
        result = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var metadataIndex = trimmed.IndexOf('+');
        if (metadataIndex >= 0)
        {
            trimmed = trimmed[..metadataIndex];
        }

        string preRelease = string.Empty;
        var preReleaseIndex = trimmed.IndexOf('-');
        if (preReleaseIndex >= 0)
        {
            preRelease = trimmed[(preReleaseIndex + 1)..];
            trimmed = trimmed[..preReleaseIndex];
        }

        var segments = trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0 || segments.Length > 3)
        {
            return false;
        }

        static bool TryGetPart(string[] parts, int index, out int value)
        {
            value = 0;
            if (index >= parts.Length)
            {
                return true;
            }

            return int.TryParse(parts[index], NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
        }

        if (!TryGetPart(segments, 0, out var major) ||
            !TryGetPart(segments, 1, out var minor) ||
            !TryGetPart(segments, 2, out var patch))
        {
            return false;
        }

        result = new SemanticVersion(major, minor, patch, preRelease);
        return true;
    }

    private static int CompareSemanticVersion(SemanticVersion left, SemanticVersion right)
    {
        var compare = left.Major.CompareTo(right.Major);
        if (compare != 0)
        {
            return compare;
        }

        compare = left.Minor.CompareTo(right.Minor);
        if (compare != 0)
        {
            return compare;
        }

        compare = left.Patch.CompareTo(right.Patch);
        if (compare != 0)
        {
            return compare;
        }

        var leftPreRelease = left.PreRelease;
        var rightPreRelease = right.PreRelease;
        var leftHasPre = !string.IsNullOrEmpty(leftPreRelease);
        var rightHasPre = !string.IsNullOrEmpty(rightPreRelease);

        if (leftHasPre && !rightHasPre)
        {
            return -1;
        }

        if (!leftHasPre && rightHasPre)
        {
            return 1;
        }

        return string.Compare(leftPreRelease, rightPreRelease, StringComparison.OrdinalIgnoreCase);
    }

    private readonly record struct SemanticVersion(int Major, int Minor, int Patch, string PreRelease);

    /// <summary>
    /// Checks if auto-update is disabled via environment variables or .env settings.
    /// </summary>
    /// <returns>True if auto-update should be skipped</returns>
    public static bool IsUpdateDisabled()
    {
        var noUpdate = Environment.GetEnvironmentVariable("XTRAQ_NO_UPDATE");
        var skipUpdate = Environment.GetEnvironmentVariable("XTRAQ_SKIP_UPDATE");

        return EnvironmentHelper.EqualsTrue(noUpdate) || EnvironmentHelper.EqualsTrue(skipUpdate);
    }
}

/// <summary>
/// Information about available updates for the Xtraq tool.
/// </summary>
public class UpdateInfo
{
    /// <summary>
    /// Version string of the currently installed CLI.
    /// </summary>
    public string CurrentVersion { get; set; } = string.Empty;

    /// <summary>
    /// Version string reported by the remote package feed.
    /// </summary>
    public string LatestVersion { get; set; } = string.Empty;

    /// <summary>
    /// Indicates whether the remote version is newer than the local installation.
    /// </summary>
    public bool IsUpdateAvailable { get; set; }

    /// <inheritdoc />
    public override string ToString()
    {
        return IsUpdateAvailable
            ? $"Update available: {CurrentVersion} → {LatestVersion}"
            : $"Up to date: {CurrentVersion}";
    }
}
