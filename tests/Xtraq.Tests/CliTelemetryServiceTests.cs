using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Xtraq.Telemetry;
using Xtraq.Utils;
using Xunit;

namespace Xtraq.Tests;

[Collection(DirectoryWorkspaceCollection.Name)]
public static class CliTelemetryServiceTests
{
    [Fact]
    public static async Task CaptureAsync_WithOptOutEnvironment_DoesNotPersistTelemetry()
    {
        using var sandbox = new TelemetrySandbox();
        sandbox.SetEnvironmentVariable("XTRAQ_CLI_TELEMETRY_OPTOUT", "1");

        var service = sandbox.CreateService();
        await service.CaptureAsync(CreateTelemetryEvent());

        Assert.False(Directory.Exists(sandbox.TelemetryDirectory));
    }

    [Fact]
    public static async Task CaptureAsync_WithCiEnvironment_DoesNotPersistTelemetry()
    {
        using var sandbox = new TelemetrySandbox();
        sandbox.SetEnvironmentVariable("CI", "true");

        var service = sandbox.CreateService();
        await service.CaptureAsync(CreateTelemetryEvent(ciMode: true));

        Assert.False(Directory.Exists(sandbox.TelemetryDirectory));
    }

    [Fact]
    public static async Task CaptureAsync_WhenTelemetryEnabled_WritesUsageAndConsent()
    {
        using var sandbox = new TelemetrySandbox();

        var service = sandbox.CreateService();
        await service.CaptureAsync(CreateTelemetryEvent());

        Assert.True(File.Exists(sandbox.UsageLogPath));
        Assert.True(File.Exists(sandbox.ConsentPath));

        var line = await File.ReadAllTextAsync(sandbox.UsageLogPath);
        using var document = JsonDocument.Parse(line);
        Assert.Equal("build", document.RootElement.GetProperty("command").GetString());
        Assert.Equal("1.2.3", document.RootElement.GetProperty("version").GetString());
    }

    private static CliTelemetryEvent CreateTelemetryEvent(bool ciMode = false)
    {
        return new CliTelemetryEvent(
            CommandName: "build",
            CliVersion: "1.2.3",
            Success: true,
            Duration: TimeSpan.FromMilliseconds(250),
            ProjectRoot: null,
            CiMode: ciMode,
            TelemetryOptionEnabled: false,
            VerboseOptionEnabled: false,
            NoCacheOptionEnabled: false,
            EntityFrameworkOptionEnabled: false,
            RefreshSnapshotRequested: false,
            ProcedureFilter: null,
            AdditionalMetadata: new Dictionary<string, string>());
    }

    private sealed class TelemetrySandbox : IDisposable
    {
        private readonly string _basePath;
        private readonly Dictionary<string, string?> _environmentSnapshot = new(StringComparer.OrdinalIgnoreCase);

        public TelemetrySandbox()
        {
            _basePath = Path.Combine(Path.GetTempPath(), "xtraq-telemetry-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_basePath);
            DirectoryUtils.SetBasePath(_basePath);
        }

        public string TelemetryDirectory => Path.Combine(_basePath, ".xtraq", "telemetry");

        public string UsageLogPath => Path.Combine(TelemetryDirectory, "cli-usage.jsonl");

        public string ConsentPath => Path.Combine(TelemetryDirectory, "cli-telemetry-consent.json");

        public CliTelemetryService CreateService(bool isVerbose = true) => new(new TestConsoleService(isVerbose));

        public void SetEnvironmentVariable(string key, string? value)
        {
            if (!_environmentSnapshot.ContainsKey(key))
            {
                _environmentSnapshot[key] = Environment.GetEnvironmentVariable(key);
            }

            Environment.SetEnvironmentVariable(key, value);
        }

        public void Dispose()
        {
            DirectoryUtils.ResetBasePath();

            foreach (var pair in _environmentSnapshot)
            {
                Environment.SetEnvironmentVariable(pair.Key, pair.Value);
            }

            if (Directory.Exists(_basePath))
            {
                try
                {
                    Directory.Delete(_basePath, recursive: true);
                }
                catch
                {
                    // Ignore best-effort cleanup failures in CI.
                }
            }
        }
    }
}
