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
    public static async Task CaptureAsync_WhenTelemetryDisabled_DoesNotPersistTelemetry()
    {
        using var sandbox = new TelemetrySandbox();

        var service = sandbox.CreateService();
        await service.CaptureAsync(CreateTelemetryEvent(enableTelemetry: false));

        Assert.False(Directory.Exists(sandbox.TelemetryDirectory));
    }

    [Fact]
    public static async Task CaptureAsync_WhenTelemetryEnabled_WritesCommandDocument()
    {
        using var sandbox = new TelemetrySandbox();

        var service = sandbox.CreateService();
        await service.CaptureAsync(CreateTelemetryEvent(enableTelemetry: true, ciMode: true));

        Assert.True(Directory.Exists(sandbox.TelemetryDirectory));
        var commandFiles = Directory.GetFiles(sandbox.TelemetryDirectory, "cli-command-*.json");
        Assert.Single(commandFiles);

        var payload = await File.ReadAllTextAsync(commandFiles[0]);
        using var document = JsonDocument.Parse(payload);
        Assert.Equal("build", document.RootElement.GetProperty("command").GetString());
        Assert.Equal("1.2.3", document.RootElement.GetProperty("version").GetString());
        Assert.True(document.RootElement.GetProperty("ciMode").GetBoolean());
        Assert.True(document.RootElement.GetProperty("verbose").GetBoolean());
    }

    private static CliTelemetryEvent CreateTelemetryEvent(bool enableTelemetry, bool ciMode = false)
    {
        return new CliTelemetryEvent(
            CommandName: "build",
            CliVersion: "1.2.3",
            Success: true,
            Duration: TimeSpan.FromMilliseconds(250),
            ProjectRoot: null,
            CiMode: ciMode,
            TelemetryOptionEnabled: enableTelemetry,
            VerboseOptionEnabled: true,
            NoCacheOptionEnabled: false,
            EntityFrameworkOptionEnabled: false,
            RefreshSnapshotRequested: false,
            ProcedureFilter: null,
            AdditionalMetadata: new Dictionary<string, string>());
    }

    private sealed class TelemetrySandbox : IDisposable
    {
        private readonly string _basePath;

        public TelemetrySandbox()
        {
            _basePath = Path.Combine(Path.GetTempPath(), "xtraq-telemetry-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_basePath);
            DirectoryUtils.SetBasePath(_basePath);
        }

        public string TelemetryDirectory => Path.Combine(_basePath, ".xtraq", "telemetry");

        public CliTelemetryService CreateService(bool isVerbose = true) => new(new TestConsoleService(isVerbose));

        public void Dispose()
        {
            DirectoryUtils.ResetBasePath();

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
