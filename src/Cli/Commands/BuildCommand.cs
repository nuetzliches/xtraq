using Xtraq.Infrastructure;
using Xtraq.Runtime;

namespace Xtraq.Cli.Commands;

/// <summary>
/// Executes the build pipeline, optionally refreshing the snapshot before generation.
/// </summary>
internal sealed class BuildCommand : IXtraqCommand
{
    private readonly XtraqCliRuntime _runtime;

    public BuildCommand(XtraqCliRuntime runtime)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
    }

    public async ValueTask<int> ExecuteAsync(XtraqCommandContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.RefreshSnapshot)
        {
            var snapshotResult = await _runtime.SnapshotAsync(context.Options).ConfigureAwait(false);
            var snapshotExit = CommandResultMapper.Map(snapshotResult);
            if (snapshotExit != ExitCodes.Success)
            {
                return snapshotExit;
            }
        }

        var buildResult = await _runtime.BuildAsync(context.Options).ConfigureAwait(false);
        var buildExitCode = CommandResultMapper.Map(buildResult);

        if (context.Options.Telemetry && buildExitCode == ExitCodes.Success)
        {
            var telemetryLabel = context.RefreshSnapshot ? "build" : "build-only";
            await _runtime.PersistTelemetrySummaryAsync(telemetryLabel, cancellationToken).ConfigureAwait(false);
        }

        return buildExitCode;
    }
}
