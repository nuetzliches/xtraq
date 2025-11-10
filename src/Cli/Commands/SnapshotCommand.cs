using Xtraq.Runtime;

namespace Xtraq.Cli.Commands;

/// <summary>
/// Executes the snapshot operation against the configured project.
/// </summary>
internal sealed class SnapshotCommand : IXtraqCommand
{
    private readonly XtraqCliRuntime _runtime;

    public SnapshotCommand(XtraqCliRuntime runtime)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
    }

    public async ValueTask<int> ExecuteAsync(XtraqCommandContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var result = await _runtime.SnapshotAsync(context.Options).ConfigureAwait(false);
        return CommandResultMapper.Map(result);
    }
}
