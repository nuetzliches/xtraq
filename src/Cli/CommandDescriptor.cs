namespace Xtraq.Cli;

/// <summary>
/// Describes the behavioural capabilities of a CLI command so that palette and CLI wiring stay in sync.
/// </summary>
[Flags]
internal enum CliCommandFeatures
{
    None = 0,
    RequiresProjectPath = 1 << 0,
    SupportsCache = 1 << 1,
    SupportsTelemetry = 1 << 2,
    SupportsProcedureFilter = 1 << 3,
    SchedulesUpdate = 1 << 4,
    SupportsRefreshOption = 1 << 5,
    DefaultAlias = 1 << 6
}

/// <summary>
/// Identifiers for all supported CLI commands.
/// </summary>
internal enum CliCommandKind
{
    Init,
    Build,
    Snapshot,
    Version,
    Update
}

/// <summary>
/// Metadata describing a CLI command including its handler type and feature set.
/// </summary>
internal sealed record CliCommandDescriptor(
    CliCommandKind Kind,
    string Name,
    string DisplayName,
    string Description,
    CliCommandFeatures Features,
    Type? HandlerType = null)
{
    internal bool HasFeature(CliCommandFeatures feature) => (Features & feature) == feature;
}

/// <summary>
/// Provides central access to the command descriptors used by the CLI and interactive palette.
/// </summary>
internal static class CliCommandCatalog
{
    private static readonly IReadOnlyList<CliCommandDescriptor> _commands = new[]
    {
        new CliCommandDescriptor(
            CliCommandKind.Init,
            "init",
            "init",
            "Initialize Xtraq project (.env bootstrap)",
            CliCommandFeatures.RequiresProjectPath),
        new CliCommandDescriptor(
            CliCommandKind.Snapshot,
            "snapshot",
            "snapshot",
            "Capture database metadata into .xtraq snapshots",
            CliCommandFeatures.RequiresProjectPath |
            CliCommandFeatures.SupportsCache |
            CliCommandFeatures.SupportsTelemetry |
            CliCommandFeatures.SupportsProcedureFilter |
            CliCommandFeatures.SchedulesUpdate,
            typeof(Commands.SnapshotCommand)),
        new CliCommandDescriptor(
            CliCommandKind.Build,
            "build",
            "build",
            "Generate client artefacts from local snapshots",
            CliCommandFeatures.RequiresProjectPath |
            CliCommandFeatures.SupportsCache |
            CliCommandFeatures.SupportsTelemetry |
            CliCommandFeatures.SupportsProcedureFilter |
            CliCommandFeatures.SchedulesUpdate |
            CliCommandFeatures.SupportsRefreshOption |
            CliCommandFeatures.DefaultAlias,
            typeof(Commands.BuildCommand)),
        new CliCommandDescriptor(
            CliCommandKind.Version,
            "version",
            "version",
            "Show installed and latest Xtraq versions",
            CliCommandFeatures.None),
        new CliCommandDescriptor(
            CliCommandKind.Update,
            "update",
            "update",
            "Update Xtraq Global Tool to the latest version",
            CliCommandFeatures.None)
    };

    internal static IReadOnlyList<CliCommandDescriptor> All => _commands;

    internal static CliCommandDescriptor Get(CliCommandKind kind)
        => _commands.First(descriptor => descriptor.Kind == kind);
}
