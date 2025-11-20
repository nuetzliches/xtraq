#pragma warning disable CS0618 // SchemaMetadataProvider remains Obsolete while downstream migration completes.
using Xtraq.Metadata;
using Xtraq.Schema;
using Xtraq.Utils;

namespace Xtraq.Services;

/// <summary>
/// Snapshot-aware schema metadata provider that warms supporting services before relying on the
/// legacy descriptor loader. Inherits existing parsing logic so consumers can migrate without
/// touching generator code while the legacy implementation is phased out.
/// </summary>
internal sealed class SnapshotSchemaMetadataProvider : SchemaMetadataProvider
{
    private readonly string _projectRoot;
    private readonly IConsoleService _console;
    private readonly SchemaSnapshotFileLayoutService _layoutService;
    private readonly IEnhancedSchemaMetadataProvider _enhancedSchemaMetadataProvider;

    /// <summary>
    /// Creates a provider using manually supplied dependencies (primarily used by dependency injection).
    /// </summary>
    internal SnapshotSchemaMetadataProvider(
        IConsoleService console,
        SchemaSnapshotFileLayoutService layoutService,
        IEnhancedSchemaMetadataProvider enhancedSchemaMetadataProvider)
        : this(null, console, layoutService, enhancedSchemaMetadataProvider)
    {
    }

    /// <summary>
    /// Creates a provider for the specified project root, constructing default dependencies for standalone usage.
    /// </summary>
    internal SnapshotSchemaMetadataProvider(
        string? projectRoot = null,
        IConsoleService? console = null,
        SchemaSnapshotFileLayoutService? layoutService = null,
        IEnhancedSchemaMetadataProvider? enhancedSchemaMetadataProvider = null)
        : base(ResolveProjectRoot(projectRoot))
    {
        _projectRoot = ResolveProjectRoot(projectRoot);
        _console = console ?? new ConsoleService(new CommandOptions());
        _layoutService = layoutService ?? new SchemaSnapshotFileLayoutService();
        _enhancedSchemaMetadataProvider = enhancedSchemaMetadataProvider ?? CreateEnhancedProvider(_console);

        WarmUpSupportingServices();
    }

    private void WarmUpSupportingServices()
    {
        try
        {
            DirectoryUtils.SetBasePath(_projectRoot);
        }
        catch
        {
            // best effort; base path initialization isn't critical when directories already resolved
        }

        try
        {
            _ = _layoutService.LoadExpanded();
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-metadata] Snapshot layout warm-up failed: {ex.Message}");
        }

        try
        {
            _enhancedSchemaMetadataProvider.IsOfflineModeAvailableAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-metadata] Offline metadata probe failed: {ex.Message}");
        }
    }

    private static string ResolveProjectRoot(string? projectRoot)
    {
        if (!string.IsNullOrWhiteSpace(projectRoot))
        {
            try
            {
                return Path.GetFullPath(projectRoot);
            }
            catch
            {
                return projectRoot!;
            }
        }

        return DirectoryUtils.GetWorkingDirectory();
    }

    private static IEnhancedSchemaMetadataProvider CreateEnhancedProvider(IConsoleService console)
    {
        var indexProvider = new SnapshotIndexMetadataProvider();
        return new EnhancedSchemaMetadataProvider(indexProvider, dbContext: null, console);
    }
}
#pragma warning restore CS0618
