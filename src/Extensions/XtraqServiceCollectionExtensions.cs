using Microsoft.Extensions.DependencyInjection.Extensions;
using Xtraq.Cache;
using Xtraq.Data;
using Xtraq.Generators;
using Xtraq.Metadata;
using Xtraq.Runtime;
using Xtraq.Schema;
using Xtraq.Services;
using Xtraq.SnapshotBuilder;
using Xtraq.Telemetry;
using Xtraq.Utils;

namespace Xtraq.Extensions;

/// <summary>
/// Extension methods for registering Xtraq services in the DI container.
/// </summary>
public static class XtraqServiceCollectionExtensions
{
    /// <summary>
    /// Registers the full Xtraq service suite required by the CLI and generators.
    /// </summary>
    /// <param name="services">The service collection to extend.</param>
    /// <returns>The same service collection instance for chaining.</returns>
    public static IServiceCollection AddXtraq(this IServiceCollection services)
    {
        services.TryAddSingleton<CommandOptions>();

        services.AddSingleton<IConsoleService>(provider =>
            new ConsoleService(provider.GetRequiredService<CommandOptions>()));

        services.AddSingleton<XtraqService>();

        AddManagerServices(services);

        services.AddSingleton<DbContextGenerator>();
        services.AddSnapshotBuilder();

        return services;
    }

    private static void AddManagerServices(IServiceCollection services)
    {
        services.AddSingleton<OutputService>();
        services.AddSingleton<Services.SchemaSnapshotFileLayoutService>();
        services.AddSingleton<SchemaManager>();
        services.AddSingleton<XtraqCliRuntime>();
        services.AddSingleton<ILocalCacheService, LocalCacheService>();
        services.AddSingleton<ISnapshotResolutionService, SnapshotResolutionService>();
        services.AddSingleton<ISchemaMetadataProvider>(provider =>
        {
            var console = provider.GetRequiredService<IConsoleService>();
            var layout = provider.GetRequiredService<SchemaSnapshotFileLayoutService>();
            var enhanced = provider.GetRequiredService<IEnhancedSchemaMetadataProvider>();
            var projectRoot = DirectoryUtils.GetWorkingDirectory();
            return new SnapshotSchemaMetadataProvider(projectRoot, console, layout, enhanced);
        });
        services.AddSingleton<UpdateService>();

        // Schema Object Caching Services
        services.AddSingleton<ISchemaObjectCacheManager, SchemaObjectCacheManager>();
        services.AddSingleton<ISchemaObjectIndexManager, SchemaObjectIndexManager>();
        services.AddSingleton<ISchemaChangeDetectionService, SchemaChangeDetectionService>();
        services.AddSingleton<ISchemaInvalidationOrchestrator, SchemaInvalidationOrchestrator>();

        // JSON Function Enhancement
        services.AddSingleton<IJsonFunctionEnhancementService, JsonFunctionEnhancementService>();

        // Snapshot Index Metadata Provider for offline build support
        services.AddSingleton<ISnapshotIndexMetadataProvider, SnapshotIndexMetadataProvider>();
        services.AddSingleton<IEnhancedSchemaMetadataProvider, EnhancedSchemaMetadataProvider>();

        // Telemetry
        services.AddSingleton<IDatabaseTelemetryCollector, DatabaseTelemetryCollector>();
        services.AddSingleton<ICliTelemetryService, CliTelemetryService>();
    }
}
