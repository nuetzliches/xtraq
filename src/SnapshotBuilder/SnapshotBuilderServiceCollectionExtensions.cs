using Microsoft.Extensions.DependencyInjection.Extensions;
using Xtraq.Data;
using Xtraq.Schema;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Analyzers;
using Xtraq.SnapshotBuilder.Cache;
using Xtraq.SnapshotBuilder.Collectors;
using Xtraq.SnapshotBuilder.Diagnostics;
using Xtraq.SnapshotBuilder.Metadata;
using Xtraq.SnapshotBuilder.Writers;

namespace Xtraq.SnapshotBuilder;

internal static class SnapshotBuilderServiceCollectionExtensions
{
    internal static IServiceCollection AddSnapshotBuilder(this IServiceCollection services)
    {
        services.AddSingleton<IDependencyMetadataProvider, DatabaseDependencyMetadataProvider>();
        services.AddSingleton<IFunctionJsonMetadataProvider, DatabaseFunctionJsonMetadataProvider>();
        services.AddSingleton<ITableMetadataProvider, DatabaseTableMetadataProvider>();
        services.AddSingleton<IUserDefinedTypeMetadataProvider, DatabaseUserDefinedTypeMetadataProvider>();
        services.TryAddSingleton<SchemaSnapshotFileLayoutService>();
        services.AddSingleton<ITableTypeMetadataProvider>(provider => new DatabaseTableTypeMetadataProvider(
            provider.GetRequiredService<DbContext>(),
            provider.GetRequiredService<IConsoleService>(),
            provider.GetRequiredService<SchemaSnapshotFileLayoutService>()));
        services.AddSingleton<IProcedureModelBuilder>(provider =>
        {
            var schemaMetadataProvider = provider.GetService<IEnhancedSchemaMetadataProvider>();
            return new ProcedureModelScriptDomBuilder(schemaMetadataProvider);
        });
        services.AddSingleton<IProcedureAstBuilder>(provider => (IProcedureAstBuilder)provider.GetRequiredService<IProcedureModelBuilder>());
        services.AddSingleton<IProcedureMetadataEnricher>(provider => new ProcedureMetadataEnricher(
            provider.GetRequiredService<IConsoleService>(),
            provider.GetRequiredService<IFunctionJsonMetadataProvider>(),
            provider.GetRequiredService<IEnhancedSchemaMetadataProvider>()));
        services.AddSingleton<IProcedureCollector, DatabaseProcedureCollector>();
        services.AddSingleton<IProcedureAnalyzer, DatabaseProcedureAnalyzer>();
        services.AddSingleton<ISnapshotWriter>(provider =>
        {
            var console = provider.GetRequiredService<IConsoleService>();
            var dbContext = provider.GetRequiredService<DbContext>();
            var tableMetadataProvider = provider.GetRequiredService<ITableMetadataProvider>();
            var tableTypeMetadataProvider = provider.GetRequiredService<ITableTypeMetadataProvider>();
            var userDefinedTypeMetadataProvider = provider.GetRequiredService<IUserDefinedTypeMetadataProvider>();
            return new ExpandedSnapshotWriter(console, dbContext, tableMetadataProvider, tableTypeMetadataProvider, userDefinedTypeMetadataProvider);
        });
        services.AddSingleton<ISnapshotCache, FileSnapshotCache>();
        services.AddSingleton<ISnapshotDiagnostics, ConsoleSnapshotDiagnostics>();
        services.AddSingleton<SnapshotBuildOrchestrator>();
        return services;
    }
}
