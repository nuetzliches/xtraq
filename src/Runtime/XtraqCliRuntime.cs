using Microsoft.Data.SqlClient;
using Xtraq.Configuration;
using Xtraq.Core;
using Xtraq.Engine;
using Xtraq.Generators;
using Xtraq.Metadata;
using Xtraq.Services;
using Xtraq.SnapshotBuilder;
using Xtraq.Telemetry;
using Xtraq.Utils;
using SnapshotProcedureDescriptor = Xtraq.SnapshotBuilder.Models.ProcedureDescriptor;

namespace Xtraq.Runtime;

/// <summary>
/// Central runtime orchestration for the CLI commands (snapshot/build/version/update).
/// Previously implemented as XtraqManager under src/Managers.
/// Consolidated here to retire the legacy manager layer.
/// </summary>
internal sealed class XtraqCliRuntime(
    XtraqService service,
    IConsoleService consoleService,
    SnapshotBuildOrchestrator snapshotBuildOrchestrator,
    Xtraq.Data.DbContext dbContext,
    ISnapshotResolutionService snapshotResolutionService,
    UpdateService updateService,
    IDatabaseTelemetryCollector telemetryCollector
)
{
    private SnapshotResolutionPlan? _lastSnapshotPlan;
    private TelemetryRunSummary? _lastSnapshotSummary;
    private TelemetryRunSummary? _lastBuildSummary;

    public Task<ExecuteResultEnum> SnapshotAsync(ICommandOptions options)
    {
        // Update debug output settings from command options
        DebugOutputHelper.UpdateFromOptions(options.Verbose, options.Debug);
        return RunWithWarningSummaryAsync(() => SnapshotCoreAsync(options));
    }

    private async Task<ExecuteResultEnum> SnapshotCoreAsync(ICommandOptions options)
    {
        var workingDirectory = DirectoryUtils.GetWorkingDirectory();
        var cliOverrides = BuildCliOverrides(options);
        XtraqConfiguration cfg;
        var attemptedInit = false;
        while (true)
        {
            try
            {
                cfg = XtraqConfiguration.Load(projectRoot: workingDirectory, cliOverrides: cliOverrides);
                if (!string.IsNullOrWhiteSpace(cfg.ProjectRoot) &&
                    !string.Equals(cfg.ProjectRoot, workingDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    workingDirectory = cfg.ProjectRoot;
                    DirectoryUtils.SetBasePath(workingDirectory);
                }

                break;
            }
            catch (Exception envEx)
            {
                consoleService.Error($"Failed to load .xtraqconfig: {envEx.Message}");

                if (attemptedInit)
                {
                    return ExecuteResultEnum.Error;
                }

                var consent = consoleService.GetYesNo("Run xtraq init now?", isDefaultConfirmed: true);
                if (!consent)
                {
                    return ExecuteResultEnum.Error;
                }

                try
                {
                    await RunInitPipelineAsync(workingDirectory, consoleService).ConfigureAwait(false);
                    consoleService.Success("Init pipeline completed. Configure XTRAQ_GENERATOR_DB in your .env before re-running commands.");
                    attemptedInit = true;
                }
                catch (Exception initEx)
                {
                    consoleService.Error($"Init pipeline failed: {initEx.Message}");
                    return ExecuteResultEnum.Error;
                }
            }
        }

        var connectionString = cfg.GeneratorConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            consoleService.Error("Missing database connection string");
            consoleService.Output("\tSet XTRAQ_GENERATOR_DB in your .env (or supply the value via CLI overrides).");
            return ExecuteResultEnum.Error;
        }

        dbContext.SetConnectionString(connectionString);

        if (options.Verbose)
        {
            consoleService.Verbose($"[snapshot] Connection length={connectionString.Length}.");
            if (options.NoCache)
            {
                consoleService.Verbose("[snapshot] Cache disabled for this run (--no-cache).");
            }
        }

        var configuredSchemas = cfg.BuildSchemas ?? Array.Empty<string>();
        var resolutionRequest = new SnapshotResolutionRequest
        {
            ConnectionString = connectionString,
            ConfiguredSchemas = configuredSchemas,
            SkipPlanner = options.NoCache,
            Verbose = options.Verbose,
            ExistingResult = null
        };

        SnapshotResolutionPlan resolutionPlan;
        var planStopwatch = Stopwatch.StartNew();
        using (var planProgress = consoleService.BeginProgressScope("Planning schema refresh"))
        {
            resolutionPlan = await snapshotResolutionService.PrepareAsync(resolutionRequest, CancellationToken.None).ConfigureAwait(false);
            planStopwatch.Stop();
            var planned = resolutionPlan.InvalidationResult?.RefreshPlan.Sum(static b => b.Count) ?? 0;
            planProgress.Complete(message: $"schemas={resolutionPlan.EffectiveSchemas.Count} objects={planned}");
        }

        snapshotResolutionService.ApplyEnvironment(resolutionPlan);
        _lastSnapshotPlan = resolutionPlan;

        var effectiveSchemas = resolutionPlan.EffectiveSchemas ?? configuredSchemas;

        if (effectiveSchemas.Count > 0 && options.Verbose)
        {
            consoleService.Verbose($"[snapshot] Schema filter: {string.Join(", ", effectiveSchemas)}");
        }

        var procedureFilter = string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure.Trim();

        var snapshotOptions = new SnapshotBuildOptions
        {
            Schemas = effectiveSchemas,
            ProcedureWildcard = string.IsNullOrWhiteSpace(procedureFilter) ? null : procedureFilter,
            NoCache = options.NoCache,
            Verbose = options.Verbose
        };

        if (!string.IsNullOrWhiteSpace(snapshotOptions.ProcedureWildcard))
        {
            consoleService.Verbose($"[snapshot] Procedure filter: {snapshotOptions.ProcedureWildcard}");
        }

        var stopwatch = Stopwatch.StartNew();
        telemetryCollector.Reset();
        SnapshotBuildResult result;
        using var snapshotProgress = consoleService.BeginProgressScope("Capturing database schema (SnapshotBuilder)");
        try
        {
            result = await snapshotBuildOrchestrator.RunAsync(snapshotOptions).ConfigureAwait(false);
            snapshotProgress.Complete(message: $"analyzed={result.ProceduresAnalyzed} reused={result.ProceduresReused} written={result.FilesWritten}");
        }
        catch (SqlException sqlEx)
        {
            snapshotProgress.Complete(success: false, message: "Database error during snapshot build.");
            consoleService.Error($"Database error during snapshot build: {sqlEx.Message}");
            if (options.Verbose)
            {
                consoleService.Error(sqlEx.StackTrace ?? string.Empty);
            }
            return ExecuteResultEnum.Error;
        }
        catch (Exception ex)
        {
            snapshotProgress.Complete(success: false, message: "Snapshot builder failed.");
            consoleService.Error($"Snapshot builder failed: {ex.Message}");
            if (options.Verbose)
            {
                consoleService.Error(ex.StackTrace ?? string.Empty);
            }
            return ExecuteResultEnum.Error;
        }
        finally
        {
            stopwatch.Stop();
        }

        var selectedProcedures = result.ProceduresSelected ?? Array.Empty<SnapshotProcedureDescriptor>();
        var groupedBySchema = selectedProcedures
            .GroupBy(p => string.IsNullOrWhiteSpace(p.Schema) ? "(unknown)" : p.Schema, StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (groupedBySchema.Count == 0)
        {
            consoleService.Warn("No stored procedures matched the configured filters.");
        }
        else
        {
            var summary = string.Join(", ", groupedBySchema.Select(g => $"{g.Key}({g.Count()})"));
            consoleService.Info($"Captured {selectedProcedures.Count} stored procedures across {groupedBySchema.Count} schema(s): {summary}");

            var schemaSegments = new Dictionary<string, double>(groupedBySchema.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var group in groupedBySchema)
            {
                schemaSegments[group.Key] = group.Count();
            }

            RenderBreakdownChart(options, "Procedures per schema", schemaSegments, "items");
        }

        var collectMs = result.CollectDuration > TimeSpan.Zero ? result.CollectDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture) : null;
        var analyzeMs = result.AnalyzeDuration > TimeSpan.Zero ? result.AnalyzeDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture) : null;
        var writeMs = result.WriteDuration > TimeSpan.Zero ? result.WriteDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture) : null;
        var perPhaseSummary = string.Join(", ", new[]
        {
            collectMs is null ? null : $"collect={collectMs}ms",
            analyzeMs is null ? null : $"analyze={analyzeMs}ms",
            writeMs is null ? null : $"write={writeMs}ms"
        }.Where(static segment => segment is not null));

        if (!string.IsNullOrWhiteSpace(perPhaseSummary))
        {
            consoleService.Info($"Analyzed={result.ProceduresAnalyzed} reused={result.ProceduresReused} written={result.FilesWritten} unchanged={result.FilesUnchanged} in {stopwatch.ElapsedMilliseconds} ms ({perPhaseSummary}).");
        }
        else
        {
            consoleService.Info($"Analyzed={result.ProceduresAnalyzed} reused={result.ProceduresReused} written={result.FilesWritten} unchanged={result.FilesUnchanged} in {stopwatch.ElapsedMilliseconds} ms.");
        }

        var collectSeconds = result.CollectDuration.TotalSeconds;
        var analyzeSeconds = result.AnalyzeDuration.TotalSeconds;
        var writeSeconds = result.WriteDuration.TotalSeconds;
        consoleService.Info($"[snapshot] total={stopwatch.Elapsed.TotalSeconds:F2}s plan={planStopwatch.Elapsed.TotalSeconds:F2}s collect={collectSeconds:F2}s analyze={analyzeSeconds:F2}s write={writeSeconds:F2}s");

        var snapshotPhaseSegments = new Dictionary<string, double>(3, StringComparer.OrdinalIgnoreCase);
        if (result.CollectDuration > TimeSpan.Zero)
        {
            snapshotPhaseSegments["Collect"] = result.CollectDuration.TotalMilliseconds;
        }
        if (result.AnalyzeDuration > TimeSpan.Zero)
        {
            snapshotPhaseSegments["Analyze"] = result.AnalyzeDuration.TotalMilliseconds;
        }
        if (result.WriteDuration > TimeSpan.Zero)
        {
            snapshotPhaseSegments["Write"] = result.WriteDuration.TotalMilliseconds;
        }

        RenderBreakdownChart(options, "Xtraq summary • Snapshot duration (ms)", snapshotPhaseSegments, "ms");

        var procedureSourceSegments = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        if (result.ProceduresAnalyzed > 0)
        {
            procedureSourceSegments["Analyzed (DB)"] = result.ProceduresAnalyzed;
        }
        if (result.ProceduresReused > 0)
        {
            procedureSourceSegments["Reused (cache)"] = result.ProceduresReused;
        }
        if (result.ProceduresSkipped > 0)
        {
            procedureSourceSegments["Skipped"] = result.ProceduresSkipped;
        }
        if (procedureSourceSegments.Count > 0)
        {
            RenderBreakdownChart(options, "Xtraq summary • Procedure source", procedureSourceSegments, "procedures");
        }

        var artifactStateSegments = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        if (result.FilesWritten > 0)
        {
            artifactStateSegments["Updated artifacts"] = result.FilesWritten;
        }
        if (result.FilesUnchanged > 0)
        {
            artifactStateSegments["Up-to-date artifacts"] = result.FilesUnchanged;
        }
        if (artifactStateSegments.Count > 0)
        {
            RenderBreakdownChart(options, "Xtraq summary • Snapshot artifacts", artifactStateSegments, "files");
        }

        if (options.Telemetry)
        {
            try
            {
                var telemetryReport = telemetryCollector.CreateReport();
                var plannerTelemetry = CreatePlannerTelemetry(resolutionPlan, telemetryReport);
                telemetryReport = telemetryReport with { Planner = plannerTelemetry };
                var warmRun = plannerTelemetry?.WarmRun;
                var warmRunGoalMet = warmRun == true && telemetryReport.TotalQueries == 0;
                var warmRunQueryCount = warmRun == true ? telemetryReport.TotalQueries : (int?)null;
                _lastSnapshotSummary = new TelemetryRunSummary
                {
                    Command = "snapshot",
                    CompletedUtc = DateTimeOffset.UtcNow,
                    PlanDurationMilliseconds = planStopwatch.Elapsed.TotalMilliseconds,
                    DurationMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
                    TotalQueries = telemetryReport.TotalQueries,
                    FailedQueries = telemetryReport.FailedQueries,
                    Planner = telemetryReport.Planner,
                    WarmRun = warmRun,
                    WarmRunGoalMet = warmRun == true ? warmRunGoalMet : null,
                    WarmRunQueryCount = warmRunQueryCount,
                    Snapshot = new SnapshotRunSummary
                    {
                        EffectiveSchemaCount = effectiveSchemas.Count,
                        MissingSnapshotCount = resolutionPlan.MissingSnapshots.Count,
                        SelectedProcedureCount = selectedProcedures.Count,
                        ProcedureFilter = snapshotOptions.ProcedureWildcard,
                        PlanDurationMs = planStopwatch.Elapsed.TotalMilliseconds,
                        CollectDurationMs = result.CollectDuration.TotalMilliseconds,
                        AnalyzeDurationMs = result.AnalyzeDuration.TotalMilliseconds,
                        WriteDurationMs = result.WriteDuration.TotalMilliseconds
                    }
                };
                PrintTelemetrySummary(telemetryReport);

                var telemetryDirectory = Path.Combine(workingDirectory, ".xtraq", "telemetry");
                var fileName = $"snapshot-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json";
                await telemetryCollector.WriteReportAsync(telemetryReport, telemetryDirectory, fileName).ConfigureAwait(false);
            }
            catch (Exception telemetryEx)
            {
                _lastSnapshotSummary = null;
                consoleService.Verbose($"telemetry: persist failed reason={telemetryEx.Message}");
            }
        }
        else
        {
            consoleService.Verbose("telemetry: persistence skipped (enable --telemetry to collect statistics)");
            _lastSnapshotSummary = null;
        }

        return ExecuteResultEnum.Succeeded;
    }

    public Task<ExecuteResultEnum> BuildAsync(ICommandOptions options)
    {
        // Update debug output settings from command options
        DebugOutputHelper.UpdateFromOptions(options.Verbose, options.Debug);
        return RunWithWarningSummaryAsync(() => BuildCoreAsync(options));
    }

    private async Task<ExecuteResultEnum> BuildCoreAsync(ICommandOptions options)
    {
        if (options.Telemetry)
        {
            telemetryCollector.Reset();
        }
        var buildStart = DateTimeOffset.UtcNow;
        var buildStopwatch = Stopwatch.StartNew();
        var tableTypeDuration = TimeSpan.Zero;
        var procedureDuration = TimeSpan.Zero;
        var dbContextDuration = TimeSpan.Zero;
        var tableTypeArtifacts = 0;
        var procedureArtifacts = 0;
        var dbContextArtifacts = 0;
        var tableTypeDetails = new TableTypeGenerationResult(0, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));
        var procedureDetails = new ProcedureGenerationResult(0, new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));
        BuildTelemetryReport? buildTelemetry = null;
        DatabaseTelemetryReport? dbTelemetry = null;

        var workingDirectory = DirectoryUtils.GetWorkingDirectory();
        XtraqConfiguration cfg;
        try
        {
            var cliOverrides = BuildCliOverrides(options);
            cfg = XtraqConfiguration.Load(projectRoot: workingDirectory, cliOverrides: cliOverrides);
            if (!string.IsNullOrWhiteSpace(cfg.ProjectRoot) &&
                !string.Equals(cfg.ProjectRoot, workingDirectory, StringComparison.OrdinalIgnoreCase))
            {
                workingDirectory = cfg.ProjectRoot;
                DirectoryUtils.SetBasePath(workingDirectory);
            }
        }
        catch (Exception envEx)
        {
            consoleService.Error($"Failed to load .xtraqconfig: {envEx.Message}");
            consoleService.Output("run xtraq init?");
            return ExecuteResultEnum.Error;
        }

        if (!await EnsureSnapshotAsync(workingDirectory))
        {
            return ExecuteResultEnum.Error;
        }

        try
        {
            if (!string.IsNullOrWhiteSpace(cfg.GeneratorConnectionString))
            {
                dbContext.SetConnectionString(cfg.GeneratorConnectionString);
            }

            var configuredSchemas = cfg.BuildSchemas ?? Array.Empty<string>();
            var resolutionRequest = new SnapshotResolutionRequest
            {
                ConnectionString = cfg.GeneratorConnectionString,
                ConfiguredSchemas = configuredSchemas,
                SkipPlanner = options.NoCache,
                Verbose = options.Verbose,
                ExistingResult = options.NoCache ? null : _lastSnapshotPlan?.InvalidationResult
            };

            var resolutionPlan = await snapshotResolutionService.PrepareAsync(resolutionRequest, CancellationToken.None).ConfigureAwait(false);
            snapshotResolutionService.ApplyEnvironment(resolutionPlan);
            _lastSnapshotPlan = resolutionPlan;

            var effectiveSchemas = resolutionPlan.EffectiveSchemas ?? configuredSchemas;

            if (effectiveSchemas.Count > 0 && options.Verbose)
            {
                consoleService.Verbose($"[build] Schema filter: {string.Join(", ", effectiveSchemas)}");
            }

            var renderer = new SimpleTemplateEngine();
            var toolRoot = Directory.GetCurrentDirectory();
            var templatesDir = Path.Combine(toolRoot, "src", "Templates");
            ITemplateLoader loader = Directory.Exists(templatesDir)
                ? new FileSystemTemplateLoader(templatesDir)
                : new EmbeddedResourceTemplateLoader(typeof(XtraqCliRuntime).Assembly, "Xtraq.Templates.");

            Xtraq.Metadata.ISchemaMetadataProvider? schemaProvider = null;
            IReadOnlyList<Xtraq.Metadata.ProcedureDescriptor> procedures = Array.Empty<Xtraq.Metadata.ProcedureDescriptor>();
            try
            {
                schemaProvider = new SnapshotSchemaMetadataProvider(workingDirectory, consoleService);
                procedures = schemaProvider.GetProcedures();
                if (options.Verbose)
                {
                    consoleService.Verbose($"[build] Procedures available: {procedures.Count}");
                }
            }
            catch (Exception ex)
            {
                consoleService.Warn($"Failed to load procedure metadata: {ex.Message}");
                procedures = Array.Empty<Xtraq.Metadata.ProcedureDescriptor>();
            }

            HashSet<string>? requiredTableTypeReferences = null;
            if (procedures.Count > 0)
            {
                requiredTableTypeReferences = CollectRequiredTableTypeReferences(procedures, cfg.BuildSchemas ?? Array.Empty<string>());
                if (options.Verbose)
                {
                    consoleService.Verbose($"[build] Table type dependencies resolved: {requiredTableTypeReferences.Count}");
                }
            }

            var metadata = new TableTypeMetadataProvider(workingDirectory);
            var generator = new TableTypesGenerator(cfg, metadata, renderer, loader, workingDirectory);
            var tableTypesStopwatch = Stopwatch.StartNew();
            using var tableTypesProgress = consoleService.BeginProgressScope("Generating table type artifacts");
            try
            {
                tableTypeDetails = generator.Generate(requiredTableTypeReferences);
                tableTypeArtifacts = tableTypeDetails.TotalArtifacts;
                tableTypesProgress.Complete(message: $"artifacts={tableTypeArtifacts}");
            }
            catch
            {
                tableTypesProgress.Complete(success: false, message: "Table type generation failed.");
                throw;
            }
            finally
            {
                tableTypesStopwatch.Stop();
                tableTypeDuration = tableTypesStopwatch.Elapsed;
            }

            var outputDir = string.IsNullOrWhiteSpace(cfg.OutputDir) ? "Xtraq" : cfg.OutputDir.Trim();
            var outputRoot = Path.IsPathRooted(outputDir)
                ? Path.GetFullPath(outputDir)
                : Path.Combine(workingDirectory, outputDir);
            Directory.CreateDirectory(outputRoot);
            if (options.Verbose)
            {
                consoleService.Verbose($"[build] TableTypes output root: {outputRoot}");
            }
            consoleService.Output($"Generated {tableTypeArtifacts} table type artifact(s) into '{outputDir}'.");

            if (tableTypeDetails.ArtifactsPerSchema.Count > 0)
            {
                var tableTypeSegments = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in tableTypeDetails.ArtifactsPerSchema)
                {
                    if (kvp.Value > 0)
                    {
                        tableTypeSegments[kvp.Key] = kvp.Value;
                    }
                }

                if (tableTypeSegments.Count > 0)
                {
                    RenderBreakdownChart(options, "► Table type generation", tableTypeSegments, "files");
                }
            }
            if (procedures.Count == 0)
            {
                consoleService.Warn("No stored procedures found in snapshot metadata – skipping procedure generation.");
            }
            else
            {
                var namespaceResolver = new NamespaceResolver(cfg, msg => consoleService.Verbose($"[proc-ns] {msg}"));
                var nsRoot = namespaceResolver.Resolve(workingDirectory);
                if (string.IsNullOrWhiteSpace(nsRoot))
                {
                    nsRoot = "Xtraq";
                    consoleService.Warn("Namespace resolution returned empty value. Falling back to 'Xtraq'.");
                }

                static string ResolveNamespaceSegment(string? outputSetting)
                {
                    if (string.IsNullOrWhiteSpace(outputSetting))
                    {
                        return "Xtraq";
                    }

                    var trimmed = outputSetting.Trim().Trim('.', '/', '\\');
                    if (string.IsNullOrWhiteSpace(trimmed))
                    {
                        return "Xtraq";
                    }

                    var segments = trimmed.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);
                    var lastSegment = segments.LastOrDefault();
                    if (string.IsNullOrWhiteSpace(lastSegment))
                    {
                        return "Xtraq";
                    }

                    var sanitized = new string(lastSegment.Where(ch => char.IsLetterOrDigit(ch) || ch == '_').ToArray());
                    return string.IsNullOrWhiteSpace(sanitized) ? "Xtraq" : sanitized;
                }

                var nsSegment = ResolveNamespaceSegment(cfg.OutputDir);
                var finalNamespace = nsRoot.EndsWith('.' + nsSegment, StringComparison.OrdinalIgnoreCase)
                    ? nsRoot
                    : nsRoot + '.' + nsSegment;

                var procedureOutputRoot = Path.IsPathRooted(outputDir)
                    ? Path.GetFullPath(outputDir)
                    : Path.Combine(workingDirectory, outputDir);

                Directory.CreateDirectory(procedureOutputRoot);

                // Create JSON Function Enhancement Service
                var jsonEnhancementService = new Services.JsonFunctionEnhancementService(consoleService);

                var functionJsonDescriptors = schemaProvider?.GetFunctionJsonDescriptors() ?? Array.Empty<FunctionJsonDescriptor>();
                if (functionJsonDescriptors.Count > 0 && options.Verbose)
                {
                    consoleService.Verbose($"[build] Function JSON descriptors: {functionJsonDescriptors.Count}");
                }

                if (functionJsonDescriptors.Count > 0)
                {
                    var functionOutputGen = new FunctionJsonOutputGenerator(renderer, loader, cfg);
                    var functionArtifacts = functionOutputGen.Generate(finalNamespace, procedureOutputRoot, functionJsonDescriptors);
                    if (options.Verbose)
                    {
                        consoleService.Verbose($"[build] Generated {functionArtifacts} function output artifact(s).");
                    }
                }

                Func<string, string, FunctionJsonDescriptor?>? functionResolver = schemaProvider is null ? null : schemaProvider.TryGetFunctionJsonDescriptor;
                var proceduresGenerator = new ProceduresGenerator(renderer, () => procedures, loader, workingDirectory, cfg, jsonEnhancementService, functionJsonResolver: functionResolver);
                var proceduresStopwatch = Stopwatch.StartNew();
                using var proceduresProgress = consoleService.BeginProgressScope("Generating procedure artifacts");
                try
                {
                    procedureDetails = proceduresGenerator.Generate(finalNamespace, procedureOutputRoot);
                    procedureArtifacts = procedureDetails.TotalArtifacts;
                    proceduresProgress.Complete(message: $"artifacts={procedureArtifacts}");
                }
                catch
                {
                    proceduresProgress.Complete(success: false, message: "Procedure generation failed.");
                    throw;
                }
                finally
                {
                    proceduresStopwatch.Stop();
                    procedureDuration = proceduresStopwatch.Elapsed;
                }
                consoleService.Output($"Generated {procedureArtifacts} procedure artifact(s) into '{outputDir}'.");

                if (procedureDetails.ArtifactsPerSchema.Count > 0)
                {
                    var procedureSegments = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                    foreach (var kvp in procedureDetails.ArtifactsPerSchema)
                    {
                        if (kvp.Value > 0)
                        {
                            procedureSegments[kvp.Key] = kvp.Value;
                        }
                    }

                    if (procedureSegments.Count > 0)
                    {
                        RenderBreakdownChart(options, "► Procedure generation", procedureSegments, "files");
                    }
                }
            }

            if (procedures.Count > 0 && options.Verbose)
            {
                consoleService.Verbose($"[build] DbContext procedures available: {procedures.Count}");
            }

            var dbContextOutputService = new OutputService(consoleService);
            var dbContextGenerator = new DbContextGenerator(
                dbContextOutputService,
                consoleService,
                renderer,
                loader,
                () => procedures);

            var dbContextStopwatch = Stopwatch.StartNew();
            using var dbContextProgress = consoleService.BeginProgressScope("Generating DbContext artifacts");
            try
            {
                await dbContextGenerator.GenerateAsync(isDryRun: false).ConfigureAwait(false);
                dbContextProgress.Complete(message: "DbContext artifacts refreshed.");
            }
            catch
            {
                dbContextProgress.Complete(success: false, message: "DbContext generation failed.");
                throw;
            }
            finally
            {
                dbContextStopwatch.Stop();
                dbContextDuration = dbContextStopwatch.Elapsed;
            }
            consoleService.Output("DbContext artifacts updated under 'Xtraq'.");

            buildStopwatch.Stop();
            var totalElapsed = buildStopwatch.Elapsed;
            var buildEnd = buildStart + totalElapsed;
            var phaseSegments = new List<string>();
            if (tableTypeDuration > TimeSpan.Zero)
            {
                phaseSegments.Add($"tableTypes={tableTypeDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture)}ms");
            }
            if (procedureDuration > TimeSpan.Zero)
            {
                phaseSegments.Add($"procedures={procedureDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture)}ms");
            }
            if (dbContextDuration > TimeSpan.Zero)
            {
                phaseSegments.Add($"dbContext={dbContextDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture)}ms");
            }

            var totalElapsedMs = totalElapsed.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture);
            var suffix = phaseSegments.Count > 0 ? $" ({string.Join(", ", phaseSegments)})" : string.Empty;
            consoleService.Info($"[build] Completed in {totalElapsedMs} ms{suffix}.");

            var buildPhaseSegments = new Dictionary<string, double>(3, StringComparer.OrdinalIgnoreCase);
            if (tableTypeDuration > TimeSpan.Zero)
            {
                buildPhaseSegments["TableTypes"] = tableTypeDuration.TotalMilliseconds;
            }
            if (procedureDuration > TimeSpan.Zero)
            {
                buildPhaseSegments["Procedures"] = procedureDuration.TotalMilliseconds;
            }
            if (dbContextDuration > TimeSpan.Zero)
            {
                buildPhaseSegments["DbContext"] = dbContextDuration.TotalMilliseconds;
            }
            RenderBreakdownChart(options, "Xtraq summary • Build duration (ms)", buildPhaseSegments, "ms");

            var generatedFiles = (options.Telemetry || options.Verbose)
                ? CollectGeneratedFiles(outputRoot, buildStart)
                : new List<GeneratedFileTelemetry>();
            if (generatedFiles.Count > 0)
            {
                dbContextArtifacts = generatedFiles.Count(static f => f.RelativePath.Contains("DbContext", StringComparison.OrdinalIgnoreCase));
            }

            if (options.Verbose)
            {
                var artifactSummary = dbContextArtifacts > 0
                    ? $" dbContext={dbContextArtifacts}"
                    : string.Empty;
                consoleService.Verbose($"[build] Artifact counts: tableTypes={tableTypeArtifacts} procedures={procedureArtifacts}{artifactSummary}.");
            }

            var artifactSegments = new Dictionary<string, double>(3, StringComparer.OrdinalIgnoreCase);
            if (tableTypeArtifacts > 0)
            {
                artifactSegments["TableTypes"] = tableTypeArtifacts;
            }
            if (procedureArtifacts > 0)
            {
                artifactSegments["Procedures"] = procedureArtifacts;
            }
            if (dbContextArtifacts > 0)
            {
                artifactSegments["DbContext"] = dbContextArtifacts;
            }
            RenderBreakdownChart(options, "Xtraq summary • Generated artifacts", artifactSegments, "files");

            if (options.Telemetry)
            {
                try
                {
                    var telemetryDirectory = Path.Combine(workingDirectory, ".xtraq", "telemetry");
                    Directory.CreateDirectory(telemetryDirectory);

                    var phases = new List<BuildTelemetryPhase>
                    {
                        new()
                        {
                            Name = "TableTypes",
                            DurationMilliseconds = tableTypeDuration.TotalMilliseconds,
                            ArtifactCount = tableTypeArtifacts
                        },
                        new()
                        {
                            Name = "Procedures",
                            DurationMilliseconds = procedureDuration.TotalMilliseconds,
                            ArtifactCount = procedureArtifacts
                        },
                        new()
                        {
                            Name = "DbContext",
                            DurationMilliseconds = dbContextDuration.TotalMilliseconds,
                            ArtifactCount = dbContextArtifacts
                        }
                    };

                    buildTelemetry = BuildTelemetryReport.Create(
                        buildStart,
                        buildEnd,
                        outputRoot,
                        phases,
                        generatedFiles,
                        tableTypeArtifacts,
                        procedureArtifacts,
                        dbContextArtifacts);

                    dbTelemetry = telemetryCollector.CreateReport();
                    var plannerTelemetry = CreatePlannerTelemetry(resolutionPlan, dbTelemetry);
                    buildTelemetry = buildTelemetry with { Planner = plannerTelemetry };

                    consoleService.Verbose($"telemetry: build files captured={buildTelemetry.TotalFiles} categories={buildTelemetry.Categories.Count} largestSet={buildTelemetry.LargestFiles.Count}");

                    var fileName = $"build-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json";
                    var filePath = Path.Combine(telemetryDirectory, fileName);
                    await using var stream = File.Create(filePath);
                    var payload = new
                    {
                        buildTelemetry.StartTime,
                        buildTelemetry.EndTime,
                        Duration = buildTelemetry.Duration,
                        buildTelemetry.OutputRoot,
                        buildTelemetry.Phases,
                        buildTelemetry.Files,
                        Planner = buildTelemetry.Planner,
                        Summary = new
                        {
                            buildTelemetry.TotalFiles,
                            buildTelemetry.TotalBytes,
                            buildTelemetry.AverageFileBytes,
                            buildTelemetry.Categories,
                            buildTelemetry.LargestFiles,
                            buildTelemetry.TableTypeArtifacts,
                            buildTelemetry.ProcedureArtifacts,
                            buildTelemetry.DbContextArtifacts,
                            DatabaseQueries = new
                            {
                                Total = dbTelemetry.TotalQueries,
                                Failed = dbTelemetry.FailedQueries
                            }
                        }
                    };
                    await JsonSerializer.SerializeAsync(stream, payload, new JsonSerializerOptions
                    {
                        WriteIndented = true
                    }).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _lastBuildSummary = null;
                    dbTelemetry = null;
                    buildTelemetry = null;
                    consoleService.Verbose($"telemetry: persist failed reason={telemetryEx.Message}");
                }
            }
            else
            {
                consoleService.Verbose("telemetry: persistence skipped (enable --telemetry to collect statistics)");
                _lastBuildSummary = null;
                dbTelemetry = null;
                buildTelemetry = null;
            }

            if (buildTelemetry is not null && dbTelemetry is not null)
            {
                var phaseDurations = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                if (tableTypeDuration > TimeSpan.Zero)
                {
                    phaseDurations["TableTypes"] = tableTypeDuration.TotalMilliseconds;
                }
                if (procedureDuration > TimeSpan.Zero)
                {
                    phaseDurations["Procedures"] = procedureDuration.TotalMilliseconds;
                }
                if (dbContextDuration > TimeSpan.Zero)
                {
                    phaseDurations["DbContext"] = dbContextDuration.TotalMilliseconds;
                }

                var warmRun = buildTelemetry.Planner?.WarmRun;
                var warmRunGoalMet = warmRun == true && dbTelemetry.TotalQueries == 0;
                var warmRunQueryCount = warmRun == true ? dbTelemetry.TotalQueries : (int?)null;

                _lastBuildSummary = new TelemetryRunSummary
                {
                    Command = "build",
                    CompletedUtc = buildEnd,
                    DurationMilliseconds = totalElapsed.TotalMilliseconds,
                    TotalQueries = dbTelemetry.TotalQueries,
                    FailedQueries = dbTelemetry.FailedQueries,
                    Planner = buildTelemetry.Planner,
                    WarmRun = warmRun,
                    WarmRunGoalMet = warmRun == true ? warmRunGoalMet : null,
                    WarmRunQueryCount = warmRunQueryCount,
                    Build = new BuildRunSummary
                    {
                        PhaseDurationsMs = phaseDurations,
                        TableTypeArtifacts = tableTypeArtifacts,
                        ProcedureArtifacts = procedureArtifacts,
                        DbContextArtifacts = dbContextArtifacts,
                        GeneratedFileCount = generatedFiles.Count
                    }
                };
            }

            return ExecuteResultEnum.Succeeded;
        }
        catch (SqlException sqlEx)
        {
            consoleService.Error($"Database error during the build process: {sqlEx.Message}");
            if (options.Verbose)
            {
                consoleService.Error(sqlEx.StackTrace ?? string.Empty);
            }
            return ExecuteResultEnum.Error;
        }
        catch (Exception ex)
        {
            consoleService.Error($"Unexpected error during the build process: {ex.Message}");
            if (options.Verbose)
            {
                consoleService.Error(ex.StackTrace ?? string.Empty);
            }
            return ExecuteResultEnum.Error;
        }
    }

    public Task<ExecuteResultEnum> GetVersionAsync()
    {
        try
        {
            consoleService.Output($"Version: {service.InformationalVersion}");
            return Task.FromResult(ExecuteResultEnum.Succeeded);
        }
        finally
        {
            consoleService.FlushWarningsSummary();
        }
    }

    public Task<ExecuteResultEnum> UpdateAsync(ICommandOptions options)
    {
        // Update debug output settings from command options
        DebugOutputHelper.UpdateFromOptions(options.Verbose, options.Debug);
        return RunWithWarningSummaryAsync(() => UpdateCoreAsync(options));
    }

    private async Task<ExecuteResultEnum> UpdateCoreAsync(ICommandOptions options)
    {
        consoleService.PrintTitle("Updating Xtraq Global Tool");

        try
        {
            // Check current version first
            var updateInfo = await updateService.CheckForUpdateAsync().ConfigureAwait(false);
            if (updateInfo == null)
            {
                consoleService.Warn("Unable to check for updates. Please check your internet connection.");
                return ExecuteResultEnum.Error;
            }

            if (!updateInfo.IsUpdateAvailable)
            {
                consoleService.Output($"Xtraq is already up to date (version {updateInfo.CurrentVersion}).");
                return ExecuteResultEnum.Succeeded;
            }

            consoleService.Output($"Updating from {updateInfo.CurrentVersion} to {updateInfo.LatestVersion}...");

            var success = await updateService.UpdateAsync().ConfigureAwait(false);
            if (success)
            {
                consoleService.Output($"Successfully updated to version {updateInfo.LatestVersion}!");
                consoleService.Output("Please restart your terminal to use the updated version.");
                return ExecuteResultEnum.Succeeded;
            }
            else
            {
                consoleService.Error("Update failed. Please try updating manually using: dotnet tool update -g xtraq");
                return ExecuteResultEnum.Error;
            }
        }
        catch (Exception ex)
        {
            consoleService.Error($"Update failed with error: {ex.Message}");
            if (options.Verbose)
            {
                consoleService.Error(ex.StackTrace ?? string.Empty);
            }
            return ExecuteResultEnum.Error;
        }
    }

    /// <summary>
    /// Prints a concise telemetry summary to the console to highlight the recorded database activity.
    /// </summary>
    /// <param name="report">Telemetry report generated for the current snapshot run.</param>
    private void PrintTelemetrySummary(DatabaseTelemetryReport report)
    {
        var summarySegments = new List<string>
        {
            $"queries={report.TotalQueries}",
            $"failed={report.FailedQueries}",
            $"durationMs={report.TotalDuration.TotalMilliseconds.ToString("F0", CultureInfo.InvariantCulture)}",
            $"operations={report.Entries.Count}"
        };

        if (report.Planner is { WarmRun: bool warmRun })
        {
            summarySegments.Add($"warmRun={warmRun.ToString().ToLowerInvariant()}");
            if (warmRun)
            {
                var targetMet = report.TotalQueries == 0;
                summarySegments.Add($"warmRunTargetMet={targetMet.ToString().ToLowerInvariant()}");
            }
        }

        consoleService.Info($"telemetry: {string.Join(' ', summarySegments)}");

        if (report.Entries.Count == 0)
        {
            consoleService.Verbose("telemetry: no database queries were recorded.");
        }

        if (report.Planner is { WarmRun: true } && report.TotalQueries > 0)
        {
            consoleService.Warn($"telemetry: warmRun queries={report.TotalQueries} target=0");
        }

        var topOperation = report.TopOperations.FirstOrDefault();
        if (topOperation != null)
        {
            var category = string.IsNullOrWhiteSpace(topOperation.Category) ? "(uncategorized)" : topOperation.Category;
            consoleService.Verbose(
                $"telemetry/top: op={topOperation.Operation} category={category} count={topOperation.Count} totalMs={topOperation.TotalDuration.TotalMilliseconds:F0} maxMs={topOperation.MaxDuration.TotalMilliseconds:F0} failures={topOperation.Failures}");
        }

        if (report.CategoryDurations.Count > 1)
        {
            var categorySummary = string.Join(' ', report.CategoryDurations
                .OrderByDescending(static pair => pair.Value)
                .Select(static pair => $"{pair.Key}={pair.Value:F0}ms"));
            consoleService.Verbose($"telemetry/categories: {categorySummary}");
        }
    }

    private static HashSet<string> CollectRequiredTableTypeReferences(
        IEnumerable<ProcedureDescriptor> procedures,
        IReadOnlyList<string> allowedSchemas)
    {
        HashSet<string>? schemaFilter = allowedSchemas.Count > 0
            ? new HashSet<string>(allowedSchemas, StringComparer.OrdinalIgnoreCase)
            : null;

        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var procedure in procedures)
        {
            if (schemaFilter is { Count: > 0 } && !schemaFilter.Contains(procedure.Schema))
            {
                continue;
            }

            if (procedure.TableTypeParameters.Count == 0)
            {
                continue;
            }

            foreach (var parameter in procedure.TableTypeParameters)
            {
                if (string.IsNullOrWhiteSpace(parameter.TableTypeName))
                {
                    continue;
                }

                var candidates = new List<string?>
                {
                    parameter.NormalizedTypeReference,
                    TableTypeRefFormatter.Combine(parameter.TableTypeCatalog, parameter.TableTypeSchema, parameter.TableTypeName)
                };

                if (!string.IsNullOrWhiteSpace(parameter.TableTypeSchema))
                {
                    candidates.Add(TableTypeRefFormatter.Combine(parameter.TableTypeSchema, parameter.TableTypeName));
                }
                else
                {
                    candidates.Add(TableTypeRefFormatter.Combine(procedure.Schema, parameter.TableTypeName));
                }

                var anyAdded = false;
                foreach (var candidate in candidates)
                {
                    anyAdded |= AddTableTypeReference(required, candidate);
                }

                if (!anyAdded)
                {
                    AddTableTypeReference(required, parameter.TableTypeName);
                }
            }
        }

        return required;
    }

    private static bool AddTableTypeReference(ISet<string> collector, string? candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        var normalized = TableTypeRefFormatter.Normalize(candidate);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        var added = collector.Add(normalized);
        var segments = normalized.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (segments.Length == 3)
        {
            collector.Add(string.Join('.', segments[1], segments[2]));
        }

        return added;
    }

    private static IDictionary<string, string?>? BuildCliOverrides(ICommandOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        Dictionary<string, string?>? map = null;

        if (options.HasJsonIncludeNullValuesOverride)
        {
            map ??= new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            map["XTRAQ_JSON_INCLUDE_NULL_VALUES"] = options.JsonIncludeNullValues ? "1" : "0";
        }

        if (options.HasEntityFrameworkIntegrationOverride)
        {
            map ??= new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            map["XTRAQ_ENTITY_FRAMEWORK"] = options.EntityFrameworkIntegration ? "1" : "0";
        }

        return map;
    }

    /// <summary>
    /// Renders breakdown charts only when interactive console enhancements are enabled.
    /// </summary>
    private void RenderBreakdownChart(ICommandOptions options, string title, IReadOnlyDictionary<string, double>? segments, string? unit = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Charts removed from default output for readability.
    }

    private async Task<ExecuteResultEnum> RunWithWarningSummaryAsync(Func<Task<ExecuteResultEnum>> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        try
        {
            return await action().ConfigureAwait(false);
        }
        finally
        {
            consoleService.FlushWarningsSummary();
        }
    }


    private Task<bool> EnsureSnapshotAsync(string workingDirectory)
    {
        try
        {
            var schemaDir = Path.Combine(workingDirectory, ".xtraq", "snapshots");
            if (!Directory.Exists(schemaDir) || Directory.GetFiles(schemaDir, "*.json").Length == 0)
            {
                consoleService.Error("No snapshot found. Run 'xtraq snapshot' before 'xtraq build'.");
                consoleService.Output("\tUse 'xtraq build --refresh-snapshot' to refresh automatically.");
                return Task.FromResult(false);
            }
            return Task.FromResult(true);
        }
        catch (Exception)
        {
            consoleService.Error("Unable to verify snapshot presence.");
            return Task.FromResult(false);
        }
    }

    private static List<GeneratedFileTelemetry> CollectGeneratedFiles(string outputRoot, DateTimeOffset buildStartUtc)
    {
        var files = new List<GeneratedFileTelemetry>();
        if (string.IsNullOrWhiteSpace(outputRoot) || !Directory.Exists(outputRoot))
        {
            return files;
        }

        foreach (var file in Directory.EnumerateFiles(outputRoot, "*.cs", SearchOption.AllDirectories))
        {
            var writtenUtc = new DateTimeOffset(File.GetLastWriteTimeUtc(file), TimeSpan.Zero);
            if (writtenUtc < buildStartUtc)
            {
                continue;
            }

            var info = new FileInfo(file);
            var relative = Path.GetRelativePath(outputRoot, file).Replace('\\', '/');
            files.Add(new GeneratedFileTelemetry
            {
                RelativePath = relative,
                SizeBytes = info.Length,
                Category = CategorizeGeneratedFile(relative),
                WrittenAtUtc = writtenUtc
            });
        }

        return files;
    }

    private static string CategorizeGeneratedFile(string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
        {
            return "(root)";
        }

        var normalized = relativePath.Replace('\\', '/');
        var separatorIndex = normalized.IndexOf('/');
        return separatorIndex < 0 ? "(root)" : normalized[..separatorIndex];
    }

    private static PlannerRunTelemetry CreatePlannerTelemetry(SnapshotResolutionPlan plan, DatabaseTelemetryReport? telemetry)
    {
        if (plan is null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        var effectiveSchemas = plan.EffectiveSchemas?.ToArray() ?? Array.Empty<string>();

        return new PlannerRunTelemetry
        {
            PlannerExecuted = plan.PlannerExecuted,
            WarmRun = plan.WarmRun,
            ReusedExistingResult = plan.ReusedExistingResult,
            PlannerInvocationCount = plan.PlannerExecuted ? 1 : 0,
            RefreshPlanBatches = plan.InvalidationResult?.RefreshPlan.Count ?? 0,
            ObjectsToRefresh = plan.InvalidationResult?.ObjectsToRefresh.Count ?? 0,
            MissingSnapshotCount = plan.MissingSnapshots.Count,
            TotalQueryCount = telemetry?.TotalQueries ?? 0,
            FailedQueryCount = telemetry?.FailedQueries ?? 0,
            EffectiveSchemas = effectiveSchemas,
            PlanFilePath = plan.PlanFilePath
        };
    }

    public async Task PersistTelemetrySummaryAsync(string label, CancellationToken cancellationToken = default)
    {
        label = string.IsNullOrWhiteSpace(label) ? "session" : label.Trim();

        var runs = new List<TelemetryRunSummary>(capacity: 2);
        if (_lastSnapshotSummary is not null)
        {
            runs.Add(_lastSnapshotSummary);
        }

        if (_lastBuildSummary is not null)
        {
            runs.Add(_lastBuildSummary);
        }

        if (runs.Count == 0)
        {
            consoleService.Verbose("telemetry: no run summaries available; skipping summary export");
            return;
        }

        var projectRoot = DirectoryUtils.GetWorkingDirectory();
        var summaryDir = Path.Combine(projectRoot, ".xtraq", "telemetry");
        Directory.CreateDirectory(summaryDir);

        var warmRunCount = runs.Count(static run => run.WarmRun == true);
        var warmRunGoalMetCount = runs.Count(static run => run.WarmRunGoalMet == true);
        double? warmRunSuccessRate = warmRunCount == 0 ? null : warmRunGoalMetCount / (double)warmRunCount;

        var document = new TelemetrySummaryDocument
        {
            Label = label,
            GeneratedUtc = DateTime.UtcNow,
            RunCount = runs.Count,
            TotalQueries = runs.Sum(static run => run.TotalQueries),
            FailedQueries = runs.Sum(static run => run.FailedQueries),
            WarmRunCount = warmRunCount,
            WarmRunTargetMetCount = warmRunGoalMetCount,
            WarmRunSuccessRate = warmRunSuccessRate,
            Runs = runs
        };

        var fileName = $"summary-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json";
        var filePath = Path.Combine(summaryDir, fileName);

        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, document, new JsonSerializerOptions
        {
            WriteIndented = true
        }, cancellationToken).ConfigureAwait(false);

        var displayPath = Path.Combine(".xtraq", "telemetry", fileName).Replace('\\', '/');

        consoleService.Info($"telemetry: summary written path={displayPath}");

        _lastSnapshotSummary = null;
        _lastBuildSummary = null;
    }

    private sealed class TelemetryRunSummary
    {
        public string Command { get; init; } = string.Empty;

        public DateTimeOffset CompletedUtc { get; init; }

        public double? PlanDurationMilliseconds { get; init; }

        public double? DurationMilliseconds { get; init; }

        public int TotalQueries { get; init; }

        public int FailedQueries { get; init; }

        public bool? WarmRun { get; init; }

        public bool? WarmRunGoalMet { get; init; }

        public int? WarmRunQueryCount { get; init; }

        public PlannerRunTelemetry? Planner { get; init; }

        public SnapshotRunSummary? Snapshot { get; init; }

        public BuildRunSummary? Build { get; init; }
    }

    private sealed class SnapshotRunSummary
    {
        public int EffectiveSchemaCount { get; init; }

        public int MissingSnapshotCount { get; init; }

        public int SelectedProcedureCount { get; init; }

        public string? ProcedureFilter { get; init; }

        public double? PlanDurationMs { get; init; }

        public double? CollectDurationMs { get; init; }

        public double? AnalyzeDurationMs { get; init; }

        public double? WriteDurationMs { get; init; }
    }

    private sealed class BuildRunSummary
    {
        public IReadOnlyDictionary<string, double> PhaseDurationsMs { get; init; } = new Dictionary<string, double>();

        public int TableTypeArtifacts { get; init; }

        public int ProcedureArtifacts { get; init; }

        public int DbContextArtifacts { get; init; }

        public int GeneratedFileCount { get; init; }
    }

    private sealed class TelemetrySummaryDocument
    {
        public string Label { get; init; } = string.Empty;

        public DateTime GeneratedUtc { get; init; }

        public int RunCount { get; init; }

        public int TotalQueries { get; init; }

        public int FailedQueries { get; init; }

        public int WarmRunCount { get; init; }

        public int WarmRunTargetMetCount { get; init; }

        public double? WarmRunSuccessRate { get; init; }

        public IReadOnlyList<TelemetryRunSummary> Runs { get; init; } = Array.Empty<TelemetryRunSummary>();
    }
    private static async Task RunInitPipelineAsync(string projectRoot, IConsoleService consoleService)
    {
        var resolvedRoot = string.IsNullOrWhiteSpace(projectRoot) ? Directory.GetCurrentDirectory() : projectRoot;
        Directory.CreateDirectory(resolvedRoot);

        var envPath = await ProjectEnvironmentBootstrapper.EnsureEnvAsync(resolvedRoot, autoApprove: true).ConfigureAwait(false);
        var examplePath = ProjectEnvironmentBootstrapper.EnsureEnvExample(resolvedRoot);
        ProjectEnvironmentBootstrapper.EnsureProjectGitignore(resolvedRoot);

        consoleService.Output($"[xtraq init] .env ready at {envPath}");
        consoleService.Output($"[xtraq init] Template available at {examplePath}");
    }
}


