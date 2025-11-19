using Xtraq.Cache;
using Xtraq.Utils;

namespace Xtraq.Services;

/// <summary>
/// Coordinates cache invalidation results with snapshot hydration so CLI commands can target
/// only the schemas that require refresh. Persists the refresh plan for telemetry/debugging.
/// </summary>
internal interface ISnapshotResolutionService
{
    /// <summary>
    /// Builds a resolution plan combining cache invalidation analysis and configured schema filters.
    /// </summary>
    /// <param name="request">Resolution context containing connection and configuration details.</param>
    /// <param name="cancellationToken">Token to observe while running the planner pipeline.</param>
    /// <returns>Plan describing the effective schemas and invalidation outcome.</returns>
    Task<SnapshotResolutionPlan> PrepareAsync(SnapshotResolutionRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies the resolved schema scope to process environment variables for downstream tooling.
    /// </summary>
    /// <param name="plan">Resolution plan produced by <see cref="PrepareAsync"/>.</param>
    void ApplyEnvironment(SnapshotResolutionPlan plan);
}

/// <summary>
/// Immutable request payload for snapshot resolution.
/// </summary>
internal sealed class SnapshotResolutionRequest
{
    public string? ConnectionString { get; init; }

    public IReadOnlyList<string> ConfiguredSchemas { get; init; } = Array.Empty<string>();

    public bool SkipPlanner { get; init; }

    public bool Verbose { get; init; }

    public SchemaInvalidationResult? ExistingResult { get; init; }
}

/// <summary>
/// Result of snapshot resolution, describing how the CLI should scope refresh operations.
/// </summary>
internal sealed class SnapshotResolutionPlan
{
    public IReadOnlyList<string> ConfiguredSchemas { get; init; } = Array.Empty<string>();

    public IReadOnlyList<string> EffectiveSchemas { get; init; } = Array.Empty<string>();

    public SchemaInvalidationResult? InvalidationResult { get; init; }

    public bool PlannerExecuted { get; init; }

    public bool ReusedExistingResult { get; init; }

    public bool WarmRun { get; init; }

    public IReadOnlyList<SchemaObjectRef> MissingSnapshots { get; init; } = Array.Empty<SchemaObjectRef>();

    public string? PlanFilePath { get; init; }
}

internal sealed class SnapshotResolutionService : ISnapshotResolutionService
{
    private readonly ISchemaInvalidationOrchestrator _invalidationOrchestrator;
    private readonly IConsoleService _console;

    public SnapshotResolutionService(ISchemaInvalidationOrchestrator invalidationOrchestrator, IConsoleService console)
    {
        _invalidationOrchestrator = invalidationOrchestrator ?? throw new ArgumentNullException(nameof(invalidationOrchestrator));
        _console = console ?? throw new ArgumentNullException(nameof(console));
    }

    /// <inheritdoc />
    public async Task<SnapshotResolutionPlan> PrepareAsync(SnapshotResolutionRequest request, CancellationToken cancellationToken = default)
    {
        if (request is null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var configuredSchemas = request.ConfiguredSchemas ?? Array.Empty<string>();
        var effectiveSchemas = configuredSchemas;
        SchemaInvalidationResult? invalidationResult = request.ExistingResult;
        var plannerExecuted = false;
        var reusedExisting = invalidationResult is not null;

        if (!request.SkipPlanner && string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            _console.Verbose("[schema-invalidation] Skipping planner (missing connection string).");
            return new SnapshotResolutionPlan
            {
                ConfiguredSchemas = configuredSchemas,
                EffectiveSchemas = effectiveSchemas,
                PlannerExecuted = false,
                ReusedExistingResult = reusedExisting
            };
        }

        if (!request.SkipPlanner && invalidationResult is null && !string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            await _invalidationOrchestrator.InitializeAsync(request.ConnectionString!, cancellationToken).ConfigureAwait(false);
            invalidationResult = await _invalidationOrchestrator.AnalyzeAndInvalidateAsync(configuredSchemas, cancellationToken).ConfigureAwait(false);
            plannerExecuted = true;
        }

        if (invalidationResult is not null && invalidationResult.RefreshPlan.Count > 0)
        {
            var plannedSchemas = invalidationResult.RefreshPlan
                .Select(static batch => batch.Schema)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (configuredSchemas.Count > 0)
            {
                var configuredSet = new HashSet<string>(configuredSchemas, StringComparer.OrdinalIgnoreCase);
                plannedSchemas.RemoveAll(schema => !configuredSet.Contains(schema));

                if (plannedSchemas.Count == 0 && configuredSchemas.Count > 0)
                {
                    plannedSchemas.AddRange(configuredSchemas);
                }
            }

            if (plannedSchemas.Count > 0)
            {
                effectiveSchemas = plannedSchemas;
            }
        }

        var missingSnapshots = CalculateMissingSnapshots(invalidationResult);
        string? planFilePath = null;

        if (invalidationResult is not null)
        {
            if (invalidationResult.RefreshPlan.Count > 0)
            {
                EmitRefreshPlanSummary(invalidationResult, plannerExecuted, request.Verbose, reusedExisting);
            }

            planFilePath = PersistRefreshPlan(invalidationResult, effectiveSchemas);
        }

        var warmRun = reusedExisting && !plannerExecuted && invalidationResult is not null;

        if (missingSnapshots.Count > 0 && request.Verbose)
        {
            var summary = string.Join(", ", missingSnapshots.Select(static obj => obj.FullName));
            _console.Verbose($"[schema-invalidation] Missing procedure snapshots detected ({missingSnapshots.Count}): {summary}");
        }

        return new SnapshotResolutionPlan
        {
            ConfiguredSchemas = configuredSchemas,
            EffectiveSchemas = effectiveSchemas,
            InvalidationResult = invalidationResult,
            PlannerExecuted = plannerExecuted,
            ReusedExistingResult = reusedExisting,
            WarmRun = warmRun,
            MissingSnapshots = missingSnapshots,
            PlanFilePath = planFilePath
        };
    }

    /// <inheritdoc />
    public void ApplyEnvironment(SnapshotResolutionPlan plan)
    {
        if (plan is null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (plan.EffectiveSchemas == null || plan.EffectiveSchemas.Count == 0)
        {
            Environment.SetEnvironmentVariable("XTRAQ_BUILD_SCHEMAS", null);
            return;
        }

        var value = string.Join(',', plan.EffectiveSchemas);
        Environment.SetEnvironmentVariable("XTRAQ_BUILD_SCHEMAS", value);
    }

    private static List<SchemaObjectRef> CalculateMissingSnapshots(SchemaInvalidationResult? result)
    {
        if (result == null || result.ObjectsToRefresh.Count == 0)
        {
            return new List<SchemaObjectRef>();
        }

        var missing = new List<SchemaObjectRef>();
        var projectRoot = ProjectRootResolver.ResolveCurrent();
        var schemaRoot = Path.Combine(projectRoot, ".xtraq", "snapshots", "procedures");

        foreach (var obj in result.ObjectsToRefresh)
        {
            if (obj.Type != SchemaObjectType.StoredProcedure)
            {
                continue;
            }

            var path = Path.Combine(schemaRoot, obj.Schema, obj.Name + ".json");
            if (!File.Exists(path))
            {
                missing.Add(obj);
            }
        }

        return missing;
    }

    private void EmitRefreshPlanSummary(SchemaInvalidationResult result, bool plannerExecuted, bool verbose, bool reusedExisting)
    {
        if (result.RefreshPlan.Count == 0)
        {
            return;
        }

        var totalObjects = result.ObjectsToRefresh.Count;
        var segments = new List<string>(result.RefreshPlan.Count);

        foreach (var batch in result.RefreshPlan)
        {
            var modifiedCount = batch.Entries.Count(static entry => entry.Reason == SchemaRefreshReason.Modified);
            var dependencyCount = batch.Count - modifiedCount;
            segments.Add($"{batch.Schema}({batch.Count}; modified={modifiedCount}; dependency={dependencyCount})");
        }

        var summary = string.Join(", ", segments);
        var warmRun = reusedExisting && !plannerExecuted;

        if (plannerExecuted)
        {
            _console.Info($"[schema-invalidation] Refresh plan scheduled {totalObjects} object(s) across {result.RefreshPlan.Count} schema batch(es): {summary}");
            return;
        }

        if (warmRun)
        {
            _console.Info($"[schema-invalidation] Warm run detected – reused cached refresh plan without database scan: {summary}");
            return;
        }

        if (verbose)
        {
            if (reusedExisting)
            {
                _console.Verbose($"[schema-invalidation] Reusing existing refresh plan -> {summary}");
                return;
            }
            _console.Verbose($"[schema-invalidation] Refresh plan summary: {summary}");
        }
    }

    private static string? PersistRefreshPlan(SchemaInvalidationResult result, IReadOnlyList<string> effectiveSchemas)
    {
        try
        {
            var projectRoot = ProjectRootResolver.ResolveCurrent();
            var cacheDir = Path.Combine(projectRoot, ".xtraq", "cache");
            Directory.CreateDirectory(cacheDir);
            var planPath = Path.Combine(cacheDir, "schema-refresh-plan.json");

            var document = new SchemaRefreshPlanDocument
            {
                GeneratedUtc = DateTime.UtcNow,
                Schemas = effectiveSchemas?.ToArray() ?? Array.Empty<string>(),
                ModifiedCount = result.ModifiedObjects.Count,
                DependencyCount = result.InvalidatedObjects.Count,
                RemovedCount = result.RemovedObjects.Count,
                SkippedCount = result.SkippedObjects.Count,
                Batches = result.RefreshPlan
                    .Select(batch => new SchemaRefreshPlanBatchDocument
                    {
                        Schema = batch.Schema,
                        Entries = batch.Entries
                            .Select(entry => new SchemaRefreshPlanEntryDocument
                            {
                                Object = entry.Object.FullName,
                                Type = entry.Object.Type.ToString(),
                                Reason = entry.Reason.ToString()
                            })
                            .ToList()
                    })
                    .ToList()
            };

            var json = JsonSerializer.Serialize(document, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(planPath, json + Environment.NewLine);
            return planPath;
        }
        catch
        {
            return null;
        }
    }

    private sealed class SchemaRefreshPlanDocument
    {
        public int Version { get; init; } = 1;
        public DateTime GeneratedUtc { get; init; }
        public IReadOnlyList<string> Schemas { get; init; } = Array.Empty<string>();
        public int ModifiedCount { get; init; }
        public int DependencyCount { get; init; }
        public int RemovedCount { get; init; }
        public int SkippedCount { get; init; }
        public IReadOnlyList<SchemaRefreshPlanBatchDocument> Batches { get; init; } = Array.Empty<SchemaRefreshPlanBatchDocument>();
    }

    private sealed class SchemaRefreshPlanBatchDocument
    {
        public string Schema { get; init; } = string.Empty;
        public IReadOnlyList<SchemaRefreshPlanEntryDocument> Entries { get; init; } = Array.Empty<SchemaRefreshPlanEntryDocument>();
    }

    private sealed class SchemaRefreshPlanEntryDocument
    {
        public string Object { get; init; } = string.Empty;
        public string Type { get; init; } = string.Empty;
        public string Reason { get; init; } = string.Empty;
    }
}
