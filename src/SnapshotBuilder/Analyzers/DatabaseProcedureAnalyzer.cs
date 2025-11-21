using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Metadata;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Analyzes stored procedures by rebuilding AST models, enriching metadata, and resolving dependencies.
/// </summary>
internal sealed class DatabaseProcedureAnalyzer : IProcedureAnalyzer
{
    private readonly DbContext _dbContext;
    private readonly IConsoleService _console;
    private readonly IDependencyMetadataProvider _dependencyMetadataProvider;
    private readonly IProcedureAstBuilder _procedureAstBuilder;
    private readonly IProcedureMetadataEnricher _metadataEnricher;
    private Dictionary<string, string>? _definitionCache;
    private Dictionary<string, List<StoredProcedureInput>>? _parameterCache;

    /// <summary>
    /// Initializes a new instance of the <see cref="DatabaseProcedureAnalyzer"/> class.
    /// </summary>
    public DatabaseProcedureAnalyzer(
        DbContext dbContext,
        IConsoleService console,
        IDependencyMetadataProvider dependencyMetadataProvider,
        IProcedureAstBuilder procedureAstBuilder,
        IProcedureMetadataEnricher metadataEnricher)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _dependencyMetadataProvider = dependencyMetadataProvider ?? throw new ArgumentNullException(nameof(dependencyMetadataProvider));
        _procedureAstBuilder = procedureAstBuilder ?? throw new ArgumentNullException(nameof(procedureAstBuilder));
        _metadataEnricher = metadataEnricher ?? throw new ArgumentNullException(nameof(metadataEnricher));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ProcedureAnalysisResult>> AnalyzeAsync(
        IReadOnlyList<ProcedureCollectionItem> items,
        SnapshotBuildOptions options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (items == null || items.Count == 0)
        {
            return Array.Empty<ProcedureAnalysisResult>();
        }

        options ??= SnapshotBuildOptions.Default;
        var results = new List<ProcedureAnalysisResult>(items.Count);

        await PrefetchProcedureMetadataAsync(items, cancellationToken).ConfigureAwait(false);

        await _console.RunProgressAsync("Analyzing stored procedures", items.Count, async advance =>
        {
            foreach (var item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    if (item == null)
                    {
                        continue;
                    }

                    var result = await AnalyzeProcedureAsync(item, options, cancellationToken).ConfigureAwait(false);
                    if (result != null)
                    {
                        results.Add(result);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    var descriptorLabel = FormatProcedureLabel(item?.Descriptor);
                    _console.Error($"[snapshot-analyze] Unhandled error analyzing {descriptorLabel}: {ex.Message}");
                }
                finally
                {
                    advance(1d);
                }
            }
        }, cancellationToken).ConfigureAwait(false);

        return results;
    }

    /// <summary>
    /// Performs detailed analysis for a single procedure collection entry.
    /// </summary>
    private async Task<ProcedureAnalysisResult?> AnalyzeProcedureAsync(
        ProcedureCollectionItem item,
        SnapshotBuildOptions options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (item.Decision != ProcedureCollectionDecision.Analyze)
        {
            return null;
        }

        var descriptor = item.Descriptor ?? new ProcedureDescriptor();
        var descriptorLabel = FormatProcedureLabel(descriptor);
        var snapshotFile = DetermineSnapshotFileName(item, descriptor);

        var definition = await LoadDefinitionAsync(descriptor, descriptorLabel, cancellationToken).ConfigureAwait(false);
        var parameters = await LoadParametersAsync(descriptor, descriptorLabel, cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(definition))
        {
            return BuildAnalysisResult(descriptor, null, parameters, item.CachedDependencies, item.LastModifiedUtc, snapshotFile, reusedFromCache: false);
        }

        var procedure = BuildProcedureModel(descriptor, descriptorLabel, definition, options?.Verbose ?? false);
        if (procedure == null)
        {
            return BuildAnalysisResult(descriptor, null, parameters, item.CachedDependencies, item.LastModifiedUtc, snapshotFile, reusedFromCache: false);
        }

        await EnrichProcedureMetadataAsync(descriptor, descriptorLabel, procedure, snapshotFile, cancellationToken).ConfigureAwait(false);
        EmitJsonWithoutArrayWarnings(descriptor, procedure);

        var dependencies = await ResolveDependenciesAsync(
            descriptorLabel,
            descriptor,
            procedure,
            parameters,
            item.CachedDependencies,
            cancellationToken).ConfigureAwait(false);

        return BuildAnalysisResult(descriptor, procedure, parameters, dependencies, item.LastModifiedUtc, snapshotFile, reusedFromCache: false);
    }

    /// <summary>
    /// Loads the stored procedure definition from the database.
    /// </summary>
    private async Task<string?> LoadDefinitionAsync(
        ProcedureDescriptor descriptor,
        string descriptorLabel,
        CancellationToken cancellationToken)
    {
        var cacheKey = BuildProcedureKey(descriptor.Schema, descriptor.Name);
        if (_definitionCache != null && cacheKey != null && _definitionCache.TryGetValue(cacheKey, out var cachedDefinition))
        {
            return cachedDefinition;
        }

        try
        {
            return await _dbContext.StoredProcedureContentAsync(
                descriptor.Schema,
                descriptor.Name,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.WarnBuffered(
                $"[snapshot-analyze] Failed to load definition for {descriptorLabel}",
                $"Exception encountered while retrieving the procedure definition: {ex}");
            return null;
        }
    }

    /// <summary>
    /// Loads the parameter metadata for the given procedure.
    /// </summary>
    private async Task<List<StoredProcedureInput>> LoadParametersAsync(
        ProcedureDescriptor descriptor,
        string descriptorLabel,
        CancellationToken cancellationToken)
    {
        var cacheKey = BuildProcedureKey(descriptor.Schema, descriptor.Name);
        if (_parameterCache != null && cacheKey != null && _parameterCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        try
        {
            var loaded = await _dbContext.StoredProcedureInputListAsync(
                descriptor.Schema,
                descriptor.Name,
                cancellationToken).ConfigureAwait(false);

            if (loaded != null && loaded.Count > 0)
            {
                return loaded;
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] Parameter discovery failed for {descriptorLabel}: {ex.Message}");
        }

        return new List<StoredProcedureInput>();
    }

    /// <summary>
    /// Creates and post-processes the procedure model for the supplied SQL definition.
    /// </summary>
    private ProcedureModel? BuildProcedureModel(
        ProcedureDescriptor descriptor,
        string descriptorLabel,
        string definition,
        bool verboseParsing)
    {
        ProcedureModel? procedure = null;

        try
        {
            var request = new ProcedureAstBuildRequest(definition, descriptor.Schema, descriptor.Catalog, verboseParsing);
            procedure = _procedureAstBuilder.Build(request);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] AST build failed for {descriptorLabel}: {ex.Message}");
            return null;
        }

        if (procedure == null)
        {
            _console.WarnBuffered(
                $"[snapshot-analyze] Unable to parse stored procedure {descriptorLabel}",
                "Procedure AST builder returned null; snapshot will contain placeholder metadata.");
            return null;
        }

        var fragment = ProcedureModelScriptDomParser.Parse(definition);
        if (fragment != null)
        {
            ProcedureModelExecAnalyzer.Apply(fragment, procedure);
            ProcedureModelJsonAnalyzer.Apply(fragment, procedure, definition);
            ProcedureModelAggregateAnalyzer.Apply(fragment, procedure);
        }
        else
        {
            ProcedureModelExecAnalyzer.Apply(definition, procedure);
            ProcedureModelJsonAnalyzer.Apply(definition, procedure);
            ProcedureModelAggregateAnalyzer.Apply(definition, procedure);
        }

        ProcedureModelPostProcessor.Apply(procedure);
        return procedure;
    }

    /// <summary>
    /// Enriches the procedure metadata with schema information.
    /// </summary>
    private async Task EnrichProcedureMetadataAsync(
        ProcedureDescriptor descriptor,
        string descriptorLabel,
        ProcedureModel procedure,
        string snapshotFile,
        CancellationToken cancellationToken)
    {
        try
        {
            var request = new ProcedureMetadataEnrichmentRequest(descriptor, procedure, snapshotFile);
            await _metadataEnricher.EnrichAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] Metadata enrichment failed for {descriptorLabel}: {ex.Message}");
        }
    }

    /// <summary>
    /// Resolves dependencies for the analyzed procedure, falling back to cached values if resolution fails.
    /// </summary>
    private async Task<IReadOnlyList<ProcedureDependency>> ResolveDependenciesAsync(
        string descriptorLabel,
        ProcedureDescriptor descriptor,
        ProcedureModel? procedure,
        IReadOnlyList<StoredProcedureInput> parameters,
        IReadOnlyList<ProcedureDependency> cachedDependencies,
        CancellationToken cancellationToken)
    {
        var accumulator = new Dictionary<string, ProcedureDependency>(StringComparer.OrdinalIgnoreCase);

        CollectParameterDependencies(parameters, descriptor, accumulator);
        CollectExecutedProcedureDependencies(procedure, descriptor, accumulator);
        CollectResultSetDependencies(procedure, descriptor, accumulator);

        if (accumulator.Count == 0)
        {
            return cachedDependencies ?? Array.Empty<ProcedureDependency>();
        }

        try
        {
            var resolved = await _dependencyMetadataProvider.ResolveAsync(accumulator.Values, cancellationToken).ConfigureAwait(false);
            if (resolved == null || resolved.Count == 0)
            {
                return cachedDependencies ?? Array.Empty<ProcedureDependency>();
            }

            return resolved;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] Dependency resolution failed for {descriptorLabel}: {ex.Message}");
            return cachedDependencies ?? Array.Empty<ProcedureDependency>();
        }
    }

    /// <summary>
    /// Captures dependencies introduced through procedure parameters.
    /// </summary>
    private static void CollectParameterDependencies(
        IReadOnlyList<StoredProcedureInput> parameters,
        ProcedureDescriptor descriptor,
        IDictionary<string, ProcedureDependency> accumulator)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var parameter in parameters)
        {
            if (parameter == null || string.IsNullOrWhiteSpace(parameter.UserTypeName))
            {
                continue;
            }

            var schema = string.IsNullOrWhiteSpace(parameter.UserTypeSchemaName) ? descriptor.Schema : parameter.UserTypeSchemaName;
            var kind = parameter.IsTableType ? ProcedureDependencyKind.UserDefinedTableType : ProcedureDependencyKind.UserDefinedType;
            AddDependency(accumulator, kind, schema, parameter.UserTypeName, descriptor.Catalog);
        }
    }

    /// <summary>
    /// Captures dependencies from EXEC statements.
    /// </summary>
    private static void CollectExecutedProcedureDependencies(
        ProcedureModel? procedure,
        ProcedureDescriptor descriptor,
        IDictionary<string, ProcedureDependency> accumulator)
    {
        if (procedure?.ExecutedProcedures == null)
        {
            return;
        }

        foreach (var executed in procedure.ExecutedProcedures)
        {
            if (executed == null || string.IsNullOrWhiteSpace(executed.Name))
            {
                continue;
            }

            var schema = string.IsNullOrWhiteSpace(executed.Schema) ? descriptor.Schema : executed.Schema;
            var catalog = string.IsNullOrWhiteSpace(executed.Catalog) ? descriptor.Catalog : executed.Catalog;
            AddDependency(accumulator, ProcedureDependencyKind.Procedure, schema, executed.Name, catalog);
        }
    }

    /// <summary>
    /// Captures dependencies from result-set projections.
    /// </summary>
    private static void CollectResultSetDependencies(
        ProcedureModel? procedure,
        ProcedureDescriptor descriptor,
        IDictionary<string, ProcedureDependency> accumulator)
    {
        if (procedure?.ResultSets == null)
        {
            return;
        }

        foreach (var resultSet in procedure.ResultSets)
        {
            if (resultSet == null)
            {
                continue;
            }

            if (resultSet.Reference != null)
            {
                var mapped = MapReferenceKind(resultSet.Reference.Kind);
                if (mapped.HasValue)
                {
                    var schema = string.IsNullOrWhiteSpace(resultSet.Reference.Schema) ? descriptor.Schema : resultSet.Reference.Schema;
                    var catalog = string.IsNullOrWhiteSpace(resultSet.Reference.Catalog) ? descriptor.Catalog : resultSet.Reference.Catalog;
                    AddDependency(accumulator, mapped.Value, schema, resultSet.Reference.Name, catalog);
                }
            }

            CollectColumnDependencies(resultSet.Columns, descriptor, accumulator);
        }
    }

    /// <summary>
    /// Traverses nested columns to capture referenced objects and user-defined types.
    /// </summary>
    private static void CollectColumnDependencies(
        IReadOnlyList<ProcedureResultColumn>? columns,
        ProcedureDescriptor descriptor,
        IDictionary<string, ProcedureDependency> accumulator)
    {
        if (columns == null)
        {
            return;
        }

        foreach (var column in columns)
        {
            if (column == null)
            {
                continue;
            }

            if (column.Reference != null)
            {
                var mapped = MapReferenceKind(column.Reference.Kind);
                if (mapped.HasValue)
                {
                    var schema = string.IsNullOrWhiteSpace(column.Reference.Schema) ? descriptor.Schema : column.Reference.Schema;
                    var catalog = string.IsNullOrWhiteSpace(column.Reference.Catalog) ? descriptor.Catalog : column.Reference.Catalog;
                    AddDependency(accumulator, mapped.Value, schema, column.Reference.Name, catalog);
                }
            }

            if (!string.IsNullOrWhiteSpace(column.UserTypeName))
            {
                var schema = string.IsNullOrWhiteSpace(column.UserTypeSchemaName) ? descriptor.Schema : column.UserTypeSchemaName;
                AddDependency(accumulator, ProcedureDependencyKind.UserDefinedType, schema, column.UserTypeName, descriptor.Catalog);
            }

            if (column.Columns is { Count: > 0 })
            {
                CollectColumnDependencies(column.Columns, descriptor, accumulator);
            }
        }
    }

    /// <summary>
    /// Registers a dependency entry when it has not been recorded yet.
    /// </summary>
    private static void AddDependency(
        IDictionary<string, ProcedureDependency> accumulator,
        ProcedureDependencyKind kind,
        string? schema,
        string? name,
        string? catalog)
    {
        if (kind == ProcedureDependencyKind.Unknown || string.IsNullOrWhiteSpace(name))
        {
            return;
        }

        var normalizedSchema = schema?.Trim() ?? string.Empty;
        var normalizedName = name.Trim();
        var normalizedCatalog = catalog?.Trim() ?? string.Empty;
        var key = $"{(int)kind}|{normalizedCatalog.ToLowerInvariant()}|{normalizedSchema.ToLowerInvariant()}|{normalizedName.ToLowerInvariant()}";

        if (accumulator.ContainsKey(key))
        {
            return;
        }

        accumulator[key] = new ProcedureDependency
        {
            Kind = kind,
            Catalog = normalizedCatalog.Length == 0 ? null : normalizedCatalog,
            Schema = normalizedSchema,
            Name = normalizedName
        };
    }

    /// <summary>
    /// Emits warnings for FOR JSON WITHOUT_ARRAY_WRAPPER projections that lack single-row guarantees.
    /// </summary>
    private void EmitJsonWithoutArrayWarnings(ProcedureDescriptor descriptor, ProcedureModel? procedure)
    {
        if (procedure?.ResultSets == null)
        {
            return;
        }

        var descriptorLabel = FormatProcedureLabel(descriptor);

        foreach (var resultSet in procedure.ResultSets)
        {
            if (resultSet == null || !resultSet.ReturnsJson || resultSet.ReturnsJsonArray)
            {
                continue;
            }

            if (RequiresSingleRowGuaranteeWarning(resultSet.JsonSingleRowGuaranteed))
            {
                _console.WarnBuffered(
                    $"[snapshot-analyze] {descriptorLabel}: FOR JSON WITHOUT_ARRAY_WRAPPER detected without a single-row guarantee.",
                    "The result set emits JSON without enforcing a single-row constraint; callers may receive invalid JSON when multiple rows are returned.");
            }
        }
    }

    /// <summary>
    /// Determines whether a WITHOUT_ARRAY_WRAPPER projection needs a warning.
    /// </summary>
    private static bool RequiresSingleRowGuaranteeWarning(bool? singleRowGuaranteed)
    {
        return !singleRowGuaranteed.HasValue || singleRowGuaranteed.Value == false;
    }

    /// <summary>
    /// Builds an immutable analysis result.
    /// </summary>
    private static ProcedureAnalysisResult BuildAnalysisResult(
        ProcedureDescriptor descriptor,
        ProcedureModel? procedure,
        IReadOnlyList<StoredProcedureInput> parameters,
        IReadOnlyList<ProcedureDependency> dependencies,
        DateTime? lastModifiedUtc,
        string snapshotFile,
        bool reusedFromCache)
    {
        var parameterSnapshot = parameters != null && parameters.Count > 0 ? parameters.ToArray() : Array.Empty<StoredProcedureInput>();
        var dependencySnapshot = dependencies != null && dependencies.Count > 0 ? dependencies.ToArray() : Array.Empty<ProcedureDependency>();

        return new ProcedureAnalysisResult
        {
            Descriptor = descriptor,
            Procedure = procedure,
            WasReusedFromCache = reusedFromCache,
            SourceLastModifiedUtc = lastModifiedUtc,
            SnapshotFile = snapshotFile,
            Parameters = parameterSnapshot,
            Dependencies = dependencySnapshot
        };
    }

    /// <summary>
    /// Determines the snapshot file name for the analyzed procedure.
    /// </summary>
    private static string DetermineSnapshotFileName(ProcedureCollectionItem item, ProcedureDescriptor descriptor)
    {
        if (!string.IsNullOrWhiteSpace(item?.CachedSnapshotFile))
        {
            return item.CachedSnapshotFile!;
        }

        var schemaPart = NameSanitizer.SanitizeForFile(descriptor.Schema ?? string.Empty);
        var namePart = NameSanitizer.SanitizeForFile(descriptor.Name ?? string.Empty);

        if (string.IsNullOrWhiteSpace(schemaPart))
        {
            schemaPart = "unknown";
        }

        if (string.IsNullOrWhiteSpace(namePart))
        {
            namePart = "unnamed";
        }

        return $"{schemaPart}.{namePart}.json";
    }

    /// <summary>
    /// Maps analyzer reference kinds to dependency kinds.
    /// </summary>
    private static ProcedureDependencyKind? MapReferenceKind(ProcedureReferenceKind kind)
    {
        return kind switch
        {
            ProcedureReferenceKind.Procedure => ProcedureDependencyKind.Procedure,
            ProcedureReferenceKind.Function => ProcedureDependencyKind.Function,
            ProcedureReferenceKind.View => ProcedureDependencyKind.View,
            ProcedureReferenceKind.Table => ProcedureDependencyKind.Table,
            ProcedureReferenceKind.TableType => ProcedureDependencyKind.UserDefinedTableType,
            _ => null
        };
    }

    /// <summary>
    /// Formats a descriptor for diagnostic output.
    /// </summary>
    private static string FormatProcedureLabel(ProcedureDescriptor? descriptor)
    {
        if (descriptor == null)
        {
            return "(unknown)";
        }

        var schema = descriptor.Schema;
        var name = descriptor.Name;

        if (string.IsNullOrWhiteSpace(schema))
        {
            return string.IsNullOrWhiteSpace(name) ? "(unknown)" : name;
        }

        return string.IsNullOrWhiteSpace(name) ? schema : $"{schema}.{name}";
    }

    private async Task PrefetchProcedureMetadataAsync(IReadOnlyList<ProcedureCollectionItem> items, CancellationToken cancellationToken)
    {
        if (items == null || items.Count == 0)
        {
            return;
        }

        var schemas = items
            .Select(static i => i?.Descriptor?.Schema)
            .Where(static s => !string.IsNullOrWhiteSpace(s))
            .Select(static s => s!.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (schemas.Length == 0)
        {
            return;
        }

        try
        {
            var definitions = await _dbContext.StoredProcedureDefinitionsBySchemaAsync(schemas, cancellationToken).ConfigureAwait(false);
            _definitionCache = definitions?
                .Where(static d => !string.IsNullOrWhiteSpace(d.SchemaName) && !string.IsNullOrWhiteSpace(d.Name))
                .ToDictionary(static d => BuildProcedureKey(d.SchemaName!, d.Name!)!, static d => d.Definition ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] prefetch definitions failed: {ex.Message}");
            _definitionCache = null;
        }

        try
        {
            var parameters = await _dbContext.StoredProcedureInputListBySchemaAsync(schemas, cancellationToken).ConfigureAwait(false);
            if (parameters != null)
            {
                _parameterCache = new Dictionary<string, List<StoredProcedureInput>>(StringComparer.OrdinalIgnoreCase);
                foreach (var parameter in parameters)
                {
                    if (string.IsNullOrWhiteSpace(parameter.SchemaName) || string.IsNullOrWhiteSpace(parameter.StoredProcedureName))
                    {
                        continue;
                    }

                    var key = BuildProcedureKey(parameter.SchemaName, parameter.StoredProcedureName);
                    if (key == null)
                    {
                        continue;
                    }

                    if (!_parameterCache.TryGetValue(key, out var list))
                    {
                        list = new List<StoredProcedureInput>();
                        _parameterCache[key] = list;
                    }

                    list.Add(new StoredProcedureInput
                    {
                        Name = parameter.Name,
                        IsNullable = parameter.IsNullable,
                        SqlTypeName = parameter.SqlTypeName,
                        MaxLength = parameter.MaxLength,
                        IsOutput = parameter.IsOutput,
                        IsTableType = parameter.IsTableType,
                        UserTypeName = parameter.UserTypeName,
                        UserTypeId = parameter.UserTypeId,
                        UserTypeSchemaName = parameter.UserTypeSchemaName
                    });
                }
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[snapshot-analyze] prefetch parameters failed: {ex.Message}");
            _parameterCache = null;
        }
    }

    private static string? BuildProcedureKey(string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return string.Concat(schema.Trim(), ".", name.Trim());
    }
}
