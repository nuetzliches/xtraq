using Xtraq.Data;
using Xtraq.Data.Models;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Metadata;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Writers;

internal sealed class ExpandedSnapshotWriter : ISnapshotWriter
{
    private readonly IConsoleService _console;
    private readonly SchemaArtifactWriter _schemaArtifactWriter;
    private readonly SnapshotIndexWriter _snapshotIndexWriter;
    private readonly IJsonFunctionEnhancementService? _jsonEnhancementService;

    public ExpandedSnapshotWriter(
        IConsoleService console,
        DbContext dbContext,
        ITableMetadataProvider tableMetadataProvider,
        ITableTypeMetadataProvider tableTypeMetadataProvider,
        IUserDefinedTypeMetadataProvider userDefinedTypeMetadataProvider,
        IJsonFunctionEnhancementService? jsonEnhancementService = null)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        if (dbContext == null)
        {
            throw new ArgumentNullException(nameof(dbContext));
        }

        var tableProvider = tableMetadataProvider ?? throw new ArgumentNullException(nameof(tableMetadataProvider));
        var tableTypeProvider = tableTypeMetadataProvider ?? throw new ArgumentNullException(nameof(tableTypeMetadataProvider));
        var userDefinedTypeProvider = userDefinedTypeMetadataProvider ?? throw new ArgumentNullException(nameof(userDefinedTypeMetadataProvider));

        _jsonEnhancementService = jsonEnhancementService;

        _schemaArtifactWriter = new SchemaArtifactWriter(
            _console,
            dbContext,
            tableProvider,
            tableTypeProvider,
            userDefinedTypeProvider,
            WriteArtifactAsync);

        _snapshotIndexWriter = new SnapshotIndexWriter(SnapshotWriterUtilities.PersistSnapshotAsync);
    }

    public async Task<SnapshotWriteResult> WriteAsync(
        IReadOnlyList<ProcedureAnalysisResult> analyzedProcedures,
        SnapshotBuildOptions options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        options ??= SnapshotBuildOptions.Default;

        if (analyzedProcedures == null || analyzedProcedures.Count == 0)
        {
            return new SnapshotWriteResult();
        }

        var projectRoot = ProjectRootResolver.ResolveCurrent();
        var schemaRoot = Path.Combine(projectRoot, ".xtraq", "snapshots");
        var proceduresRoot = Path.Combine(schemaRoot, "procedures");
        Directory.CreateDirectory(proceduresRoot);

        var verbose = options.Verbose;
        var degreeOfParallelism = options.MaxDegreeOfParallelism > 0
            ? options.MaxDegreeOfParallelism
            : Environment.ProcessorCount;

        var parallelOptions = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Max(1, degreeOfParallelism)
        };

        var writePlans = new ConcurrentBag<ProcedureWritePlan>();
        var requiredTypeRefs = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
        var requiredTableRefs = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        await Parallel.ForEachAsync(
            analyzedProcedures.Select(static (item, index) => (item, index)),
            parallelOptions,
            async (entry, ct) =>
            {
                var (item, index) = entry;
                if (item == null)
                {
                    return;
                }

                ct.ThrowIfCancellationRequested();

                var descriptor = item.Descriptor ?? new ProcedureDescriptor();
                var fileName = string.IsNullOrWhiteSpace(item.SnapshotFile)
                    ? BuildDefaultSnapshotFile(descriptor)
                    : item.SnapshotFile;
                var filePath = Path.Combine(proceduresRoot, fileName);

                var localTypeRefs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var localTableRefs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
                    descriptor,
                    item.Parameters,
                    item.Procedure,
                    localTypeRefs,
                    localTableRefs,
                    _jsonEnhancementService);

                writePlans.Add(new ProcedureWritePlan(
                    index,
                    item,
                    descriptor,
                    fileName,
                    filePath));

                foreach (var typeRef in localTypeRefs)
                {
                    if (!string.IsNullOrWhiteSpace(typeRef))
                    {
                        requiredTypeRefs.TryAdd(typeRef, 0);
                    }
                }

                foreach (var tableRef in localTableRefs)
                {
                    if (!string.IsNullOrWhiteSpace(tableRef))
                    {
                        requiredTableRefs.TryAdd(tableRef, 0);
                    }
                }
            }).ConfigureAwait(false);

        var orderedPlans = writePlans
            .OrderBy(static plan => plan.Index)
            .ToList();

        var schemaArtifacts = await _schemaArtifactWriter.WriteAsync(
            schemaRoot,
            options,
            analyzedProcedures,
            new HashSet<string>(requiredTypeRefs.Keys, StringComparer.OrdinalIgnoreCase),
            new HashSet<string>(requiredTableRefs.Keys, StringComparer.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

        var updated = new List<ProcedureAnalysisResult>(orderedPlans.Count);
        var filesWritten = 0;
        var filesUnchanged = 0;

        foreach (var plan in orderedPlans)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var descriptor = plan.Descriptor;
            var parameters = plan.Source.Parameters ?? Array.Empty<StoredProcedureInput>();
            var procedure = plan.Source.Procedure;
            var localTypeRefs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var localTableRefs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var content = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
                descriptor,
                parameters,
                procedure,
                localTypeRefs,
                localTableRefs,
                _jsonEnhancementService);

            foreach (var typeRef in localTypeRefs)
            {
                if (!string.IsNullOrWhiteSpace(typeRef))
                {
                    requiredTypeRefs.TryAdd(typeRef, 0);
                }
            }

            foreach (var tableRef in localTableRefs)
            {
                if (!string.IsNullOrWhiteSpace(tableRef))
                {
                    requiredTableRefs.TryAdd(tableRef, 0);
                }
            }

            var writeOutcome = await WriteArtifactAsync(plan.FilePath, content, cancellationToken).ConfigureAwait(false);
            if (writeOutcome.Wrote)
            {
                filesWritten++;
                if (verbose)
                {
                    _console.Verbose($"[snapshot-write] wrote {plan.FileName}");
                }
            }
            else
            {
                filesUnchanged++;
            }

            updated.Add(new ProcedureAnalysisResult
            {
                Descriptor = descriptor,
                Procedure = procedure,
                WasReusedFromCache = plan.Source.WasReusedFromCache,
                SourceLastModifiedUtc = plan.Source.SourceLastModifiedUtc,
                SnapshotFile = plan.FileName,
                SnapshotHash = writeOutcome.Hash,
                Parameters = plan.Source.Parameters ?? Array.Empty<StoredProcedureInput>(),
                Dependencies = plan.Source.Dependencies ?? Array.Empty<ProcedureDependency>()
            });
        }

        filesWritten += schemaArtifacts.FilesWritten;
        filesUnchanged += schemaArtifacts.FilesUnchanged;

        var indexDocument = await _snapshotIndexWriter.UpdateAsync(schemaRoot, updated, schemaArtifacts, cancellationToken).ConfigureAwait(false);

        return new SnapshotWriteResult
        {
            FilesWritten = filesWritten,
            FilesUnchanged = filesUnchanged,
            UpdatedProcedures = updated
        };
    }

    private static string BuildDefaultSnapshotFile(ProcedureDescriptor descriptor)
    {
        var schema = string.IsNullOrWhiteSpace(descriptor?.Schema) ? "unknown" : descriptor.Schema;
        var name = string.IsNullOrWhiteSpace(descriptor?.Name) ? "unnamed" : descriptor.Name;
        return $"{schema}.{name}.json";
    }

    private async Task<ArtifactWriteOutcome> WriteArtifactAsync(string filePath, byte[] content, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var hash = SnapshotWriterUtilities.ComputeHash(content);
        var shouldWrite = true;

        if (File.Exists(filePath))
        {
            try
            {
                var existingBytes = await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
                var existingHash = SnapshotWriterUtilities.ComputeHash(existingBytes);
                if (string.Equals(existingHash, hash, StringComparison.OrdinalIgnoreCase))
                {
                    shouldWrite = false;
                }
            }
            catch (Exception ex)
            {
                _console.Verbose($"[snapshot-write] failed to read existing snapshot at {filePath}: {ex.Message}");
            }
        }

        if (shouldWrite)
        {
            await SnapshotWriterUtilities.PersistSnapshotAsync(filePath, content, cancellationToken).ConfigureAwait(false);
        }

        return new ArtifactWriteOutcome(shouldWrite, hash);
    }

    private sealed record ProcedureWritePlan(
        int Index,
        ProcedureAnalysisResult Source,
        ProcedureDescriptor Descriptor,
        string FileName,
        string FilePath);
}
