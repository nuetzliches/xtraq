using Xtraq.Data;
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

        var updated = new List<ProcedureAnalysisResult>(analyzedProcedures.Count);
        var verbose = options.Verbose;
        var degreeOfParallelism = options.MaxDegreeOfParallelism > 0
            ? options.MaxDegreeOfParallelism
            : Environment.ProcessorCount;

        var parallelOptions = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Max(1, degreeOfParallelism)
        };

        var writeResults = new ConcurrentBag<ProcedureWriteRecord>();
        var requiredTypeRefs = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
        var requiredTableRefs = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        await Parallel.ForEachAsync(
            analyzedProcedures.Select((item, index) => (item, index)),
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

                var jsonBytes = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
                    descriptor,
                    item.Parameters,
                    item.Procedure,
                    localTypeRefs,
                    localTableRefs,
                    _jsonEnhancementService);

                var writeOutcome = await WriteArtifactAsync(filePath, jsonBytes, ct).ConfigureAwait(false);
                if (writeOutcome.Wrote && verbose)
                {
                    _console.Verbose($"[snapshot-write] wrote {fileName}");
                }

                var result = new ProcedureAnalysisResult
                {
                    Descriptor = descriptor,
                    Procedure = item.Procedure,
                    WasReusedFromCache = item.WasReusedFromCache,
                    SourceLastModifiedUtc = item.SourceLastModifiedUtc,
                    SnapshotFile = fileName,
                    SnapshotHash = writeOutcome.Hash,
                    Parameters = item.Parameters,
                    Dependencies = item.Dependencies
                };

                writeResults.Add(new ProcedureWriteRecord(index, result, writeOutcome.Wrote));

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

        var orderedWrites = writeResults
            .OrderBy(static record => record.Index)
            .ToList();

        var filesWritten = orderedWrites.Count(static record => record.Wrote);
        var filesUnchanged = orderedWrites.Count - filesWritten;
        updated.AddRange(orderedWrites.Select(static record => record.Result));

        var schemaArtifacts = await _schemaArtifactWriter.WriteAsync(
            schemaRoot,
            options,
            updated,
            new HashSet<string>(requiredTypeRefs.Keys, StringComparer.OrdinalIgnoreCase),
            new HashSet<string>(requiredTableRefs.Keys, StringComparer.OrdinalIgnoreCase),
            cancellationToken).ConfigureAwait(false);

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

    private sealed record ProcedureWriteRecord(int Index, ProcedureAnalysisResult Result, bool Wrote);
}
