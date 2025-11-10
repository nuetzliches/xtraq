namespace Xtraq.Telemetry;

/// <summary>
/// Captures performance and output statistics for a build run.
/// </summary>
internal sealed record BuildTelemetryReport
{
    public required DateTimeOffset StartTime { get; init; }

    public required DateTimeOffset EndTime { get; init; }

    public TimeSpan Duration => EndTime - StartTime;

    public required string OutputRoot { get; init; }

    public required IReadOnlyList<BuildTelemetryPhase> Phases { get; init; }

    public required IReadOnlyList<GeneratedFileTelemetry> Files { get; init; }

    public int TotalFiles { get; init; }

    public long TotalBytes { get; init; }

    public double AverageFileBytes { get; init; }

    public required IReadOnlyList<BuildTelemetryCategorySummary> Categories { get; init; }

    public IReadOnlyList<GeneratedFileTelemetry> LargestFiles { get; init; } = Array.Empty<GeneratedFileTelemetry>();

    public int TableTypeArtifacts { get; init; }

    public int ProcedureArtifacts { get; init; }

    public int DbContextArtifacts { get; init; }

    public PlannerRunTelemetry? Planner { get; init; }

    public static BuildTelemetryReport Create(
        DateTimeOffset start,
        DateTimeOffset end,
        string outputRoot,
        IEnumerable<BuildTelemetryPhase> phases,
        IEnumerable<GeneratedFileTelemetry> files,
        int tableTypeArtifacts,
        int procedureArtifacts,
        int dbContextArtifacts)
    {
        var phaseList = phases?.ToList() ?? new List<BuildTelemetryPhase>();
        var fileList = files?.ToList() ?? new List<GeneratedFileTelemetry>();

        var categorySummaries = fileList
            .GroupBy(static f => f.Category ?? "(uncategorized)", StringComparer.OrdinalIgnoreCase)
            .Select(group => new BuildTelemetryCategorySummary
            {
                Category = group.Key,
                FileCount = group.Count(),
                TotalBytes = group.Sum(static f => f.SizeBytes)
            })
            .OrderByDescending(static s => s.TotalBytes)
            .ThenByDescending(static s => s.FileCount)
            .ToList();

        var largest = fileList
            .OrderByDescending(static f => f.SizeBytes)
            .ThenBy(static f => f.RelativePath, StringComparer.OrdinalIgnoreCase)
            .Take(25)
            .ToList();

        var totalFiles = fileList.Count;
        var totalBytes = fileList.Sum(static f => f.SizeBytes);
        var averageBytes = totalFiles == 0 ? 0 : totalBytes / (double)totalFiles;

        return new BuildTelemetryReport
        {
            StartTime = start,
            EndTime = end,
            OutputRoot = outputRoot,
            Phases = phaseList,
            Files = fileList,
            TotalFiles = totalFiles,
            TotalBytes = totalBytes,
            AverageFileBytes = averageBytes,
            Categories = categorySummaries,
            LargestFiles = largest,
            TableTypeArtifacts = tableTypeArtifacts,
            ProcedureArtifacts = procedureArtifacts,
            DbContextArtifacts = dbContextArtifacts
        };
    }
}

/// <summary>
/// Represents a high-level build phase.
/// </summary>
internal sealed record BuildTelemetryPhase
{
    public required string Name { get; init; }

    public double DurationMilliseconds { get; init; }

    public int ArtifactCount { get; init; }
}

/// <summary>
/// Contains metadata for a generated file.
/// </summary>
internal sealed record GeneratedFileTelemetry
{
    public required string RelativePath { get; init; }

    public required long SizeBytes { get; init; }

    public string? Category { get; init; }

    public required DateTimeOffset WrittenAtUtc { get; init; }
}

/// <summary>
/// Aggregated metrics per top-level category.
/// </summary>
internal sealed record BuildTelemetryCategorySummary
{
    public required string Category { get; init; }

    public int FileCount { get; init; }

    public long TotalBytes { get; init; }
}
