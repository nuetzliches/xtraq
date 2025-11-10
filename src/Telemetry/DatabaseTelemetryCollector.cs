using System.Data;
using System.Runtime.CompilerServices;

namespace Xtraq.Telemetry;

/// <summary>
/// Collects telemetry information for database queries executed during a snapshot/build run.
/// </summary>
internal interface IDatabaseTelemetryCollector
{
    IDatabaseQueryTelemetryScope StartQuery(DatabaseQueryTelemetryMetadata metadata);

    DatabaseTelemetryReport CreateReport(bool reset = false);

    void Reset();

    Task WriteReportAsync(DatabaseTelemetryReport report, string directoryPath, string fileName, CancellationToken cancellationToken = default);
}

/// <summary>
/// Scope interface returned for an individual database query execution.
/// </summary>
internal interface IDatabaseQueryTelemetryScope : IDisposable
{
    void MarkCompleted(int rowCount);

    void MarkIntercepted(int rowCount);

    void MarkFailed(int rowCount, Exception exception);
}

/// <summary>
/// Metadata describing a database query before execution.
/// </summary>
/// <param name="Operation">Logical operation name (e.g. StoredProcedureQueries.GetAll).</param>
/// <param name="Category">Optional category (Collector/Analyzer/etc.).</param>
/// <param name="CommandText">Command text that will be executed.</param>
/// <param name="CommandType">Command type.</param>
/// <param name="ParameterCount">Number of parameters.</param>
/// <param name="Caller">Caller information (member name).</param>
/// <param name="FilePath">Optional source file path.</param>
/// <param name="LineNumber">Optional source line number.</param>
internal sealed record DatabaseQueryTelemetryMetadata(
    string Operation,
    string? Category,
    string CommandText,
    CommandType CommandType,
    int ParameterCount,
    string Caller,
    string? FilePath,
    int? LineNumber);

/// <summary>
/// Single telemetry entry describing an executed query.
/// </summary>
internal sealed record DatabaseQueryTelemetryEntry
{
    public required string Operation { get; init; }

    public string? Category { get; init; }

    public required CommandType CommandType { get; init; }

    public required string CommandTextPreview { get; init; }

    public required string CommandHash { get; init; }

    public required DateTimeOffset StartTime { get; init; }

    public required DateTimeOffset EndTime { get; init; }

    public required TimeSpan Duration { get; init; }

    public int ParameterCount { get; init; }

    public int RowCount { get; init; }

    public bool IsSuccess { get; init; }

    public bool IsIntercepted { get; init; }

    public string? ExceptionType { get; init; }

    public string? ExceptionMessage { get; init; }

    public string? Caller { get; init; }

    public string? FilePath { get; init; }

    public int? LineNumber { get; init; }
}

/// <summary>
/// Aggregated telemetry report.
/// </summary>
internal sealed record DatabaseTelemetryReport
{
    public required DateTimeOffset StartTime { get; init; }

    public required DateTimeOffset EndTime { get; init; }

    public required IReadOnlyList<DatabaseQueryTelemetryEntry> Entries { get; init; }

    public TimeSpan TotalDuration => TimeSpan.FromMilliseconds(Entries.Sum(static e => e.Duration.TotalMilliseconds));

    public int TotalQueries => Entries.Count;

    public int FailedQueries => Entries.Count(static e => !e.IsSuccess && !e.IsIntercepted);

    public IReadOnlyList<DatabaseTelemetryOperationSummary> TopOperations { get; init; } = Array.Empty<DatabaseTelemetryOperationSummary>();

    public IReadOnlyList<DatabaseQueryTelemetryEntry> LongestQueries { get; init; } = Array.Empty<DatabaseQueryTelemetryEntry>();

    public IReadOnlyDictionary<string, double> CategoryDurations { get; init; } = new Dictionary<string, double>();

    public PlannerRunTelemetry? Planner { get; init; }

    public static DatabaseTelemetryReport FromEntries(DateTimeOffset start, DateTimeOffset end, IReadOnlyList<DatabaseQueryTelemetryEntry> entries)
    {
        var summaries = entries
            .GroupBy(static e => (Operation: e.Operation, e.Category), OperationGroupingComparer.Instance)
            .Select(group => new DatabaseTelemetryOperationSummary
            {
                Operation = group.Key.Operation,
                Category = group.Key.Category,
                Count = group.Count(),
                TotalDuration = TimeSpan.FromMilliseconds(group.Sum(static e => e.Duration.TotalMilliseconds)),
                AverageDuration = group.Count() == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(group.Average(static e => e.Duration.TotalMilliseconds)),
                MaxDuration = group.Count() == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(group.Max(static e => e.Duration.TotalMilliseconds)),
                RowCount = group.Sum(static e => e.RowCount),
                Failures = group.Count(static e => !e.IsSuccess && !e.IsIntercepted)
            })
            .OrderByDescending(static s => s.TotalDuration)
            .ThenByDescending(static s => s.Count)
            .Take(25)
            .ToList();

        var longest = entries
            .OrderByDescending(static e => e.Duration)
            .Take(25)
            .ToList();

        var categoryDurations = entries
            .GroupBy(static e => e.Category ?? "(uncategorized)", StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                static g => g.Key,
                static g => g.Sum(e => e.Duration.TotalMilliseconds));

        return new DatabaseTelemetryReport
        {
            StartTime = start,
            EndTime = end,
            Entries = entries,
            TopOperations = summaries,
            LongestQueries = longest,
            CategoryDurations = categoryDurations
        };
    }
}

/// <summary>
/// Aggregated statistics per operation/category.
/// </summary>
internal sealed record DatabaseTelemetryOperationSummary
{
    public required string Operation { get; init; }

    public string? Category { get; init; }

    public int Count { get; init; }

    public TimeSpan TotalDuration { get; init; }

    public TimeSpan AverageDuration { get; init; }

    public TimeSpan MaxDuration { get; init; }

    public int RowCount { get; init; }

    public int Failures { get; init; }
}

/// <summary>
/// Snapshot planner execution details for telemetry correlation.
/// </summary>
internal sealed record PlannerRunTelemetry
{
    public bool PlannerExecuted { get; init; }

    public bool WarmRun { get; init; }

    public bool ReusedExistingResult { get; init; }

    public int PlannerInvocationCount { get; init; }

    public int RefreshPlanBatches { get; init; }

    public int ObjectsToRefresh { get; init; }

    public int MissingSnapshotCount { get; init; }

    public int TotalQueryCount { get; init; }

    public int FailedQueryCount { get; init; }

    public IReadOnlyList<string> EffectiveSchemas { get; init; } = Array.Empty<string>();

    public string? PlanFilePath { get; init; }
}

/// <summary>
/// Default implementation that stores telemetry entries in memory.
/// </summary>
internal sealed class DatabaseTelemetryCollector : IDatabaseTelemetryCollector
{
    private readonly object _sync = new();
    private ConcurrentBag<DatabaseQueryTelemetryEntry> _entries = new();
    private DateTimeOffset _runStart = DateTimeOffset.UtcNow;

    public IDatabaseQueryTelemetryScope StartQuery(DatabaseQueryTelemetryMetadata metadata)
    {
        return new DatabaseQueryTelemetryScope(this, metadata);
    }

    public DatabaseTelemetryReport CreateReport(bool reset = false)
    {
        var snapshot = _entries.ToArray();
        var report = DatabaseTelemetryReport.FromEntries(_runStart, DateTimeOffset.UtcNow, snapshot);
        if (reset)
        {
            Reset();
        }

        return report;
    }

    public void Reset()
    {
        lock (_sync)
        {
            _entries = new ConcurrentBag<DatabaseQueryTelemetryEntry>();
            _runStart = DateTimeOffset.UtcNow;
        }
    }

    internal void AddEntry(DatabaseQueryTelemetryEntry entry)
    {
        _entries.Add(entry);
    }

    public async Task WriteReportAsync(DatabaseTelemetryReport report, string directoryPath, string fileName, CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(directoryPath);
        var filePath = Path.Combine(directoryPath, fileName);

        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, report, new JsonSerializerOptions
        {
            WriteIndented = true
        }, cancellationToken).ConfigureAwait(false);
    }

    private sealed class DatabaseQueryTelemetryScope : IDatabaseQueryTelemetryScope
    {
        private readonly DatabaseTelemetryCollector _collector;
        private readonly DatabaseQueryTelemetryMetadata _metadata;
        private readonly Stopwatch _stopwatch;
        private readonly DateTimeOffset _start;
        private bool _completed;
        private int _rowCount;
        private Exception? _exception;
        private bool _intercepted;

        public DatabaseQueryTelemetryScope(DatabaseTelemetryCollector collector, DatabaseQueryTelemetryMetadata metadata)
        {
            _collector = collector;
            _metadata = metadata;
            _start = DateTimeOffset.UtcNow;
            _stopwatch = Stopwatch.StartNew();
        }

        public void MarkCompleted(int rowCount)
        {
            _completed = true;
            _rowCount = rowCount;
            _stopwatch.Stop();
            EnqueueEntry(isSuccess: true, isIntercepted: false, null);
        }

        public void MarkIntercepted(int rowCount)
        {
            _completed = true;
            _rowCount = rowCount;
            _intercepted = true;
            _stopwatch.Stop();
            EnqueueEntry(isSuccess: true, isIntercepted: true, null);
        }

        public void MarkFailed(int rowCount, Exception exception)
        {
            _completed = true;
            _rowCount = rowCount;
            _exception = exception;
            _stopwatch.Stop();
            EnqueueEntry(isSuccess: false, isIntercepted: false, exception);
        }

        public void Dispose()
        {
            if (_completed)
            {
                return;
            }

            _stopwatch.Stop();
            EnqueueEntry(isSuccess: false, isIntercepted: _intercepted, _exception ?? new InvalidOperationException("Telemetry scope disposed without completion."));
        }

        private void EnqueueEntry(bool isSuccess, bool isIntercepted, Exception? exception)
        {
            _collector.AddEntry(new DatabaseQueryTelemetryEntry
            {
                Operation = _metadata.Operation,
                Category = _metadata.Category,
                CommandType = _metadata.CommandType,
                CommandTextPreview = CreatePreview(_metadata.CommandText),
                CommandHash = ComputeHash(_metadata.CommandText),
                StartTime = _start,
                EndTime = _start + _stopwatch.Elapsed,
                Duration = _stopwatch.Elapsed,
                ParameterCount = _metadata.ParameterCount,
                RowCount = _rowCount,
                IsSuccess = isSuccess,
                IsIntercepted = isIntercepted,
                ExceptionType = exception?.GetType().FullName,
                ExceptionMessage = exception?.Message,
                Caller = _metadata.Caller,
                FilePath = _metadata.FilePath,
                LineNumber = _metadata.LineNumber
            });
        }

        private static string CreatePreview(string commandText)
        {
            if (string.IsNullOrWhiteSpace(commandText))
            {
                return string.Empty;
            }

            const int maxLength = 240;
            if (commandText.Length <= maxLength)
            {
                return commandText.Trim();
            }

            return string.Concat(commandText.AsSpan(0, maxLength).Trim(), " …");
        }

        private static string ComputeHash(string commandText)
        {
            using var sha = SHA256.Create();
            var bytes = Encoding.UTF8.GetBytes(commandText);
            var hash = sha.ComputeHash(bytes);
            return Convert.ToHexString(hash);
        }
    }
}

/// <summary>
/// Helper utilities for telemetry instrumentation.
/// </summary>
internal static class DatabaseTelemetryCollectorExtensions
{
    public static IDatabaseQueryTelemetryScope StartQuery(
        this IDatabaseTelemetryCollector collector,
        string operation,
        string? category,
        string commandText,
        CommandType commandType,
        int parameterCount,
        string? telemetryOverrideName = null,
        string? telemetryCategory = null,
        [CallerMemberName] string caller = "",
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = 0)
    {
        var metadata = new DatabaseQueryTelemetryMetadata(
            Operation: telemetryOverrideName ?? operation,
            Category: telemetryCategory ?? category,
            CommandText: commandText,
            CommandType: commandType,
            ParameterCount: parameterCount,
            Caller: caller,
            FilePath: filePath,
            LineNumber: lineNumber == 0 ? null : lineNumber);

        return collector.StartQuery(metadata);
    }
}

file sealed class OperationGroupingComparer : IEqualityComparer<(string Operation, string? Category)>
{
    public static readonly OperationGroupingComparer Instance = new();

    public bool Equals((string Operation, string? Category) x, (string Operation, string? Category) y)
    {
        return string.Equals(x.Operation, y.Operation, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Category, y.Category, StringComparison.OrdinalIgnoreCase);
    }

    public int GetHashCode((string Operation, string? Category) obj)
    {
        var operationHash = obj.Operation == null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Operation);
        var categoryHash = obj.Category == null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Category);
        return HashCode.Combine(operationHash, categoryHash);
    }
}
