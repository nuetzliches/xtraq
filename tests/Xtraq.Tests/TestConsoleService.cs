namespace Xtraq.Tests;

/// <summary>
/// Minimal console implementation used to capture messages during tests without relying on standard output.
/// </summary>
internal sealed class TestConsoleService : Xtraq.Services.IConsoleService
{
    private readonly System.Collections.Generic.List<string> _messages = new();
    private bool _verbose;

    /// <summary>
    /// Initializes a new instance of the <see cref="TestConsoleService"/> class.
    /// </summary>
    /// <param name="isVerbose">Controls whether verbose messages are recorded.</param>
    public TestConsoleService(bool isVerbose = true)
    {
        _verbose = isVerbose;
    }

    /// <inheritdoc />
    public bool IsVerbose => _verbose;

    /// <inheritdoc />
    public void Info(string message) => Record(message);

    /// <inheritdoc />
    public void Error(string message) => Record(message);

    /// <inheritdoc />
    public void Warn(string message) => Record(message);

    /// <inheritdoc />
    public void WarnBuffered(string summary, string detail, int occurrences = 1) => Record(summary);

    /// <inheritdoc />
    public void Output(string message) => Record(message);

    /// <inheritdoc />
    public void Verbose(string message)
    {
        if (IsVerbose)
        {
            Record(message);
        }
    }

    /// <inheritdoc />
    public void Success(string message) => Record(message);

    /// <inheritdoc />
    public void DrawProgressBar(int percentage, int barSize = 40)
    {
        // Progress output is not needed during tests.
    }

    /// <inheritdoc />
    public void Green(string message) => Record(message);

    /// <inheritdoc />
    public void Yellow(string message) => Record(message);

    /// <inheritdoc />
    public void Red(string message) => Record(message);

    /// <inheritdoc />
    public void Gray(string message) => Record(message);

    /// <inheritdoc />
    public Xtraq.Services.Choice GetSelection(string prompt, System.Collections.Generic.List<string> options)
    {
        Record(prompt);
        var selection = options.Count > 0 ? options[0] : string.Empty;
        return new Xtraq.Services.Choice(0, selection);
    }

    /// <inheritdoc />
    public Xtraq.Services.Choice GetSelectionMultiline(string prompt, System.Collections.Generic.List<string> options) => GetSelection(prompt, options);

    /// <inheritdoc />
    public bool GetYesNo(string prompt, bool isDefaultConfirmed, System.ConsoleColor? promptColor = null, System.ConsoleColor? promptBgColor = null)
    {
        Record(prompt);
        return isDefaultConfirmed;
    }

    /// <inheritdoc />
    public string GetString(string prompt, string defaultValue = "", System.ConsoleColor? promptColor = null)
    {
        Record(prompt);
        return defaultValue;
    }

    /// <inheritdoc />
    public void PrintTitle(string title) => Record(title);

    /// <inheritdoc />
    public void PrintFileActionMessage(string fileName, Xtraq.Core.FileActionEnum fileAction)
    {
        Record(string.Concat(fileAction, ": ", fileName));
    }

    /// <inheritdoc />
    public void StartProgress(string message) => Record(message);

    /// <inheritdoc />
    public void CompleteProgress(bool success = true, string? message = null)
    {
        if (!string.IsNullOrWhiteSpace(message))
        {
            Record(message);
        }
    }

    /// <summary>
    /// Sets the verbosity flag for subsequent calls.
    /// </summary>
    /// <param name="isVerbose">New verbosity value.</param>
    public void SetVerbose(bool isVerbose)
    {
        _verbose = isVerbose;
    }

    /// <inheritdoc />
    public Xtraq.Services.IConsoleProgressScope BeginProgressScope(string message)
    {
        Record(message);
        return new TestConsoleProgressScope(this);
    }

    /// <inheritdoc />
    public void FlushWarningsSummary()
    {
        // No aggregation performed in tests; nothing to flush.
    }

    /// <inheritdoc />
    public void RenderBreakdownChart(string title, System.Collections.Generic.IReadOnlyDictionary<string, double> segments, string? unit = null)
    {
        if (segments is null || segments.Count == 0)
        {
            return;
        }

        var suffix = string.IsNullOrWhiteSpace(unit) ? string.Empty : $" {unit}";
        var builder = new System.Text.StringBuilder();
        foreach (var segment in segments)
        {
            if (builder.Length > 0)
            {
                builder.Append(", ");
            }

            builder.Append(segment.Key);
            builder.Append(':');
            builder.Append(segment.Value.ToString("F0", System.Globalization.CultureInfo.InvariantCulture));
        }

        Record($"{title}{suffix} -> {builder}");
    }

    /// <inheritdoc />
    public void RenderBreakdownTable(string title, System.Collections.Generic.IReadOnlyDictionary<string, double> segments, string valueHeader, string? unit = null)
    {
        if (segments is null || segments.Count == 0)
        {
            return;
        }

        var suffix = string.IsNullOrWhiteSpace(unit) ? string.Empty : $" {unit}";
        var builder = new System.Text.StringBuilder();
        foreach (var segment in segments)
        {
            if (builder.Length > 0)
            {
                builder.Append(", ");
            }

            builder.Append(segment.Key);
            builder.Append('=');
            builder.Append(segment.Value.ToString("F0", System.Globalization.CultureInfo.InvariantCulture));
            builder.Append(suffix);
        }

        Record($"{title} [{valueHeader}] -> {builder}");
    }

    /// <inheritdoc />
    public void RenderKeyValueTable(string title, System.Collections.Generic.IReadOnlyDictionary<string, string> entries)
    {
        if (entries is null || entries.Count == 0)
        {
            return;
        }

        var builder = new System.Text.StringBuilder();
        foreach (var entry in entries)
        {
            if (builder.Length > 0)
            {
                builder.Append(", ");
            }

            builder.Append(entry.Key);
            builder.Append('=');
            builder.Append(entry.Value);
        }

        Record($"{title} -> {builder}");
    }

    /// <inheritdoc />
    public void RenderFiglet(string text) => Record(text);

    /// <inheritdoc />
    public void RenderJsonPayload(string title, string jsonPayload)
    {
        if (!string.IsNullOrWhiteSpace(title))
        {
            Record(title);
        }

        if (!string.IsNullOrWhiteSpace(jsonPayload))
        {
            Record(jsonPayload);
        }
    }

    /// <inheritdoc />
    public System.Threading.Tasks.Task RunProgressAsync(string title, int totalUnits, System.Func<System.Action<double>, System.Threading.Tasks.Task> workAsync, System.Threading.CancellationToken cancellationToken = default)
    {
        Record(title);
        if (workAsync == null)
        {
            return System.Threading.Tasks.Task.CompletedTask;
        }

        return workAsync(_ => cancellationToken.ThrowIfCancellationRequested());
    }

    public void RunProgress(string title, int totalUnits, System.Action<System.Action<double>> work)
    {
        Record(title);
        work?.Invoke(_ => { });
    }

    /// <summary>
    /// Provides all recorded messages for assertions.
    /// </summary>
    /// <returns>List with recorded messages.</returns>
    public System.Collections.Generic.IReadOnlyList<string> GetMessages() => _messages;

    private void Record(string? message)
    {
        if (!string.IsNullOrEmpty(message))
        {
            _messages.Add(message);
        }
    }

    private sealed class TestConsoleProgressScope : Xtraq.Services.IConsoleProgressScope
    {
        private readonly TestConsoleService _owner;
        private bool _completed;

        public TestConsoleProgressScope(TestConsoleService owner)
        {
            _owner = owner;
        }

        public void Complete(bool success = true, string? message = null)
        {
            if (_completed)
            {
                return;
            }

            _completed = true;
            if (!string.IsNullOrWhiteSpace(message))
            {
                _owner.Record(message);
            }
        }

        public void Dispose()
        {
            Complete();
        }
    }
}
