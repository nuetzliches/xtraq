using Spectre.Console;
using Xtraq.Cli;
using Xtraq.Core;

namespace Xtraq.Services;

/// <summary>
/// Interface describing the console abstraction used by the CLI and generators.
/// </summary>
internal interface IConsoleService
{
    bool IsVerbose { get; }

    void Info(string message);
    void Error(string message);
    void Warn(string message);
    void Output(string message);
    void Verbose(string message);
    void Success(string message);
    void DrawProgressBar(int percentage, int barSize = 40);

    /// <summary>
    /// Records a warning without emitting an immediate console line; details surface via verbose logging.
    /// </summary>
    /// <param name="summary">Summary label used for aggregated output.</param>
    /// <param name="detail">Detailed message written only when verbose logging is enabled.</param>
    /// <param name="occurrences">Optional occurrence count to add to the aggregate.</param>
    void WarnBuffered(string summary, string detail, int occurrences = 1);

    void Green(string message);
    void Yellow(string message);
    void Red(string message);
    void Gray(string message);

    Choice GetSelection(string prompt, List<string> options);
    Choice GetSelectionMultiline(string prompt, List<string> options);
    bool GetYesNo(string prompt, bool isDefaultConfirmed, ConsoleColor? promptColor = null, ConsoleColor? promptBgColor = null);
    string GetString(string prompt, string defaultValue = "", ConsoleColor? promptColor = null);

    void PrintTitle(string title);
    void PrintFileActionMessage(string fileName, FileActionEnum fileAction);

    void StartProgress(string message);
    void CompleteProgress(bool success = true, string? message = null);

    /// <summary>
    /// Begins a scoped progress block that automatically completes when disposed.
    /// </summary>
    /// <param name="message">Human readable label describing the progress block.</param>
    /// <returns>A progress scope that should be disposed when the batch finishes.</returns>
    IConsoleProgressScope BeginProgressScope(string message);

    /// <summary>
    /// Emits an aggregated warning summary for the current run and clears tracked warnings.
    /// </summary>
    void FlushWarningsSummary();

    /// <summary>
    /// Renders a breakdown chart highlighting the relative contribution of individual segments.
    /// Falls back to textual output when Spectre enhancements are disabled.
    /// </summary>
    /// <param name="title">Heading rendered above the chart.</param>
    /// <param name="segments">Dictionary of segment labels mapped to their numeric contribution.</param>
    /// <param name="unit">Optional unit postfix appended to textual fallbacks.</param>
    void RenderBreakdownChart(string title, IReadOnlyDictionary<string, double> segments, string? unit = null);

    /// <summary>
    /// Emits a tabular distribution overview for the provided segments.
    /// In Spectre mode the method renders a styled table with percentage shares; otherwise it prints plain text.
    /// </summary>
    /// <param name="title">Caption printed above the table.</param>
    /// <param name="segments">Dictionary of segment labels mapped to their numeric contribution.</param>
    /// <param name="valueHeader">Header text for the value column.</param>
    /// <param name="unit">Optional unit label appended to value cells.</param>
    void RenderBreakdownTable(string title, IReadOnlyDictionary<string, double> segments, string valueHeader, string? unit = null);

    /// <summary>
    /// Renders a key/value table, preserving insertion order where possible and falling back to plain text output.
    /// </summary>
    /// <param name="title">Heading rendered above the table.</param>
    /// <param name="entries">Dictionary of keys mapped to their textual values.</param>
    void RenderKeyValueTable(string title, IReadOnlyDictionary<string, string> entries);

    /// <summary>
    /// Renders a stylized Figlet banner when Spectre enhancements are enabled, falling back to plain text output otherwise.
    /// </summary>
    /// <param name="text">Banner text to render.</param>
    void RenderFiglet(string text);

    /// <summary>
    /// Emits a JSON payload inside a Spectre panel, preserving indentation. Falls back to plain text output in CI mode.
    /// </summary>
    /// <param name="title">Heading rendered above the JSON content.</param>
    /// <param name="jsonPayload">JSON document to display.</param>
    void RenderJsonPayload(string title, string jsonPayload);

    /// <summary>
    /// Runs a progress-aware operation, emitting Spectre progress widgets when enabled.
    /// </summary>
    /// <param name="title">Description shown alongside the progress bar.</param>
    /// <param name="totalUnits">Total units to process. When zero or negative, an indeterminate indicator is shown.</param>
    /// <param name="work">Callback receiving an increment delegate. Invoke the delegate to advance progress.</param>
    void RunProgress(string title, int totalUnits, Action<Action<double>> work);

    /// <summary>
    /// Asynchronously runs a progress-aware operation, using Spectre progress widgets when available.
    /// </summary>
    /// <param name="title">Description shown alongside the progress bar.</param>
    /// <param name="totalUnits">Total units to process. When zero or negative, an indeterminate indicator is shown.</param>
    /// <param name="workAsync">Asynchronous callback receiving an increment delegate. Invoke the delegate to advance progress.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    Task RunProgressAsync(string title, int totalUnits, Func<Action<double>, Task> workAsync, CancellationToken cancellationToken = default);
}

/// <summary>
/// Disposable scope that wraps <see cref="IConsoleService.StartProgress"/> and <see cref="IConsoleService.CompleteProgress"/> calls.
/// </summary>
internal interface IConsoleProgressScope : IDisposable
{
    /// <summary>
    /// Marks the progress scope as finished and prints the completion line once.
    /// </summary>
    /// <param name="success">Indicates whether the operation succeeded.</param>
    /// <param name="message">Optional trailing message rendered under the completion marker.</param>
    void Complete(bool success = true, string? message = null);
}

/// <summary>
/// System.Console based implementation intentionally independent from external console libraries.
/// </summary>
internal sealed class ConsoleService : IConsoleService
{
    private readonly CommandOptions _commandOptions;
    private readonly object _writeLock = new();
    private readonly object _warningLock = new();

    private readonly Dictionary<string, WarningAggregate> _warnings = new(StringComparer.Ordinal);
    private int _warningOrder;

    private bool UseSpectre => !_commandOptions.CiMode;

    public ConsoleService(CommandOptions commandOptions)
    {
        _commandOptions = commandOptions ?? throw new ArgumentNullException(nameof(commandOptions));
    }

    public bool IsVerbose => _commandOptions?.Verbose ?? false;

    private static TextWriter StdOut => Console.Out;
    private static TextWriter StdErr => Console.Error;

    public void Info(string message) => Output(message);

    public void Error(string message) => WriteLine(StdErr, message, ConsoleColor.Red);

    public void Warn(string message)
    {
        RecordWarning(message);
        WriteLine(StdOut, message, ConsoleColor.Yellow);
    }

    public void WarnBuffered(string summary, string detail, int occurrences = 1)
    {
        RecordWarning(summary, occurrences);

        if (IsVerbose && !string.IsNullOrWhiteSpace(detail))
        {
            Verbose(detail);
        }
    }

    public void Output(string message)
    {
        WriteLine(StdOut, message, foregroundColor: null);
    }

    public void Verbose(string message)
    {
        if (!IsVerbose)
        {
            return;
        }

        WriteLine(StdOut, message, ConsoleColor.DarkGray);
    }

    public void Success(string message) => WriteLine(StdOut, message, ConsoleColor.Green);

    public void DrawProgressBar(int percentage, int barSize = 40)
    {
        percentage = Math.Clamp(percentage, 0, 100);
        barSize = Math.Max(10, barSize);

        lock (_writeLock)
        {
            try
            {
                Console.CursorVisible = false;
            }
            catch
            {
                // ignore cursor visibility errors (stdout redirected)
            }

            StdOut.Write('\r');

            var filled = (int)Math.Round(barSize * (percentage / 100.0));
            var empty = barSize - filled;

            TrySetColors(ConsoleColor.DarkGray, null);
            StdOut.Write('[');

            TrySetColors(ConsoleColor.Green, null);
            StdOut.Write(new string('#', filled));

            TrySetColors(ConsoleColor.DarkGray, null);
            StdOut.Write(new string('-', empty));
            StdOut.Write("] ");

            TrySetColors(ConsoleColor.Cyan, null);
            StdOut.Write($"{percentage}%");
            TryResetColors();

            if (percentage == 100)
            {
                StdOut.WriteLine();
                try
                {
                    Console.CursorVisible = true;
                }
                catch
                {
                }
            }
        }
    }

    public void Green(string message) => Success(message);
    public void Yellow(string message) => Warn(message);
    public void Red(string message) => Error(message);
    public void Gray(string message) => Verbose(message);

    public Choice GetSelection(string prompt, List<string> options)
    {
        return GetSelectionInternal(prompt, options, multiline: false);
    }

    public Choice GetSelectionMultiline(string prompt, List<string> options)
    {
        return GetSelectionInternal(prompt, options, multiline: true);
    }

    public bool GetYesNo(string prompt, bool isDefaultConfirmed, ConsoleColor? promptColor = null, ConsoleColor? promptBgColor = null)
    {
        var defaultLabel = isDefaultConfirmed ? "Y/n" : "y/N";
        while (true)
        {
            Write(StdOut, $"{prompt} ", promptColor, promptBgColor);
            Write(StdOut, $"[{defaultLabel}]", ConsoleColor.White);
            Write(StdOut, ": ", null);

            var line = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(line))
            {
                return isDefaultConfirmed;
            }

            line = line.Trim();
            if (line.Equals("y", StringComparison.OrdinalIgnoreCase) ||
                line.Equals("yes", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (line.Equals("n", StringComparison.OrdinalIgnoreCase) ||
                line.Equals("no", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            Warn("Please answer with 'y' or 'n'.");
        }
    }

    public string GetString(string prompt, string defaultValue = "", ConsoleColor? promptColor = null)
    {
        Write(StdOut, $"{prompt} ", promptColor);
        if (!string.IsNullOrEmpty(defaultValue))
        {
            Write(StdOut, $"[{defaultValue}] ", ConsoleColor.White);
        }

        Write(StdOut, ": ", null);
        var response = Console.ReadLine();
        if (string.IsNullOrEmpty(response))
        {
            response = defaultValue;
        }

        return response ?? string.Empty;
    }

    public void PrintTitle(string title)
    {
        if (string.IsNullOrWhiteSpace(title))
        {
            return;
        }

        if (UseSpectre)
        {
            AnsiConsole.MarkupLine($"[cyan]► {Escape(title)}[/]");
            return;
        }

        Success($"► {title}");
    }

    public void PrintFileActionMessage(string fileName, FileActionEnum fileAction)
    {
        switch (fileAction)
        {
            case FileActionEnum.Created:
                Success($"{fileName} (created)");
                break;
            case FileActionEnum.Modified:
                Yellow($"{fileName} (modified)");
                break;
            case FileActionEnum.UpToDate:
                Gray($"{fileName} (up to date)");
                break;
            default:
                Output($"{fileName} ({fileAction})");
                break;
        }
    }

    public void StartProgress(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            message = "Progress";
        }

        Success($"► {message}");
    }

    public void CompleteProgress(bool success = true, string? message = null)
    {
        if (UseSpectre)
        {
            var status = success ? "[green]✓ Completed[/]" : "[red]✗ Failed[/]";
            if (!string.IsNullOrWhiteSpace(message))
            {
                AnsiConsole.MarkupLine($"{status} [grey]{Escape(message)}[/]");
            }
            else
            {
                AnsiConsole.MarkupLine(status);
            }
            AnsiConsole.WriteLine();
            return;
        }

        var statusMessage = success ? "✓ Completed" : "✗ Failed";
        if (!string.IsNullOrWhiteSpace(message))
        {
            statusMessage = string.Concat(statusMessage, " – ", message);
        }

        if (success)
        {
            Success(statusMessage);
        }
        else
        {
            Error(statusMessage);
        }
    }

    public IConsoleProgressScope BeginProgressScope(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            message = "Progress";
        }

        if (UseSpectre)
        {
            AnsiConsole.MarkupLine($"[cyan]► {Escape(message)}[/]");
            return new ConsoleProgressScope(this);
        }

        StartProgress(message);
        return new ConsoleProgressScope(this);
    }

    public void FlushWarningsSummary()
    {
        List<(string Message, WarningAggregate Aggregate)> snapshot;
        lock (_warningLock)
        {
            if (_warnings.Count == 0)
            {
                return;
            }

            snapshot = _warnings
                .Select(static pair => (pair.Key, pair.Value))
                .ToList();
            _warnings.Clear();
            _warningOrder = 0;
        }

        var ordered = snapshot
            .OrderByDescending(static item => item.Aggregate.Count)
            .ThenBy(static item => item.Aggregate.Order)
            .ToList();

        var totalCount = ordered.Sum(static entry => entry.Aggregate.Count);
        if (UseSpectre)
        {
            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderStyle(new Style(Color.Gold1))
                .Title(new TableTitle("[yellow][[xtraq]] Warning summary[/]"));

            table.AddColumn(new TableColumn("[grey]#[/]").Centered());
            table.AddColumn(new TableColumn("[grey]Message[/]").LeftAligned());
            table.AddColumn(new TableColumn("[grey]Count[/]").RightAligned());
            table.AddColumn(new TableColumn("[grey]Share[/]").RightAligned());

            var index = 1;
            foreach (var entry in ordered)
            {
                var count = entry.Aggregate.Count;
                var share = totalCount > 0 ? (count / (double)totalCount) * 100d : 0d;
                table.AddRow(
                    index.ToString(CultureInfo.InvariantCulture),
                    Escape(entry.Message),
                    count.ToString(CultureInfo.InvariantCulture),
                    share > 0 ? $"{share.ToString("F1", CultureInfo.InvariantCulture)}%" : "—");
                index++;
            }

            table.Caption = new TableTitle($"[grey]{snapshot.Count} unique • {totalCount} total[/]");

            AnsiConsole.WriteLine();
            AnsiConsole.Write(table);
            return;
        }

        Output(string.Empty);
        WriteLine(StdOut, "[xtraq] Warning summary", ConsoleColor.Yellow);

        var rowIndex = 1;
        foreach (var entry in ordered)
        {
            var count = entry.Aggregate.Count;
            var share = totalCount > 0 ? $"{(count / (double)totalCount * 100d).ToString("F1", CultureInfo.InvariantCulture)}%" : "n/a";
            WriteLine(StdOut, $"  {rowIndex,2}. {entry.Message} (count={count}, share={share})", ConsoleColor.DarkYellow);
            rowIndex++;
        }

        Gray($"  unique={ordered.Count}, total={totalCount}");
    }

    public void RenderBreakdownChart(string title, IReadOnlyDictionary<string, double> segments, string? unit = null)
    {
        if (segments == null || segments.Count == 0)
        {
            return;
        }

        var safeTitle = string.IsNullOrWhiteSpace(title) ? "Breakdown" : title;

        if (UseSpectre)
        {
            var chart = new BreakdownChart().Width(60);
            var palette = new[]
            {
                new Color(46, 204, 113),   // green
                new Color(52, 152, 219),   // blue
                new Color(241, 196, 15),   // yellow
                new Color(155, 89, 182),   // purple
                new Color(26, 188, 156),   // teal
                new Color(230, 126, 34)    // orange
            };
            var colorIndex = 0;
            foreach (var segment in segments)
            {
                var color = palette[colorIndex++ % palette.Length];
                var item = new BreakdownChartItem(Escape(segment.Key), segment.Value, color);
                chart.AddItem(item);
            }

            var panel = new Panel(chart)
            {
                Header = new PanelHeader(Escape(safeTitle)),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey93),
                Padding = new Padding(1, 0, 1, 0)
            };

            AnsiConsole.Write(panel);
            AnsiConsole.WriteLine();
            return;
        }

        Output(safeTitle);
        var suffix = string.IsNullOrWhiteSpace(unit) ? string.Empty : $" {unit}";
        foreach (var segment in segments)
        {
            Output($"  - {segment.Key}: {segment.Value.ToString("F0", CultureInfo.InvariantCulture)}{suffix}");
        }
    }

    public void RenderBreakdownTable(string title, IReadOnlyDictionary<string, double> segments, string valueHeader, string? unit = null)
    {
        if (segments is null || segments.Count == 0)
        {
            return;
        }

        var ordered = segments
            .OrderByDescending(static pair => pair.Value)
            .ThenBy(static pair => pair.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var total = ordered.Sum(static pair => pair.Value);
        var safeTitle = string.IsNullOrWhiteSpace(title) ? "Breakdown" : title;
        var safeHeader = string.IsNullOrWhiteSpace(valueHeader) ? "Value" : valueHeader;

        if (UseSpectre)
        {
            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderStyle(new Style(Color.CadetBlue))
                .Title(new TableTitle(Escape(safeTitle)));

            table.AddColumn(new TableColumn("[grey]#[/]").Centered());
            table.AddColumn(new TableColumn("[grey]Label[/]").LeftAligned());
            table.AddColumn(new TableColumn($"[grey]{Escape(safeHeader)}[/]").RightAligned());
            table.AddColumn(new TableColumn("[grey]Share[/]").RightAligned());

            var index = 1;
            foreach (var entry in ordered)
            {
                var share = total > 0 ? (entry.Value / total) * 100d : 0d;
                table.AddRow(
                    index.ToString(CultureInfo.InvariantCulture),
                    Escape(entry.Key),
                    Escape(FormatValue(entry.Value, unit)),
                    share > 0 ? $"{share.ToString("F1", CultureInfo.InvariantCulture)}%" : "—");
                index++;
            }

            var caption = $"{ordered.Count.ToString(CultureInfo.InvariantCulture)} unique • {FormatValue(total, unit)} total";
            table.Caption = new TableTitle($"[grey]{Escape(caption)}[/]");

            AnsiConsole.WriteLine();
            AnsiConsole.Write(table);
            return;
        }

        Output(string.Empty);
        Output(safeTitle);

        var rowIndex = 1;
        foreach (var entry in ordered)
        {
            var share = total > 0 ? $"{(entry.Value / total * 100d).ToString("F1", CultureInfo.InvariantCulture)}%" : "n/a";
            Output($"  {rowIndex,2}. {entry.Key} -> {FormatValue(entry.Value, unit)} ({share})");
            rowIndex++;
        }

        Output($"  totals: unique={ordered.Count}, total={FormatValue(total, unit)}");
    }

    public void RenderKeyValueTable(string title, IReadOnlyDictionary<string, string> entries)
    {
        if (entries is null || entries.Count == 0)
        {
            return;
        }

        var safeTitle = string.IsNullOrWhiteSpace(title) ? "Values" : title;

        if (UseSpectre)
        {
            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderStyle(new Style(Color.Grey93))
                .Title(new TableTitle(Escape(safeTitle)));

            table.AddColumn(new TableColumn("[grey]Key[/]").LeftAligned());
            table.AddColumn(new TableColumn("[grey]Value[/]").LeftAligned());

            foreach (var entry in entries)
            {
                table.AddRow(Escape(entry.Key), Escape(entry.Value));
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
            return;
        }

        Output(safeTitle);
        foreach (var entry in entries)
        {
            WriteLine(StdOut, $"  - {entry.Key}: {entry.Value}", ConsoleColor.Gray);
        }
    }

    public void RenderFiglet(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        if (UseSpectre)
        {
            var figlet = new FigletText(text)
                .Color(Color.CadetBlue);
            AnsiConsole.Write(figlet);
            AnsiConsole.WriteLine();
            return;
        }

        Output(text);
        Output(string.Empty);
    }

    public void RenderJsonPayload(string title, string jsonPayload)
    {
        if (string.IsNullOrWhiteSpace(jsonPayload))
        {
            return;
        }

        var safeTitle = string.IsNullOrWhiteSpace(title) ? "JSON" : title;

        if (UseSpectre)
        {
            var jsonText = new Text(jsonPayload, new Style(Color.LightSteelBlue))
            {
                Justification = Justify.Left
            };
            var panel = new Panel(jsonText)
            {
                Header = new PanelHeader(Escape(safeTitle)),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey93),
                Padding = new Padding(1, 0, 1, 0)
            };

            AnsiConsole.Write(panel);
            AnsiConsole.WriteLine();
            return;
        }

        Output(safeTitle);
        foreach (var line in jsonPayload.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None))
        {
            if (line is { Length: > 0 })
            {
                WriteLine(StdOut, $"  {line}", ConsoleColor.Gray);
            }
            else
            {
                Output(string.Empty);
            }
        }
    }

    public async Task RunProgressAsync(string title, int totalUnits, Func<Action<double>, Task> workAsync, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workAsync);

        if (UseSpectre)
        {
            var safeTitle = string.IsNullOrWhiteSpace(title) ? "Progress" : title;
            var maxValue = totalUnits > 0 ? totalUnits : 100;

            await AnsiConsole.Progress()
                .AutoClear(false)
                .HideCompleted(false)
                .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn(), new RemainingTimeColumn(), new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask(Escape(safeTitle), maxValue: maxValue);
                    if (totalUnits <= 0)
                    {
                        task.IsIndeterminate = true;
                    }

                    void Advance(double incrementValue)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var delta = incrementValue <= 0 ? 1d : incrementValue;
                        if (totalUnits > 0)
                        {
                            var remaining = task.MaxValue - task.Value;
                            var increment = Math.Min(delta, remaining);
                            if (increment > 0)
                            {
                                task.Increment(increment);
                            }
                        }
                        else
                        {
                            task.Increment(delta);
                        }
                    }

                    await workAsync(Advance).ConfigureAwait(false);

                    if (totalUnits > 0)
                    {
                        task.Value = task.MaxValue;
                    }
                    else
                    {
                        task.StopTask();
                    }
                }).ConfigureAwait(false);
        }
        else
        {
            var safeTitle = string.IsNullOrWhiteSpace(title) ? "Progress" : title;
            Output($"{safeTitle}...");

            var total = totalUnits > 0 ? totalUnits : 0;
            var processed = 0;

            void Advance(double incrementValue)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var delta = incrementValue <= 0 ? 1 : (int)Math.Round(incrementValue);
                processed += delta;
                if (total > 0)
                {
                    var percent = Math.Clamp((int)Math.Round((double)processed / total * 100), 0, 100);
                    DrawProgressBar(percent);
                }
            }

            await workAsync(Advance).ConfigureAwait(false);

            if (total > 0)
            {
                DrawProgressBar(100);
            }

            Output($"{safeTitle} completed ({processed} items).");
        }
    }

    public void RunProgress(string title, int totalUnits, Action<Action<double>> work)
    {
        ArgumentNullException.ThrowIfNull(work);

        RunProgressAsync(title, totalUnits, advance =>
        {
            work(advance);
            return Task.CompletedTask;
        }).GetAwaiter().GetResult();
    }

    private Choice GetSelectionInternal(string prompt, List<string> options, bool multiline)
    {
        if (options == null || options.Count == 0)
        {
            throw new ArgumentException("Options list cannot be empty", nameof(options));
        }

        if (multiline)
        {
            Output(prompt);
            for (var i = 0; i < options.Count; i++)
            {
                Output($"  [{i + 1}] {options[i]}");
            }
        }
        else
        {
            Output($"{prompt} [{string.Join(", ", options)}]");
        }

        while (true)
        {
            Write(StdOut, "Select option (number): ", ConsoleColor.Green);
            var line = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(line) && int.TryParse(line, out var index))
            {
                index -= 1;
                if (index >= 0 && index < options.Count)
                {
                    return new Choice(index, options[index]);
                }
            }

            Warn("Invalid selection, please enter the number shown in the list.");
        }
    }

    private static string Escape(string? value) => Markup.Escape(value ?? string.Empty);

    private void WriteLine(TextWriter writer, string message, ConsoleColor? foregroundColor, ConsoleColor? backgroundColor = null)
    {
        lock (_writeLock)
        {
            TrySetColors(foregroundColor, backgroundColor);
            writer.WriteLine(message);
            if (foregroundColor.HasValue || backgroundColor.HasValue)
            {
                TryResetColors();
            }
        }
    }

    private void Write(TextWriter writer, string text, ConsoleColor? foregroundColor, ConsoleColor? backgroundColor = null)
    {
        lock (_writeLock)
        {
            TrySetColors(foregroundColor, backgroundColor);
            writer.Write(text);
            if (foregroundColor.HasValue || backgroundColor.HasValue)
            {
                TryResetColors();
            }
        }
    }

    private void RecordWarning(string? message, int occurrences = 1)
    {
        if (string.IsNullOrWhiteSpace(message) || occurrences <= 0)
        {
            return;
        }

        lock (_warningLock)
        {
            if (_warnings.TryGetValue(message, out var aggregate))
            {
                aggregate.Count += occurrences;
            }
            else
            {
                aggregate = new WarningAggregate
                {
                    Count = occurrences,
                    Order = _warningOrder++
                };
                _warnings[message] = aggregate;
            }
        }
    }

    private static void TrySetColors(ConsoleColor? foregroundColor, ConsoleColor? backgroundColor)
    {
        try
        {
            if (foregroundColor.HasValue)
            {
                Console.ForegroundColor = foregroundColor.Value;
            }

            if (backgroundColor.HasValue)
            {
                Console.BackgroundColor = backgroundColor.Value;
            }
        }
        catch
        {
            // ignore coloring failures
        }
    }

    private static void TryResetColors()
    {
        try
        {
            Console.ResetColor();
        }
        catch
        {
            // ignore reset failures (e.g. redirected output)
        }
    }

    private static string FormatValue(double value, string? unit)
    {
        var rounded = Math.Abs(value % 1d) < 0.001d
            ? value.ToString("F0", CultureInfo.InvariantCulture)
            : value.ToString("F1", CultureInfo.InvariantCulture);

        if (!string.IsNullOrWhiteSpace(unit))
        {
            return string.Concat(rounded, " ", unit);
        }

        return rounded;
    }

    private sealed class ConsoleProgressScope : IConsoleProgressScope
    {
        private readonly ConsoleService _owner;
        private int _completed;

        public ConsoleProgressScope(ConsoleService owner)
        {
            _owner = owner;
        }

        public void Complete(bool success = true, string? message = null)
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
            {
                return;
            }

            _owner.CompleteProgress(success, message);
        }

        public void Dispose()
        {
            Complete();
        }
    }

    private sealed class WarningAggregate
    {
        public int Count { get; set; }

        public int Order { get; set; }
    }
}

/// <summary>
/// Simple container representing an answer chosen by the user.
/// </summary>
internal sealed class Choice
{
    public Choice(int key, string value)
    {
        Key = key;
        Value = value;
    }

    public int Key { get; set; }
    public string Value { get; set; }
}

