using Xtraq.Infrastructure;
using Xtraq.Runtime;
using Xtraq.Services;

namespace Xtraq.Cli;

/// <summary>
/// Interactive command palette prototype that lets users trigger common CLI workflows.
/// </summary>
internal sealed class CommandPalettePrototype
{
    private readonly IConsoleService _console;
    private readonly CommandOptions _commandOptions;
    private readonly XtraqCliRuntime _runtime;
    private readonly Action<CliCommandOptions> _prepareEnvironment;
    private readonly Action<CliCommandOptions> _scheduleUpdate;
    private readonly IReadOnlyList<CliCommandDescriptor> _entries;

    public CommandPalettePrototype(
        IConsoleService console,
        CommandOptions commandOptions,
        XtraqCliRuntime runtime,
        Action<CliCommandOptions> prepareEnvironment,
        Action<CliCommandOptions> scheduleUpdate)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _commandOptions = commandOptions ?? throw new ArgumentNullException(nameof(commandOptions));
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        _prepareEnvironment = prepareEnvironment ?? throw new ArgumentNullException(nameof(prepareEnvironment));
        _scheduleUpdate = scheduleUpdate ?? throw new ArgumentNullException(nameof(scheduleUpdate));
        _entries = CliCommandCatalog.All;
    }

    public async Task<int> ExecuteAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var entry = PromptForCommand();
        cancellationToken.ThrowIfCancellationRequested();

        switch (entry.Kind)
        {
            case CliCommandKind.Init:
                {
                    var defaultPath = string.IsNullOrWhiteSpace(_commandOptions.Path)
                        ? Directory.GetCurrentDirectory()
                        : _commandOptions.Path;
                    var targetPath = _console.GetString("Project path", defaultPath)?.Trim() ?? string.Empty;
                    var force = _console.GetYesNo("Overwrite existing .env (--force)?", false);
                    var ns = _console.GetString("Namespace (optional)", string.Empty)?.Trim() ?? string.Empty;
                    var connection = _console.GetString("Connection string (optional)", string.Empty)?.Trim() ?? string.Empty;
                    var schemas = _console.GetString("Schemas allow-list (optional)", string.Empty)?.Trim() ?? string.Empty;

                    var initArgs = new List<string>();
                    if (!string.IsNullOrWhiteSpace(targetPath))
                    {
                        initArgs.Add(targetPath);
                    }

                    initArgs.Add(entry.Name);

                    if (force)
                    {
                        initArgs.Add("--force");
                    }

                    if (!string.IsNullOrWhiteSpace(ns))
                    {
                        initArgs.Add("--namespace");
                        initArgs.Add(ns);
                    }

                    if (!string.IsNullOrWhiteSpace(connection))
                    {
                        initArgs.Add("--connection");
                        initArgs.Add(connection);
                    }

                    if (!string.IsNullOrWhiteSpace(schemas))
                    {
                        initArgs.Add("--schemas");
                        initArgs.Add(schemas);
                    }

                    var exit = await Program.RunCliAsync(initArgs.ToArray()).ConfigureAwait(false);

                    var updated = new CliCommandOptions
                    {
                        Path = targetPath
                    };
                    _commandOptions.Update(updated);

                    return exit;
                }
            case CliCommandKind.Snapshot:
                {
                    var options = GatherInteractiveOptions(entry);
                    ConfigureCommonState(options, entry);
                    var result = await _runtime.SnapshotAsync(options).ConfigureAwait(false);
                    return CommandResultMapper.Map(result);
                }
            case CliCommandKind.Build:
                {
                    var options = GatherInteractiveOptions(entry);
                    ConfigureCommonState(options, entry);
                    var refreshSnapshot = _console.GetYesNo("Refresh snapshot before building?", true);

                    if (refreshSnapshot)
                    {
                        var snapshotResult = await _runtime.SnapshotAsync(options).ConfigureAwait(false);
                        var snapshotExit = CommandResultMapper.Map(snapshotResult);
                        if (snapshotExit != ExitCodes.Success)
                        {
                            return snapshotExit;
                        }
                    }

                    var buildResult = await _runtime.BuildAsync(options).ConfigureAwait(false);
                    var buildExit = CommandResultMapper.Map(buildResult);
                    if (options.Telemetry && buildExit == ExitCodes.Success)
                    {
                        var label = refreshSnapshot ? "build" : "build-only";
                        await _runtime.PersistTelemetrySummaryAsync(label, cancellationToken).ConfigureAwait(false);
                    }

                    return buildExit;
                }
            case CliCommandKind.Version:
                {
                    var options = GatherInteractiveOptions(entry);
                    _commandOptions.Update(options);
                    var result = await _runtime.GetVersionAsync().ConfigureAwait(false);
                    return CommandResultMapper.Map(result);
                }
            case CliCommandKind.Update:
                {
                    var options = GatherInteractiveOptions(entry);
                    _commandOptions.Update(options);
                    var result = await _runtime.UpdateAsync(options).ConfigureAwait(false);
                    return CommandResultMapper.Map(result);
                }
            default:
                throw new InvalidOperationException($"Unsupported palette command kind: {entry.Kind}");
        }
    }

    private CliCommandDescriptor PromptForCommand()
    {
        var options = new List<string>(_entries.Count);
        foreach (var entry in _entries)
        {
            options.Add($"{entry.DisplayName.PadRight(10)} {entry.Description}");
        }

        var choice = _console.GetSelectionMultiline("Select command to execute", options);
        if (choice.Key < 0 || choice.Key >= _entries.Count)
        {
            throw new InvalidOperationException("Command palette selection out of range.");
        }

        return _entries[choice.Key];
    }

    private CliCommandOptions GatherInteractiveOptions(CliCommandDescriptor entry)
    {
        var options = new CliCommandOptions
        {
            Verbose = _console.GetYesNo("Enable verbose output?", _commandOptions.Verbose),
            Debug = _commandOptions.Debug,
            NoUpdate = false,
            CiMode = _commandOptions.CiMode
        };

        if (entry.HasFeature(CliCommandFeatures.RequiresProjectPath))
        {
            var defaultPath = string.IsNullOrWhiteSpace(_commandOptions.Path)
                ? Directory.GetCurrentDirectory()
                : _commandOptions.Path;
            var pathInput = _console.GetString("Project path", defaultPath);
            options.Path = string.IsNullOrWhiteSpace(pathInput) ? string.Empty : pathInput.Trim();
        }

        if (entry.HasFeature(CliCommandFeatures.SupportsCache))
        {
            options.NoCache = _console.GetYesNo("Skip metadata cache (--no-cache)?", _commandOptions.NoCache);
        }

        if (entry.HasFeature(CliCommandFeatures.SupportsTelemetry))
        {
            options.Telemetry = _console.GetYesNo("Persist telemetry report (--telemetry)?", _commandOptions.Telemetry);
        }

        if (entry.HasFeature(CliCommandFeatures.SupportsProcedureFilter))
        {
            var procedureDefault = _commandOptions.Procedure ?? string.Empty;
            var procedureInput = _console.GetString("Procedure filter (schema.name, optional)", procedureDefault);
            options.Procedure = string.IsNullOrWhiteSpace(procedureInput) ? string.Empty : procedureInput.Trim();
        }

        return options;
    }

    private void ConfigureCommonState(CliCommandOptions options, CliCommandDescriptor entry)
    {
        if (entry.HasFeature(CliCommandFeatures.RequiresProjectPath))
        {
            _prepareEnvironment(options);
        }

        if (entry.HasFeature(CliCommandFeatures.SchedulesUpdate))
        {
            _scheduleUpdate(options);
        }

        _commandOptions.Update(options);
    }
}
