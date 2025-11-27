using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using Xtraq.Cli.Commands;
using Xtraq.Configuration;
using Xtraq.Infrastructure;
using Xtraq.Runtime;
using Xtraq.Services;
using Xtraq.Telemetry;
using Xtraq.Utils;

namespace Xtraq.Cli.Hosting;

/// <summary>
/// Builds the System.CommandLine command tree and associated handlers for the Xtraq CLI.
/// </summary>
internal sealed class CliCommandAppBuilder
{
    private readonly CliHostContext _hostContext;
    private readonly IServiceProvider _services;
    private readonly XtraqCliRuntime _runtime;
    private readonly CommandOptions _commandOptionsAccessor;
    private readonly ICliTelemetryService _cliTelemetry;
    private readonly string _environment;

    private Option<bool> _verboseOption = null!;
    private Option<bool> _debugOption = null!;
    private Option<bool> _debugAliasOption = null!;
    private Option<bool> _noCacheOption = null!;
    private Option<bool> _noUpdateOption = null!;
    private Option<string?> _procedureOption = null!;
    private Option<bool> _telemetryOption = null!;
    private Option<bool> _jsonIncludeNullValuesOption = null!;
    private Option<bool> _entityFrameworkOption = null!;
    private Option<bool> _ciOption = null!;
    private Option<string?> _projectOption = null!;

    private CliCommandDescriptor _initDescriptor = null!;
    private CliCommandDescriptor _buildDescriptor = null!;
    private CliCommandDescriptor _snapshotDescriptor = null!;
    private CliCommandDescriptor _versionDescriptor = null!;
    private CliCommandDescriptor _updateDescriptor = null!;

    private IXtraqCommand _buildCommandHandler = null!;
    private IXtraqCommand _snapshotCommandHandler = null!;

    public CliCommandAppBuilder(CliHostContext hostContext)
    {
        _hostContext = hostContext ?? throw new ArgumentNullException(nameof(hostContext));
        _services = hostContext.Services ?? throw new ArgumentNullException(nameof(hostContext.Services));
        _runtime = _services.GetRequiredService<XtraqCliRuntime>();
        _commandOptionsAccessor = _services.GetRequiredService<CommandOptions>();
        _cliTelemetry = _services.GetRequiredService<ICliTelemetryService>();
        _environment = hostContext.EnvironmentName;
    }

    public RootCommand Build()
    {
        InitializeOptions();
        InitializeDescriptors();
        return CreateCommandTree();
    }

    private void InitializeOptions()
    {
        _verboseOption = new Option<bool>("--verbose", () => false, "Show additional diagnostic information");
        _verboseOption.AddAlias("-v");

        _debugOption = new Option<bool>("--debug", "Use debug environment settings");
        _debugAliasOption = new Option<bool>("--debug-alias", "Enable alias scope debug logging (promotes XTRAQ_LOG_LEVEL to debug)");

        _noCacheOption = new Option<bool>("--no-cache", "Do not read or write the local procedure metadata cache");
        _noUpdateOption = new Option<bool>("--no-update", "Skip update checks and prompts for this run");

        _procedureOption = new Option<string?>("--procedure", "Process only specific procedures (comma separated schema.name with optional '*' or '?' wildcards)");
        _procedureOption.AddValidator(result =>
        {
            var rawValue = result.GetValueOrDefault<string?>();
            if (!CliHostUtilities.TryNormalizeProcedureFilter(rawValue, out _, out var error) && !string.IsNullOrEmpty(error))
            {
                result.ErrorMessage = error;
            }
        });

        _telemetryOption = new Option<bool>("--telemetry", "Persist a database telemetry report to .xtraq/telemetry");
        _jsonIncludeNullValuesOption = new Option<bool>("--json-include-null-values", "Emit JsonIncludeNullValues attribute for JSON result properties");
        _entityFrameworkOption = new Option<bool>("--entity-framework", "Enable Entity Framework integration helper generation (sets XTRAQ_ENTITY_FRAMEWORK_ENABLED)");
        _ciOption = new Option<bool>("--ci", "Disable Spectre.Console enhancements for CI/plain output modes");

        _projectOption = new Option<string?>("--project-path", "Project root path (.env file or directory). Defaults to current directory when omitted.");
        _projectOption.AddAlias("--project");
        _projectOption.AddAlias("-p");
        _projectOption.AddValidator(result =>
        {
            if (result.IsImplicit)
            {
                return;
            }

            if (result.Tokens.Count == 0)
            {
                result.ErrorMessage = "Option '-p|--project' requires a path argument.";
                return;
            }

            var invalidToken = result.Tokens.FirstOrDefault(token => token.Value.StartsWith("-", StringComparison.Ordinal));
            if (invalidToken is not null)
            {
                result.ErrorMessage = "Option '-p|--project' requires a path argument.";
            }
        });
    }

    private void InitializeDescriptors()
    {
        _initDescriptor = CliCommandCatalog.Get(CliCommandKind.Init);
        _buildDescriptor = CliCommandCatalog.Get(CliCommandKind.Build);
        _snapshotDescriptor = CliCommandCatalog.Get(CliCommandKind.Snapshot);
        _versionDescriptor = CliCommandCatalog.Get(CliCommandKind.Version);
        _updateDescriptor = CliCommandCatalog.Get(CliCommandKind.Update);

        _buildCommandHandler = (IXtraqCommand)_services.GetRequiredService(_buildDescriptor.HandlerType!);
        _snapshotCommandHandler = (IXtraqCommand)_services.GetRequiredService(_snapshotDescriptor.HandlerType!);
    }

    private RootCommand CreateCommandTree()
    {
        var root = new RootCommand("Xtraq CLI")
        {
            TreatUnmatchedTokensAsErrors = true
        };

        root.AddGlobalOption(_verboseOption);
        root.AddGlobalOption(_debugOption);
        root.AddGlobalOption(_debugAliasOption);
        root.AddGlobalOption(_noCacheOption);
        root.AddGlobalOption(_noUpdateOption);
        root.AddGlobalOption(_procedureOption);
        root.AddGlobalOption(_telemetryOption);
        root.AddGlobalOption(_jsonIncludeNullValuesOption);
        root.AddGlobalOption(_entityFrameworkOption);
        root.AddGlobalOption(_ciOption);
        root.AddGlobalOption(_projectOption);

        ConfigureDefaultHandler(root);
        ConfigureSnapshotCommand(root);
        ConfigureBuildCommand(root);
        ConfigureVersionCommand(root);
        ConfigureUpdateCommand(root);
        ConfigureInitCommand(root);

        return root;
    }

    private void ConfigureDefaultHandler(RootCommand root)
    {
        root.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(invocationContext, _buildDescriptor, _buildCommandHandler, null, null, defaultRefresh: true).ConfigureAwait(false);
        });
    }

    private void ConfigureSnapshotCommand(RootCommand root)
    {
        var snapshotProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");
        var snapshotCommand = new Command(_snapshotDescriptor.Name, _snapshotDescriptor.Description);
        snapshotCommand.AddArgument(snapshotProjectArgument);
        snapshotCommand.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(invocationContext, _snapshotDescriptor, _snapshotCommandHandler, snapshotProjectArgument, null, defaultRefresh: false).ConfigureAwait(false);
        });
        root.AddCommand(snapshotCommand);
    }

    private void ConfigureBuildCommand(RootCommand root)
    {
        var buildProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");
        var refreshSnapshotOption = new Option<bool>("--refresh-snapshot", () => false, "Refresh snapshot before executing the build command");

        var buildCommand = new Command(_buildDescriptor.Name, _buildDescriptor.Description);
        buildCommand.AddArgument(buildProjectArgument);
        buildCommand.AddOption(refreshSnapshotOption);
        buildCommand.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(
                invocationContext,
                _buildDescriptor,
                _buildCommandHandler,
                buildProjectArgument,
                refreshSnapshotOption,
                defaultRefresh: false).ConfigureAwait(false);
        });
        root.AddCommand(buildCommand);
    }

    private void ConfigureVersionCommand(RootCommand root)
    {
        var versionCommand = new Command(_versionDescriptor.Name, _versionDescriptor.Description);
        versionCommand.AddOption(_verboseOption);
        versionCommand.SetHandler(async context =>
        {
            var options = CreateBaseOptions(context.ParseResult);
            _commandOptionsAccessor.Update(options);

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await _runtime.GetVersionAsync().ConfigureAwait(false));
                context.ExitCode = result;
                exitCode = result;
            }
            catch
            {
                exitCode = ExitCodes.InternalError;
                throw;
            }
            finally
            {
                stopwatch.Stop();
                try
                {
                    var telemetryEvent = new CliTelemetryEvent(
                        _versionDescriptor.Name,
                        CliHostUtilities.ResolveProductVersion(),
                        exitCode == ExitCodes.Success,
                        stopwatch.Elapsed,
                        null,
                        options.CiMode,
                        options.Telemetry,
                        options.Verbose,
                        options.NoCache,
                        options.EntityFrameworkIntegration,
                        RefreshSnapshotRequested: false,
                        string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure,
                        null);
                    await _cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });
        root.AddCommand(versionCommand);
    }

    private void ConfigureUpdateCommand(RootCommand root)
    {
        var updateCommand = new Command(_updateDescriptor.Name, _updateDescriptor.Description);
        updateCommand.AddOption(_verboseOption);
        updateCommand.SetHandler(async context =>
        {
            var options = CreateBaseOptions(context.ParseResult);
            _commandOptionsAccessor.Update(options);

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await _runtime.UpdateAsync(options).ConfigureAwait(false));
                context.ExitCode = result;
                exitCode = result;
            }
            catch
            {
                exitCode = ExitCodes.InternalError;
                throw;
            }
            finally
            {
                stopwatch.Stop();
                try
                {
                    var telemetryEvent = new CliTelemetryEvent(
                        _updateDescriptor.Name,
                        CliHostUtilities.ResolveProductVersion(),
                        exitCode == ExitCodes.Success,
                        stopwatch.Elapsed,
                        null,
                        options.CiMode,
                        options.Telemetry,
                        options.Verbose,
                        options.NoCache,
                        options.EntityFrameworkIntegration,
                        RefreshSnapshotRequested: false,
                        string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure,
                        null);
                    await _cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });
        root.AddCommand(updateCommand);
    }

    private void ConfigureInitCommand(RootCommand root)
    {
        var initCommand = new Command(_initDescriptor.Name, _initDescriptor.Description);
        var initProjectArgument = CreateOptionalProjectArgument("Target directory or .env file. Defaults to current directory when omitted.");
        initCommand.AddArgument(initProjectArgument);

        var initForceOption = new Option<bool>("--force", "Overwrite existing .env");
        initForceOption.AddAlias("-f");
        initCommand.AddOption(initForceOption);

        var namespaceOption = new Option<string?>("--namespace", "Root namespace (XTRAQ_NAMESPACE)");
        namespaceOption.AddAlias("-n");
        initCommand.AddOption(namespaceOption);

        var connectionOption = new Option<string?>("--connection", "Snapshot connection string (XTRAQ_GENERATOR_DB)");
        connectionOption.AddAlias("-c");
        initCommand.AddOption(connectionOption);

        var schemasOption = new Option<string?>("--schemas", "Comma separated allow-list (XTRAQ_BUILD_SCHEMAS)");
        schemasOption.AddAlias("-s");
        initCommand.AddOption(schemasOption);

        initCommand.SetHandler(async context =>
        {
            var options = CreateBaseOptions(context.ParseResult);
            _commandOptionsAccessor.Update(options);
            var console = _services.GetRequiredService<IConsoleService>();

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            string? telemetryProjectRoot = null;
            var force = context.ParseResult.GetValueForOption(initForceOption);
            var namespaceProvided = false;
            var connectionProvided = false;
            var schemasProvided = false;

            try
            {
                var targetPath = context.ParseResult.GetValueForArgument(initProjectArgument)?.Trim();
                if (string.IsNullOrWhiteSpace(targetPath))
                {
                    targetPath = context.ParseResult.GetValueForOption(_projectOption)?.Trim();
                }

                var nsValue = context.ParseResult.GetValueForOption(namespaceOption)?.Trim();
                var connection = context.ParseResult.GetValueForOption(connectionOption)?.Trim();
                var schemas = context.ParseResult.GetValueForOption(schemasOption)?.Trim();

                namespaceProvided = !string.IsNullOrWhiteSpace(nsValue);
                connectionProvided = !string.IsNullOrWhiteSpace(connection);

                if (!connectionProvided && !options.CiMode)
                {
                    console.Output("[xtraq init] No connection string provided.");
                    var entered = console.GetString("Enter XTRAQ_GENERATOR_DB connection string", defaultValue: string.Empty);
                    if (!string.IsNullOrWhiteSpace(entered))
                    {
                        connection = entered.Trim();
                        connectionProvided = true;
                    }
                }

                var effectivePath = CliHostUtilities.NormalizeProjectPath(targetPath);
                var resolved = DirectoryUtils.IsPath(effectivePath) ? effectivePath : Path.GetFullPath(effectivePath);
                Directory.CreateDirectory(resolved);
                telemetryProjectRoot = resolved;

                var envPath = await ProjectEnvironmentBootstrapper.EnsureEnvAsync(resolved, autoApprove: true, force: force, connectionString: connection).ConfigureAwait(false);
                var examplePath = ProjectEnvironmentBootstrapper.EnsureEnvExample(resolved, force);

                try
                {
                    var lines = File.ReadAllLines(envPath);

                    static string NormalizeKey(string key) => key.Trim().ToUpperInvariant();

                    void Upsert(string key, string value)
                    {
                        var normalized = NormalizeKey(key);
                        for (int i = 0; i < lines.Length; i++)
                        {
                            var line = lines[i];
                            if (line.TrimStart().StartsWith(normalized + "=", StringComparison.OrdinalIgnoreCase))
                            {
                                lines[i] = normalized + "=" + value;
                                return;
                            }
                        }

                        Array.Resize(ref lines, lines.Length + 1);
                        lines[^1] = normalized + "=" + value;
                    }

                    if (!string.IsNullOrWhiteSpace(connection))
                    {
                        Upsert("XTRAQ_GENERATOR_DB", connection);
                    }

                    File.WriteAllLines(envPath, lines);

                    try
                    {
                        var envValues = Xtraq.Configuration.TrackableConfigManager.BuildEnvMap(lines);

                        if (!string.IsNullOrWhiteSpace(nsValue))
                        {
                            envValues["XTRAQ_NAMESPACE"] = nsValue;
                        }

                        if (!string.IsNullOrWhiteSpace(schemas))
                        {
                            var normalizedSchemas = string.Join(',', schemas
                                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                                .Select(static s => s.Trim())
                                .Where(static s => s.Length > 0)
                                .Distinct(StringComparer.OrdinalIgnoreCase));

                            if (!string.IsNullOrWhiteSpace(normalizedSchemas))
                            {
                                envValues["XTRAQ_BUILD_SCHEMAS"] = normalizedSchemas;
                                schemasProvided = true;
                            }
                        }

                        Xtraq.Configuration.TrackableConfigManager.Write(resolved, envValues);
                        ProjectEnvironmentBootstrapper.EnsureProjectGitignore(resolved);
                        Console.WriteLine($"[xtraq init] Trackable config updated at {Path.Combine(resolved, ".xtraqconfig")}");
                    }
                    catch (Exception configEx)
                    {
                        Console.Error.WriteLine($"[xtraq init warn] trackable config failed: {configEx.Message}");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[xtraq init warn] post-processing .env failed: {ex.Message}");
                }

                Console.WriteLine($"[xtraq init] .env ready at {envPath}");
                Console.WriteLine($"[xtraq init] Template available at {examplePath}");
                Console.WriteLine("JSON helpers ship enabled by default; no preview flags required.");
                Console.WriteLine("Next: run 'xtraq snapshot' followed by 'xtraq build' (or just 'xtraq').");
                DirectoryUtils.ResetBasePath();
                context.ExitCode = ExitCodes.Success;
                exitCode = ExitCodes.Success;
            }
            catch
            {
                exitCode = ExitCodes.InternalError;
                throw;
            }
            finally
            {
                stopwatch.Stop();
                try
                {
                    var metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["force"] = force ? "true" : "false",
                        ["namespaceProvided"] = namespaceProvided ? "true" : "false",
                        ["connectionProvided"] = connectionProvided ? "true" : "false",
                        ["schemasProvided"] = schemasProvided ? "true" : "false"
                    };
                    var telemetryEvent = new CliTelemetryEvent(
                        _initDescriptor.Name,
                        CliHostUtilities.ResolveProductVersion(),
                        exitCode == ExitCodes.Success,
                        stopwatch.Elapsed,
                        telemetryProjectRoot,
                        options.CiMode,
                        options.Telemetry,
                        options.Verbose,
                        options.NoCache,
                        options.EntityFrameworkIntegration,
                        RefreshSnapshotRequested: false,
                        string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure,
                        metadata);
                    await _cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });

        root.AddCommand(initCommand);
    }

    private CliCommandOptions CreateBaseOptions(ParseResult parseResult)
    {
        var options = new CliCommandOptions
        {
            Path = Directory.GetCurrentDirectory(),
            Verbose = parseResult.GetValueForOption(_verboseOption),
            Debug = parseResult.GetValueForOption(_debugOption),
            NoCache = parseResult.GetValueForOption(_noCacheOption),
            NoUpdate = parseResult.GetValueForOption(_noUpdateOption),
            Procedure = CliHostUtilities.NormalizeProcedureFilter(parseResult.GetValueForOption(_procedureOption)),
            Telemetry = parseResult.GetValueForOption(_telemetryOption),
            JsonIncludeNullValues = parseResult.GetValueForOption(_jsonIncludeNullValuesOption),
            HasJsonIncludeNullValuesOverride = parseResult.HasOption(_jsonIncludeNullValuesOption),
            EntityFrameworkIntegration = parseResult.GetValueForOption(_entityFrameworkOption),
            HasEntityFrameworkIntegrationOverride = parseResult.HasOption(_entityFrameworkOption),
            CiMode = parseResult.GetValueForOption(_ciOption)
        };

        return options;
    }

    private async Task ExecuteCommandAsync(
        InvocationContext invocationContext,
        CliCommandDescriptor descriptor,
        IXtraqCommand command,
        Argument<string?>? commandArgument,
        Option<bool>? refreshOption,
        bool defaultRefresh)
    {
        ApplyDebugAlias(invocationContext.ParseResult);

        var projectPath = descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath)
            ? ResolveProjectPath(invocationContext.ParseResult, commandArgument)
            : Directory.GetCurrentDirectory();
        var options = new CliCommandOptions
        {
            Path = projectPath,
            Verbose = invocationContext.ParseResult.GetValueForOption(_verboseOption),
            Debug = invocationContext.ParseResult.GetValueForOption(_debugOption),
            NoCache = invocationContext.ParseResult.GetValueForOption(_noCacheOption),
            NoUpdate = invocationContext.ParseResult.GetValueForOption(_noUpdateOption),
            Procedure = CliHostUtilities.NormalizeProcedureFilter(invocationContext.ParseResult.GetValueForOption(_procedureOption)),
            Telemetry = invocationContext.ParseResult.GetValueForOption(_telemetryOption),
            JsonIncludeNullValues = invocationContext.ParseResult.GetValueForOption(_jsonIncludeNullValuesOption),
            HasJsonIncludeNullValuesOverride = invocationContext.ParseResult.HasOption(_jsonIncludeNullValuesOption),
            EntityFrameworkIntegration = invocationContext.ParseResult.GetValueForOption(_entityFrameworkOption),
            HasEntityFrameworkIntegrationOverride = invocationContext.ParseResult.HasOption(_entityFrameworkOption),
            CiMode = invocationContext.ParseResult.GetValueForOption(_ciOption)
        };

        Task<UpdateInfo?>? updateCheckTask = null;
        if (descriptor.HasFeature(CliCommandFeatures.SchedulesUpdate) && !UpdateService.IsUpdateDisabled())
        {
            updateCheckTask = ScheduleUpdateCheck(options);
        }

        var shouldRefresh = defaultRefresh;
        if (descriptor.HasFeature(CliCommandFeatures.SupportsRefreshOption) && refreshOption is not null)
        {
            shouldRefresh = invocationContext.ParseResult.GetValueForOption(refreshOption);
        }

        if (descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath))
        {
            PrepareCommandEnvironment(options);
        }

        var console = _services.GetRequiredService<IConsoleService>();

        if (descriptor.Kind != CliCommandKind.Init && descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath))
        {
            var needsConnection = descriptor.Kind == CliCommandKind.Snapshot
                || (descriptor.Kind == CliCommandKind.Build && shouldRefresh);

            var initialized = await EnsureProjectInitializedAsync(projectPath, console, needsConnection).ConfigureAwait(false);
            if (!initialized)
            {
                invocationContext.ExitCode = ExitCodes.InternalError;
                return;
            }
        }

        _commandOptionsAccessor.Update(options);

        EmitSessionPreamble(console, descriptor.Name, options, projectPath, shouldRefresh);

        var commandContext = new XtraqCommandContext(
            projectPath,
            options,
            _services,
            console,
            shouldRefresh);

        var commandStopwatch = Stopwatch.StartNew();
        var exitCode = ExitCodes.InternalError;
        try
        {
            var result = await command.ExecuteAsync(commandContext, invocationContext.GetCancellationToken()).ConfigureAwait(false);
            invocationContext.ExitCode = result;
            exitCode = result;
        }
        catch
        {
            exitCode = ExitCodes.InternalError;
            throw;
        }
        finally
        {
            DirectoryUtils.ResetBasePath();
            commandStopwatch.Stop();

            try
            {
                var telemetryEvent = new CliTelemetryEvent(
                    descriptor.Name,
                    CliHostUtilities.ResolveProductVersion(),
                    exitCode == ExitCodes.Success,
                    commandStopwatch.Elapsed,
                    projectPath,
                    options.CiMode,
                    options.Telemetry,
                    options.Verbose,
                    options.NoCache,
                    options.EntityFrameworkIntegration,
                    shouldRefresh,
                    string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure,
                    null);
                await _cliTelemetry.CaptureAsync(telemetryEvent, invocationContext.GetCancellationToken()).ConfigureAwait(false);
            }
            catch (Exception telemetryEx)
            {
                console.Verbose($"telemetry capture failed: {telemetryEx.Message}");
            }
        }

        if (exitCode == ExitCodes.Success)
        {
            await PromptForUpdateAsync(updateCheckTask, options, invocationContext.GetCancellationToken()).ConfigureAwait(false);
        }
    }

    private async Task<bool> EnsureProjectInitializedAsync(string projectPath, IConsoleService console, bool requireConnection)
    {
        var configDirectory = TrackableConfigManager.LocateConfigDirectory(projectPath);
        var baseConfigPath = Path.Combine(configDirectory, ".xtraqconfig");
        if (File.Exists(baseConfigPath))
        {
            var resolvedRoot = TrackableConfigManager.ResolveRedirectTargets(configDirectory) ?? configDirectory;
            var resolvedConfigPath = Path.Combine(resolvedRoot, ".xtraqconfig");
            if (File.Exists(resolvedConfigPath))
            {
                return true;
            }

            // Redirected root missing tracked config -> treat as uninitialised.
            configDirectory = resolvedRoot;
        }
        else
        {
            // No tracked config anywhere.
            configDirectory = projectPath;
        }

        console.Error("Failed to load .xtraqconfig: Xtraq project is not initialised.");
        var consent = console.GetYesNo("Run xtraq init now?", isDefaultConfirmed: false);
        if (!consent)
        {
            return false;
        }

        string? connection = null;
        if (requireConnection)
        {
            console.Output("[xtraq init] Connection string is required for snapshot/refresh.");
            var entered = console.GetString("Enter XTRAQ_GENERATOR_DB connection string", defaultValue: string.Empty);
            if (string.IsNullOrWhiteSpace(entered))
            {
                console.Error("Aborted: missing connection string.");
                return false;
            }

            connection = entered.Trim();
        }

        try
        {
            var envPath = await ProjectEnvironmentBootstrapper.EnsureEnvAsync(configDirectory, autoApprove: true, connectionString: connection).ConfigureAwait(false);
            var examplePath = ProjectEnvironmentBootstrapper.EnsureEnvExample(configDirectory);
            ProjectEnvironmentBootstrapper.EnsureProjectGitignore(configDirectory);

            console.Output($"[xtraq init] .env ready at {envPath}");
            console.Output($"[xtraq init] Template available at {examplePath}");
            return true;
        }
        catch (Exception ex)
        {
            console.Error($"Init pipeline failed: {ex.Message}");
            return false;
        }
    }

    private static Argument<string?> CreateOptionalProjectArgument(string description)
    {
        var argument = new Argument<string?>("project-path")
        {
            Description = description,
            Arity = ArgumentArity.ZeroOrOne
        };

        return argument;
    }

    private void ApplyDebugAlias(ParseResult parseResult)
    {
        if (!parseResult.HasOption(_debugAliasOption))
        {
            return;
        }

        var debugAliasValue = parseResult.GetValueForOption(_debugAliasOption);
        if (debugAliasValue)
        {
            LogLevelConfiguration.PromoteTo(LogLevelThreshold.Debug);
        }
    }

    private void PrepareCommandEnvironment(CliCommandOptions options)
    {
        DirectoryUtils.SetBasePath(options.Path);
        CacheControl.ForceReload = options.NoCache;

        var procedureFilter = string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure;
        Environment.SetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES", procedureFilter);

        if (options.HasEntityFrameworkIntegrationOverride)
        {
            Environment.SetEnvironmentVariable("XTRAQ_ENTITY_FRAMEWORK_ENABLED", options.EntityFrameworkIntegration ? "1" : null);
        }
    }

    private Task<UpdateInfo?>? ScheduleUpdateCheck(CliCommandOptions options)
    {
        if (options.NoUpdate || UpdateService.IsUpdateDisabled())
        {
            return null;
        }

        return Task.Run(async () =>
        {
            try
            {
                var updateService = _services.GetRequiredService<UpdateService>();
                var updateInfo = await updateService.CheckForUpdateAsync().ConfigureAwait(false);
                return updateInfo;
            }
            catch
            {
                return null;
            }
        });
    }

    private async Task PromptForUpdateAsync(Task<UpdateInfo?>? updateCheckTask, CliCommandOptions options, CancellationToken cancellationToken)
    {
        if (updateCheckTask is null || options.NoUpdate)
        {
            return;
        }

        UpdateInfo? updateInfo;
        try
        {
            updateInfo = await updateCheckTask.ConfigureAwait(false);
        }
        catch
        {
            return;
        }

        if (updateInfo?.IsUpdateAvailable != true)
        {
            return;
        }

        var console = _services.GetRequiredService<IConsoleService>();

        if (options.CiMode)
        {
            console.Output($"[xtraq] Update available: {updateInfo.CurrentVersion} -> {updateInfo.LatestVersion} (CI mode - skipping prompt). Run 'xtraq update' when appropriate.");
            return;
        }

        console.Output(string.Empty);
        console.Output($"[xtraq] Update available: {updateInfo.CurrentVersion} -> {updateInfo.LatestVersion}");

        var confirm = console.GetYesNo("[xtraq] Apply update now?", true, ConsoleColor.Yellow);
        if (!confirm)
        {
            console.Output("[xtraq] Update skipped. Run 'xtraq update' later or pass --no-update to suppress prompts.");
            return;
        }

        // On Windows the shim executable is locked while this process runs; run the updater after exit.
        if (OperatingSystem.IsWindows())
        {
            var launched = TryLaunchPostExitUpdater(console);
            if (launched)
            {
                console.Output("[xtraq] Update will run after this command finishes. Leave the terminal open until it completes.");
            }
            else
            {
                console.Warn("[xtraq] Could not launch post-exit updater. Please run 'xtraq update' manually.");
            }
            return;
        }

        console.Output("[xtraq] Updating via dotnet tool...");

        var updateService = _services.GetRequiredService<UpdateService>();
        var succeeded = await updateService.UpdateAsync(cancellationToken).ConfigureAwait(false);
        if (succeeded)
        {
            console.Success($"[xtraq] Updated to {updateInfo.LatestVersion}. Restart your terminal to load the new version.");
        }
        else
        {
            console.Warn("[xtraq] Update failed. Try again with 'xtraq update' when convenient.");
        }
    }

    private bool TryLaunchPostExitUpdater(IConsoleService console)
    {
        try
        {
            var currentPid = Process.GetCurrentProcess().Id;
            string fileName;
            string arguments;

            if (OperatingSystem.IsWindows())
            {
                fileName = "powershell";
                arguments = $"-NoProfile -Command \"while (Get-Process -Id {currentPid} -ErrorAction SilentlyContinue) {{ Start-Sleep -Milliseconds 200 }}; dotnet tool update -g xtraq\"";
            }
            else
            {
                fileName = "/bin/sh";
                arguments = $"-c \"while kill -0 {currentPid} 2>/dev/null; do sleep 0.2; done; dotnet tool update -g xtraq\"";
            }

            var startInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = false,
                RedirectStandardError = false,
                CreateNoWindow = true
            };

            return Process.Start(startInfo) is not null;
        }
        catch (Exception ex)
        {
            console.Warn($"[xtraq] Post-exit updater failed to start: {ex.Message}");
            return false;
        }
    }

    private string ResolveProjectPath(ParseResult parseResult, Argument<string?>? commandArgument)
    {
        string? candidate = null;
        if (commandArgument is not null)
        {
            candidate = parseResult.GetValueForArgument(commandArgument);
        }

        if (string.IsNullOrWhiteSpace(candidate))
        {
            candidate = parseResult.GetValueForOption(_projectOption);
        }

        return CliHostUtilities.NormalizeProjectPath(candidate);
    }

    private void EmitSessionPreamble(IConsoleService console, string commandName, CliCommandOptions options, string projectPath, bool refreshRequested)
    {
        ArgumentNullException.ThrowIfNull(console);

        var banner = CliHostUtilities.ResolveProductBanner();
        console.RenderFiglet(banner);

        var metadataJson = CliHostUtilities.BuildSessionMetadataJson(commandName, options, projectPath, _environment, refreshRequested);
        console.RenderJsonPayload($"{(string.IsNullOrWhiteSpace(commandName) ? "session" : commandName)} metadata", metadataJson);
    }
}
