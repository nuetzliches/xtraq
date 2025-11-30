using System.CommandLine;
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
        _verboseOption = new Option<bool>("--verbose")
        {
            Description = "Show additional diagnostic information",
            Recursive = true
        };
        _verboseOption.Aliases.Add("-v");

        _debugOption = new Option<bool>("--debug")
        {
            Description = "Use debug environment settings",
            Recursive = true
        };
        _debugAliasOption = new Option<bool>("--debug-alias")
        {
            Description = "Enable alias scope debug logging (promotes XTRAQ_LOG_LEVEL to debug)",
            Recursive = true
        };

        _noCacheOption = new Option<bool>("--no-cache")
        {
            Description = "Do not read or write the local procedure metadata cache",
            Recursive = true
        };
        _noUpdateOption = new Option<bool>("--no-update")
        {
            Description = "Skip update checks and prompts for this run",
            Recursive = true
        };

        _procedureOption = new Option<string?>("--procedure")
        {
            Description = "Process only specific procedures (comma separated schema.name with optional '*' or '?' wildcards)",
            Recursive = true
        };
        _procedureOption.Validators.Add(result =>
        {
            var rawValue = result.GetValueOrDefault<string?>();
            if (!CliHostUtilities.TryNormalizeProcedureFilter(rawValue, out _, out var error) && !string.IsNullOrEmpty(error))
            {
                result.AddError(error);
            }
        });

        _telemetryOption = new Option<bool>("--telemetry")
        {
            Description = "Persist a database telemetry report to .xtraq/telemetry",
            Recursive = true
        };
        _jsonIncludeNullValuesOption = new Option<bool>("--json-include-null-values")
        {
            Description = "Emit JsonIncludeNullValues attribute for JSON result properties",
            Recursive = true
        };
        _entityFrameworkOption = new Option<bool>("--entity-framework")
        {
            Description = "Enable Entity Framework integration helper generation (sets XTRAQ_ENTITY_FRAMEWORK_ENABLED)",
            Recursive = true
        };
        _ciOption = new Option<bool>("--ci")
        {
            Description = "Disable Spectre.Console enhancements for CI/plain output modes",
            Recursive = true
        };

        _projectOption = new Option<string?>("--project-path")
        {
            Description = "Project root path (.env file or directory). Defaults to current directory when omitted.",
            Recursive = true
        };
        _projectOption.Aliases.Add("--project");
        _projectOption.Aliases.Add("-p");
        _projectOption.Validators.Add(result =>
        {
            if (result.Implicit)
            {
                return;
            }

            if (result.Tokens.Count == 0)
            {
                result.AddError("Option '-p|--project' requires a path argument.");
                return;
            }

            var invalidToken = result.Tokens.FirstOrDefault(token => token.Value.StartsWith("-", StringComparison.Ordinal));
            if (invalidToken is not null)
            {
                result.AddError("Option '-p|--project' requires a path argument.");
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

        root.Add(_verboseOption);
        root.Add(_debugOption);
        root.Add(_debugAliasOption);
        root.Add(_noCacheOption);
        root.Add(_noUpdateOption);
        root.Add(_procedureOption);
        root.Add(_telemetryOption);
        root.Add(_jsonIncludeNullValuesOption);
        root.Add(_entityFrameworkOption);
        root.Add(_ciOption);
        root.Add(_projectOption);

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
        root.SetAction(async (parseResult, cancellationToken) =>
        {
            return await ExecuteCommandAsync(parseResult, cancellationToken, _buildDescriptor, _buildCommandHandler, null, null, defaultRefresh: true).ConfigureAwait(false);
        });
    }

    private void ConfigureSnapshotCommand(RootCommand root)
    {
        var snapshotProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");
        var snapshotCommand = new Command(_snapshotDescriptor.Name, _snapshotDescriptor.Description);
        snapshotCommand.Add(snapshotProjectArgument);
        snapshotCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            return await ExecuteCommandAsync(parseResult, cancellationToken, _snapshotDescriptor, _snapshotCommandHandler, snapshotProjectArgument, null, defaultRefresh: false).ConfigureAwait(false);
        });
        root.Add(snapshotCommand);
    }

    private void ConfigureBuildCommand(RootCommand root)
    {
        var buildProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");
        var refreshSnapshotOption = new Option<bool>("--refresh-snapshot")
        {
            Description = "Refresh snapshot before executing the build command"
        };

        var buildCommand = new Command(_buildDescriptor.Name, _buildDescriptor.Description);
        buildCommand.Add(buildProjectArgument);
        buildCommand.Add(refreshSnapshotOption);
        buildCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            return await ExecuteCommandAsync(
                parseResult,
                cancellationToken,
                _buildDescriptor,
                _buildCommandHandler,
                buildProjectArgument,
                refreshSnapshotOption,
                defaultRefresh: false).ConfigureAwait(false);
        });
        root.Add(buildCommand);
    }

    private void ConfigureVersionCommand(RootCommand root)
    {
        var versionCommand = new Command(_versionDescriptor.Name, _versionDescriptor.Description);
        versionCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var options = CreateBaseOptions(parseResult);
            _commandOptionsAccessor.Update(options);

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await _runtime.GetVersionAsync().ConfigureAwait(false));
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
                    await _cliTelemetry.CaptureAsync(telemetryEvent, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }

            return exitCode;
        });
        root.Add(versionCommand);
    }

    private void ConfigureUpdateCommand(RootCommand root)
    {
        var updateCommand = new Command(_updateDescriptor.Name, _updateDescriptor.Description);
        updateCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var options = CreateBaseOptions(parseResult);
            _commandOptionsAccessor.Update(options);

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await _runtime.UpdateAsync(options).ConfigureAwait(false));
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
                    await _cliTelemetry.CaptureAsync(telemetryEvent, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }

            return exitCode;
        });
        root.Add(updateCommand);
    }

    private void ConfigureInitCommand(RootCommand root)
    {
        var initCommand = new Command(_initDescriptor.Name, _initDescriptor.Description);
        var initProjectArgument = CreateOptionalProjectArgument("Target directory or .env file. Defaults to current directory when omitted.");
        initCommand.Add(initProjectArgument);

        var initForceOption = new Option<bool>("--force")
        {
            Description = "Overwrite existing .env"
        };
        initForceOption.Aliases.Add("-f");
        initCommand.Add(initForceOption);

        var namespaceOption = new Option<string?>("--namespace")
        {
            Description = "Root namespace (XTRAQ_NAMESPACE)"
        };
        namespaceOption.Aliases.Add("-n");
        initCommand.Add(namespaceOption);

        var connectionOption = new Option<string?>("--connection")
        {
            Description = "Snapshot connection string (XTRAQ_GENERATOR_DB)"
        };
        connectionOption.Aliases.Add("-c");
        initCommand.Add(connectionOption);

        var schemasOption = new Option<string?>("--schemas")
        {
            Description = "Comma separated allow-list (XTRAQ_BUILD_SCHEMAS)"
        };
        schemasOption.Aliases.Add("-s");
        initCommand.Add(schemasOption);

        initCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var options = CreateBaseOptions(parseResult);
            _commandOptionsAccessor.Update(options);
            var console = _services.GetRequiredService<IConsoleService>();

            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            string? telemetryProjectRoot = null;
            var force = parseResult.GetValue(initForceOption);
            var namespaceProvided = false;
            var connectionProvided = false;
            var schemasProvided = false;

            try
            {
                var targetPath = parseResult.GetValue(initProjectArgument)?.Trim();
                if (string.IsNullOrWhiteSpace(targetPath))
                {
                    targetPath = parseResult.GetValue(_projectOption)?.Trim();
                }

                var nsValue = parseResult.GetValue(namespaceOption)?.Trim();
                var connection = parseResult.GetValue(connectionOption)?.Trim();
                var schemas = parseResult.GetValue(schemasOption)?.Trim();

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
                    await _cliTelemetry.CaptureAsync(telemetryEvent, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    _services.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }

            return exitCode;
        });

        root.Add(initCommand);
    }

    private CliCommandOptions CreateBaseOptions(ParseResult parseResult)
    {
        var options = new CliCommandOptions
        {
            Path = Directory.GetCurrentDirectory(),
            Verbose = parseResult.GetValue(_verboseOption),
            Debug = parseResult.GetValue(_debugOption),
            NoCache = parseResult.GetValue(_noCacheOption),
            NoUpdate = parseResult.GetValue(_noUpdateOption),
            Procedure = CliHostUtilities.NormalizeProcedureFilter(parseResult.GetValue(_procedureOption)),
            Telemetry = parseResult.GetValue(_telemetryOption),
            JsonIncludeNullValues = parseResult.GetValue(_jsonIncludeNullValuesOption),
            HasJsonIncludeNullValuesOverride = parseResult.GetResult(_jsonIncludeNullValuesOption)?.Implicit is false,
            EntityFrameworkIntegration = parseResult.GetValue(_entityFrameworkOption),
            HasEntityFrameworkIntegrationOverride = parseResult.GetResult(_entityFrameworkOption)?.Implicit is false,
            CiMode = parseResult.GetValue(_ciOption)
        };

        return options;
    }

    private async Task<int> ExecuteCommandAsync(
        ParseResult parseResult,
        CancellationToken cancellationToken,
        CliCommandDescriptor descriptor,
        IXtraqCommand command,
        Argument<string?>? commandArgument,
        Option<bool>? refreshOption,
        bool defaultRefresh)
    {
        ApplyDebugAlias(parseResult);

        var projectPath = descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath)
            ? ResolveProjectPath(parseResult, commandArgument)
            : Directory.GetCurrentDirectory();
        var options = new CliCommandOptions
        {
            Path = projectPath,
            Verbose = parseResult.GetValue(_verboseOption),
            Debug = parseResult.GetValue(_debugOption),
            NoCache = parseResult.GetValue(_noCacheOption),
            NoUpdate = parseResult.GetValue(_noUpdateOption),
            Procedure = CliHostUtilities.NormalizeProcedureFilter(parseResult.GetValue(_procedureOption)),
            Telemetry = parseResult.GetValue(_telemetryOption),
            JsonIncludeNullValues = parseResult.GetValue(_jsonIncludeNullValuesOption),
            HasJsonIncludeNullValuesOverride = parseResult.GetResult(_jsonIncludeNullValuesOption)?.Implicit is false,
            EntityFrameworkIntegration = parseResult.GetValue(_entityFrameworkOption),
            HasEntityFrameworkIntegrationOverride = parseResult.GetResult(_entityFrameworkOption)?.Implicit is false,
            CiMode = parseResult.GetValue(_ciOption)
        };

        Task<UpdateInfo?>? updateCheckTask = null;
        if (descriptor.HasFeature(CliCommandFeatures.SchedulesUpdate) && !UpdateService.IsUpdateDisabled())
        {
            updateCheckTask = ScheduleUpdateCheck(options);
        }

        var shouldRefresh = defaultRefresh;
        if (descriptor.HasFeature(CliCommandFeatures.SupportsRefreshOption) && refreshOption is not null)
        {
            shouldRefresh = parseResult.GetValue(refreshOption);
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
                return ExitCodes.InternalError;
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
            var result = await command.ExecuteAsync(commandContext, cancellationToken).ConfigureAwait(false);
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
                await _cliTelemetry.CaptureAsync(telemetryEvent, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception telemetryEx)
            {
                console.Verbose($"telemetry capture failed: {telemetryEx.Message}");
            }
        }

        if (exitCode == ExitCodes.Success)
        {
            await PromptForUpdateAsync(updateCheckTask, options, cancellationToken).ConfigureAwait(false);
        }

        console.FlushDeferredPanels();
        return exitCode;
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
        var debugAliasResult = parseResult.GetResult(_debugAliasOption);
        if (debugAliasResult is null || debugAliasResult.Implicit)
        {
            return;
        }

        var debugAliasValue = parseResult.GetValue(_debugAliasOption);
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

        var currentVersion = FormatVersionLabel(updateInfo.CurrentVersion);
        var latestVersion = FormatVersionLabel(updateInfo.LatestVersion);

        if (options.CiMode)
        {
            console.RenderPanel("[xtraq] Update available", $"Current: {currentVersion}\nLatest: {latestVersion}\nCI mode - skipping prompt. Run 'xtraq update' when appropriate.");
            return;
        }

        var panelMessage = $"Current: {currentVersion}\nLatest: {latestVersion}\nRun 'xtraq update' to apply or pass --no-update to suppress this reminder.";
        if (console.IsPromptActive)
        {
            console.EnqueuePanel("[xtraq] Update available", panelMessage);
            return;
        }

        console.RenderPanel("[xtraq] Update available", panelMessage);

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
            candidate = parseResult.GetValue(commandArgument);
        }

        if (string.IsNullOrWhiteSpace(candidate))
        {
            candidate = parseResult.GetValue(_projectOption);
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

    private static string FormatVersionLabel(string? version)
    {
        if (string.IsNullOrWhiteSpace(version))
        {
            return string.Empty;
        }

        var plusIndex = version.IndexOf('+');
        return plusIndex >= 0 ? version.Substring(0, plusIndex) : version.Trim();
    }
}
