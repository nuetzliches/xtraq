using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using Microsoft.Extensions.Configuration;
using Xtraq.Cli;
using Xtraq.Cli.Commands;
using Xtraq.Cli.Hosting;
using Xtraq.Data;
using Xtraq.Extensions;
using Xtraq.Infrastructure;
using Xtraq.Runtime;
using Xtraq.Services;
using Xtraq.Telemetry;
using Xtraq.Utils;

namespace Xtraq;

/// <summary>
/// Entry point wiring for the Xtraq command-line interface.
/// </summary>
public static class Program
{

    /// <summary>
    /// Executes the Xtraq CLI with the provided arguments.
    /// </summary>
    /// <param name="args">Command-line arguments received from the host.</param>
    /// <returns>Exit code emitted by the invoked command.</returns>
    public static async Task<int> RunCliAsync(string[] args)
    {
        var normalizedArgs = CliHostUtilities.NormalizeInvocationArguments(args);
        CliEnvironmentBootstrapper.Initialize(normalizedArgs);

        string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ??
                             Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ??
                             "Production";

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true)
            .Build();

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);

        services.AddXtraq();
        services.AddDbContext();

        services.AddSingleton<BuildCommand>();
        services.AddSingleton<SnapshotCommand>();

        try
        {
            var templateRoot = Path.Combine(Directory.GetCurrentDirectory(), "src", "Templates");
            if (Directory.Exists(templateRoot))
            {
                services.AddSingleton<Xtraq.Engine.ITemplateRenderer, Xtraq.Engine.SimpleTemplateEngine>();
                services.AddSingleton<Xtraq.Engine.ITemplateLoader>(_ => new Xtraq.Engine.FileSystemTemplateLoader(templateRoot));
            }
        }
        catch (Exception tex)
        {
            Console.Error.WriteLine($"[xtraq templates warn] {tex.Message}");
        }

        using var serviceProvider = services.BuildServiceProvider();
        var runtime = serviceProvider.GetRequiredService<XtraqCliRuntime>();
        var commandOptionsAccessor = serviceProvider.GetRequiredService<CommandOptions>();
        var cliTelemetry = serviceProvider.GetRequiredService<ICliTelemetryService>();
        var cliVersion = CliHostUtilities.ResolveProductVersion();

        var initDescriptor = CliCommandCatalog.Get(CliCommandKind.Init);
        var buildDescriptor = CliCommandCatalog.Get(CliCommandKind.Build);
        var snapshotDescriptor = CliCommandCatalog.Get(CliCommandKind.Snapshot);
        var versionDescriptor = CliCommandCatalog.Get(CliCommandKind.Version);
        var updateDescriptor = CliCommandCatalog.Get(CliCommandKind.Update);

        var buildCommandHandler = (IXtraqCommand)serviceProvider.GetRequiredService(buildDescriptor.HandlerType!);
        var snapshotCommandHandler = (IXtraqCommand)serviceProvider.GetRequiredService(snapshotDescriptor.HandlerType!);

        static Argument<string?> CreateOptionalProjectArgument(string description)
        {
            var argument = new Argument<string?>("project-path")
            {
                Description = description,
                Arity = ArgumentArity.ZeroOrOne
            };

            return argument;
        }

        var verboseOption = new Option<bool>("--verbose", () => false, "Show additional diagnostic information");
        verboseOption.AddAlias("-v");
        var debugOption = new Option<bool>("--debug", "Use debug environment settings");
        var debugAliasOption = new Option<bool>("--debug-alias", "Enable alias scope debug logging (sets XTRAQ_ALIAS_DEBUG=1)");
        var noCacheOption = new Option<bool>("--no-cache", "Do not read or write the local procedure metadata cache");
        var procedureOption = new Option<string?>("--procedure", "Process only specific procedures (comma separated schema.name with optional '*' or '?' wildcards)");
        var telemetryOption = new Option<bool>("--telemetry", "Persist a database telemetry report to .xtraq/telemetry");
        var jsonIncludeNullValuesOption = new Option<bool>("--json-include-null-values", "Emit JsonIncludeNullValues attribute for JSON result properties");
        var entityFrameworkOption = new Option<bool>("--entity-framework", "Enable Entity Framework integration helper generation (sets XTRAQ_ENTITY_FRAMEWORK)");
        var refreshSnapshotOption = new Option<bool>("--refresh-snapshot", () => false, "Refresh snapshot before executing the build command");
        var ciOption = new Option<bool>("--ci", "Disable Spectre.Console enhancements for CI/plain output modes");
        var projectOption = new Option<string?>("--project-path", "Project root path (.env file or directory). Defaults to current directory when omitted.");
        projectOption.AddAlias("--project");
        projectOption.AddAlias("-p");
        projectOption.AddValidator(result =>
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
        procedureOption.AddValidator(result =>
        {
            var rawValue = result.GetValueOrDefault<string?>();
            if (!CliHostUtilities.TryNormalizeProcedureFilter(rawValue, out _, out var error) && !string.IsNullOrEmpty(error))
            {
                result.ErrorMessage = error;
            }
        });

        var root = new RootCommand("Xtraq CLI")
        {
            TreatUnmatchedTokensAsErrors = true
        };
        root.AddGlobalOption(verboseOption);
        root.AddGlobalOption(debugOption);
        root.AddGlobalOption(debugAliasOption);
        root.AddGlobalOption(noCacheOption);
        root.AddGlobalOption(procedureOption);
        root.AddGlobalOption(telemetryOption);
        root.AddGlobalOption(jsonIncludeNullValuesOption);
        root.AddGlobalOption(entityFrameworkOption);
        root.AddGlobalOption(ciOption);
        root.AddGlobalOption(projectOption);

        void ApplyDebugAlias(ParseResult parseResult, Option<bool> debugAlias)
        {
            if (!parseResult.HasOption(debugAlias))
            {
                return;
            }

            var debugAliasValue = parseResult.GetValueForOption(debugAlias);
            Environment.SetEnvironmentVariable("XTRAQ_ALIAS_DEBUG", debugAliasValue ? "1" : null);
        }

        void PrepareCommandEnvironment(CliCommandOptions options)
        {
            DirectoryUtils.SetBasePath(options.Path);
            CacheControl.ForceReload = options.NoCache;

            var procedureFilter = string.IsNullOrWhiteSpace(options.Procedure) ? null : options.Procedure;
            Environment.SetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES", procedureFilter);

            if (options.HasEntityFrameworkIntegrationOverride)
            {
                Environment.SetEnvironmentVariable("XTRAQ_ENTITY_FRAMEWORK", options.EntityFrameworkIntegration ? "1" : null);
            }

        }

        void ScheduleUpdateCheck(CliCommandOptions options, IServiceProvider serviceProvider)
        {
            // Schedule async update check if not disabled
            if (!options.NoUpdate && !UpdateService.IsUpdateDisabled())
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var updateService = serviceProvider.GetRequiredService<UpdateService>();
                        var updateInfo = await updateService.CheckForUpdateAsync().ConfigureAwait(false);
                        if (updateInfo?.IsUpdateAvailable == true)
                        {
                            Console.WriteLine();
                            Console.WriteLine($"[xtraq] Update available: {updateInfo.CurrentVersion} ? {updateInfo.LatestVersion}");
                            Console.WriteLine("[xtraq] Run 'xtraq update' to upgrade or use --no-update to suppress this message.");
                        }
                    }
                    catch
                    {
                        // Silently ignore update check failures
                    }
                });
            }
        }

        string ResolveProjectPath(ParseResult parseResult, Argument<string?>? commandArgument)
        {
            string? candidate = null;
            if (commandArgument is not null)
            {
                candidate = parseResult.GetValueForArgument(commandArgument);
            }

            if (string.IsNullOrWhiteSpace(candidate))
            {
                candidate = parseResult.GetValueForOption(projectOption);
            }

            return CliHostUtilities.NormalizeProjectPath(candidate);
        }

        async Task ExecuteCommandAsync(
            InvocationContext invocationContext,
            CliCommandDescriptor descriptor,
            IXtraqCommand command,
            Argument<string?>? commandArgument,
            Option<bool>? refreshOption,
            bool defaultRefresh,
            ICliTelemetryService telemetryService,
            string cliVersion)
        {
            ApplyDebugAlias(invocationContext.ParseResult, debugAliasOption);

            var projectPath = descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath)
                ? ResolveProjectPath(invocationContext.ParseResult, commandArgument)
                : Directory.GetCurrentDirectory();
            var options = new CliCommandOptions
            {
                Path = projectPath,
                Verbose = invocationContext.ParseResult.GetValueForOption(verboseOption),
                Debug = invocationContext.ParseResult.GetValueForOption(debugOption),
                NoCache = invocationContext.ParseResult.GetValueForOption(noCacheOption),
                NoUpdate = false,
                Procedure = CliHostUtilities.NormalizeProcedureFilter(invocationContext.ParseResult.GetValueForOption(procedureOption)),
                Telemetry = invocationContext.ParseResult.GetValueForOption(telemetryOption),
                JsonIncludeNullValues = invocationContext.ParseResult.GetValueForOption(jsonIncludeNullValuesOption),
                HasJsonIncludeNullValuesOverride = invocationContext.ParseResult.HasOption(jsonIncludeNullValuesOption),
                EntityFrameworkIntegration = invocationContext.ParseResult.GetValueForOption(entityFrameworkOption),
                HasEntityFrameworkIntegrationOverride = invocationContext.ParseResult.HasOption(entityFrameworkOption),
                CiMode = invocationContext.ParseResult.GetValueForOption(ciOption)
            };

            if (descriptor.HasFeature(CliCommandFeatures.RequiresProjectPath))
            {
                PrepareCommandEnvironment(options);
            }

            if (descriptor.HasFeature(CliCommandFeatures.SchedulesUpdate) && !UpdateService.IsUpdateDisabled())
            {
                ScheduleUpdateCheck(options, serviceProvider);
            }

            var shouldRefresh = defaultRefresh;
            if (descriptor.HasFeature(CliCommandFeatures.SupportsRefreshOption) && refreshOption is not null)
            {
                shouldRefresh = invocationContext.ParseResult.GetValueForOption(refreshOption);
            }

            commandOptionsAccessor.Update(options);

            var console = serviceProvider.GetRequiredService<IConsoleService>();
            EmitSessionPreamble(console, descriptor.Name, options, projectPath, environment, shouldRefresh);

            var commandContext = new XtraqCommandContext(
                projectPath,
                options,
                serviceProvider,
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
                        cliVersion,
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
                    await telemetryService.CaptureAsync(telemetryEvent, invocationContext.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    console.Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        }

        var buildProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");

        root.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(invocationContext, buildDescriptor, buildCommandHandler, null, null, defaultRefresh: true, cliTelemetry, cliVersion);
        });

        var snapshotProjectArgument = CreateOptionalProjectArgument("Optional project root path (.env file or directory). Defaults to current directory when omitted.");

        var snapshotCommand = new Command(snapshotDescriptor.Name, snapshotDescriptor.Description);
        snapshotCommand.AddArgument(snapshotProjectArgument);
        snapshotCommand.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(invocationContext, snapshotDescriptor, snapshotCommandHandler, snapshotProjectArgument, null, defaultRefresh: false, cliTelemetry, cliVersion);
        });
        root.AddCommand(snapshotCommand);

        var buildCommand = new Command(buildDescriptor.Name, buildDescriptor.Description);
        buildCommand.AddArgument(buildProjectArgument);
        buildCommand.AddOption(refreshSnapshotOption);
        buildCommand.SetHandler(async invocationContext =>
        {
            await ExecuteCommandAsync(
                invocationContext,
                buildDescriptor,
                buildCommandHandler,
                buildProjectArgument,
                refreshSnapshotOption,
                defaultRefresh: false,
                cliTelemetry,
                cliVersion);
        });
        root.AddCommand(buildCommand);

        var versionCommand = new Command(versionDescriptor.Name, versionDescriptor.Description);
        versionCommand.AddOption(verboseOption);
        versionCommand.SetHandler(async context =>
        {
            var options = new CliCommandOptions
            {
                Path = Directory.GetCurrentDirectory(),
                Verbose = context.ParseResult.GetValueForOption(verboseOption),
                Debug = context.ParseResult.GetValueForOption(debugOption),
                NoCache = context.ParseResult.GetValueForOption(noCacheOption),
                NoUpdate = false,
                Procedure = CliHostUtilities.NormalizeProcedureFilter(context.ParseResult.GetValueForOption(procedureOption)),
                Telemetry = context.ParseResult.GetValueForOption(telemetryOption),
                JsonIncludeNullValues = context.ParseResult.GetValueForOption(jsonIncludeNullValuesOption),
                HasJsonIncludeNullValuesOverride = context.ParseResult.HasOption(jsonIncludeNullValuesOption),
                EntityFrameworkIntegration = context.ParseResult.GetValueForOption(entityFrameworkOption),
                HasEntityFrameworkIntegrationOverride = context.ParseResult.HasOption(entityFrameworkOption),
                CiMode = context.ParseResult.GetValueForOption(ciOption)
            };
            commandOptionsAccessor.Update(options);
            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await runtime.GetVersionAsync().ConfigureAwait(false));
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
                        versionDescriptor.Name,
                        cliVersion,
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
                    await cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    serviceProvider.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });
        root.AddCommand(versionCommand);

        var updateCommand = new Command(updateDescriptor.Name, updateDescriptor.Description);
        updateCommand.AddOption(verboseOption);
        updateCommand.SetHandler(async context =>
        {
            var options = new CliCommandOptions
            {
                Path = Directory.GetCurrentDirectory(),
                Verbose = context.ParseResult.GetValueForOption(verboseOption),
                Debug = context.ParseResult.GetValueForOption(debugOption),
                NoCache = context.ParseResult.GetValueForOption(noCacheOption),
                NoUpdate = false,
                Procedure = CliHostUtilities.NormalizeProcedureFilter(context.ParseResult.GetValueForOption(procedureOption)),
                Telemetry = context.ParseResult.GetValueForOption(telemetryOption),
                JsonIncludeNullValues = context.ParseResult.GetValueForOption(jsonIncludeNullValuesOption),
                HasJsonIncludeNullValuesOverride = context.ParseResult.HasOption(jsonIncludeNullValuesOption),
                EntityFrameworkIntegration = context.ParseResult.GetValueForOption(entityFrameworkOption),
                HasEntityFrameworkIntegrationOverride = context.ParseResult.HasOption(entityFrameworkOption),
                CiMode = context.ParseResult.GetValueForOption(ciOption)
            };
            commandOptionsAccessor.Update(options);
            var stopwatch = Stopwatch.StartNew();
            var exitCode = ExitCodes.InternalError;
            try
            {
                var result = CommandResultMapper.Map(await runtime.UpdateAsync(options).ConfigureAwait(false));
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
                        updateDescriptor.Name,
                        cliVersion,
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
                    await cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    serviceProvider.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });
        root.AddCommand(updateCommand);

        var initCommand = new Command(initDescriptor.Name, initDescriptor.Description);
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
            var options = new CliCommandOptions
            {
                Path = Directory.GetCurrentDirectory(),
                Verbose = context.ParseResult.GetValueForOption(verboseOption),
                Debug = context.ParseResult.GetValueForOption(debugOption),
                NoCache = context.ParseResult.GetValueForOption(noCacheOption),
                NoUpdate = false,
                Procedure = CliHostUtilities.NormalizeProcedureFilter(context.ParseResult.GetValueForOption(procedureOption)),
                Telemetry = context.ParseResult.GetValueForOption(telemetryOption),
                JsonIncludeNullValues = context.ParseResult.GetValueForOption(jsonIncludeNullValuesOption),
                HasJsonIncludeNullValuesOverride = context.ParseResult.HasOption(jsonIncludeNullValuesOption),
                EntityFrameworkIntegration = context.ParseResult.GetValueForOption(entityFrameworkOption),
                HasEntityFrameworkIntegrationOverride = context.ParseResult.HasOption(entityFrameworkOption),
                CiMode = context.ParseResult.GetValueForOption(ciOption)
            };
            commandOptionsAccessor.Update(options);

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
                    targetPath = context.ParseResult.GetValueForOption(projectOption)?.Trim();
                }

                var nsValue = context.ParseResult.GetValueForOption(namespaceOption)?.Trim();
                var connection = context.ParseResult.GetValueForOption(connectionOption)?.Trim();
                var schemas = context.ParseResult.GetValueForOption(schemasOption)?.Trim();

                namespaceProvided = !string.IsNullOrWhiteSpace(nsValue);
                connectionProvided = !string.IsNullOrWhiteSpace(connection);

                var effectivePath = CliHostUtilities.NormalizeProjectPath(targetPath);
                var resolved = DirectoryUtils.IsPath(effectivePath) ? effectivePath : Path.GetFullPath(effectivePath);
                Directory.CreateDirectory(resolved);
                telemetryProjectRoot = resolved;

                var envPath = await Xtraq.Cli.ProjectEnvironmentBootstrapper.EnsureEnvAsync(resolved, autoApprove: true, force: force).ConfigureAwait(false);
                var examplePath = Xtraq.Cli.ProjectEnvironmentBootstrapper.EnsureEnvExample(resolved, force);

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
                        Xtraq.Cli.ProjectEnvironmentBootstrapper.EnsureProjectGitignore(resolved);
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
                        initDescriptor.Name,
                        cliVersion,
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
                    await cliTelemetry.CaptureAsync(telemetryEvent, context.GetCancellationToken()).ConfigureAwait(false);
                }
                catch (Exception telemetryEx)
                {
                    serviceProvider.GetRequiredService<IConsoleService>().Verbose($"telemetry capture failed: {telemetryEx.Message}");
                }
            }
        });
        root.AddCommand(initCommand);

        return await root.InvokeAsync(normalizedArgs).ConfigureAwait(false);
    }

    private static void EmitSessionPreamble(IConsoleService console, string commandName, CliCommandOptions options, string projectPath, string environment, bool refreshRequested)
    {
        ArgumentNullException.ThrowIfNull(console);

        var banner = CliHostUtilities.ResolveProductBanner();
        console.RenderFiglet(banner);

        var metadataJson = CliHostUtilities.BuildSessionMetadataJson(commandName, options, projectPath, environment, refreshRequested);
        console.RenderJsonPayload($"{(string.IsNullOrWhiteSpace(commandName) ? "session" : commandName)} metadata", metadataJson);
    }
    /// <summary>
    /// Default entry point for the CLI host.
    /// </summary>
    /// <param name="args">Command-line arguments forwarded by the runtime.</param>
    /// <returns>Exit code produced by <see cref="RunCliAsync(string[])"/>.</returns>
    public static Task<int> Main(string[] args) => RunCliAsync(args);
}

