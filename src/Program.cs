using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using Microsoft.Extensions.Configuration;
using Xtraq.Cli;
using Xtraq.Cli.Commands;
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
    private static readonly HashSet<string> KnownCommandNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "init",
        "snapshot",
        "build",
        "version",
        "update"
    };

    /// <summary>
    /// Executes the Xtraq CLI with the provided arguments.
    /// </summary>
    /// <param name="args">Command-line arguments received from the host.</param>
    /// <returns>Exit code emitted by the invoked command.</returns>
    public static async Task<int> RunCliAsync(string[] args)
    {
        var normalizedArgs = NormalizeInvocationArguments(args);

        try
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT")))
            {
                var defaultRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(Directory.GetCurrentDirectory());
                if (!string.IsNullOrWhiteSpace(defaultRoot))
                {
                    Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", defaultRoot);
                }
            }
        }
        catch
        {
        }

        try
        {
            static void LoadSkipVarsFromEnv(string path)
            {
                if (!File.Exists(path))
                {
                    return;
                }

                foreach (var rawLine in File.ReadAllLines(path))
                {
                    var line = rawLine.Trim();
                    if (line.Length == 0 || line.StartsWith('#'))
                    {
                        continue;
                    }

                    var equalsIndex = line.IndexOf('=');
                    if (equalsIndex <= 0)
                    {
                        continue;
                    }

                    var key = line.Substring(0, equalsIndex).Trim();
                    if (!key.Equals("XTRAQ_NO_UPDATE", StringComparison.OrdinalIgnoreCase) &&
                        !key.Equals("XTRAQ_SKIP_UPDATE", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var value = line.Substring(equalsIndex + 1).Trim();
                    if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(key)))
                    {
                        Environment.SetEnvironmentVariable(key, value);
                    }
                }
            }

            static void LoadEnvVariables(string path)
            {
                try
                {
                    Xtraq.Utils.EnvFileLoader.Apply(path);
                }
                catch
                {
                    // Ignore .env load failures and continue with existing environment variables.
                }
            }

            var cwd = Directory.GetCurrentDirectory();
            LoadSkipVarsFromEnv(Path.Combine(cwd, ".env"));
            LoadEnvVariables(Path.Combine(cwd, ".env"));

            try
            {
                var resolvedRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(cwd);
                if (!string.Equals(resolvedRoot, cwd, StringComparison.OrdinalIgnoreCase))
                {
                    LoadSkipVarsFromEnv(Path.Combine(resolvedRoot, ".env"));
                    LoadEnvVariables(Path.Combine(resolvedRoot, ".env"));
                }
            }
            catch
            {
            }
        }
        catch
        {
        }

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
        var cliVersion = ResolveProductVersion();

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
        var procedureOption = new Option<string?>("--procedure", "Process only specific procedures (comma separated schema.name)");
        var telemetryOption = new Option<bool>("--telemetry", "Persist a database telemetry report to .xtraq/telemetry");
        var jsonIncludeNullValuesOption = new Option<bool>("--json-include-null-values", "Emit JsonIncludeNullValues attribute for JSON result properties");
        var entityFrameworkOption = new Option<bool>("--entity-framework", "Enable Entity Framework integration helper generation (sets XTRAQ_ENTITY_FRAMEWORK)");
        var refreshSnapshotOption = new Option<bool>("--refresh-snapshot", () => false, "Refresh snapshot before executing the build command");
        var ciOption = new Option<bool>("--ci", "Disable Spectre.Console enhancements for CI/plain output modes");
        var projectOption = new Option<string?>("--project-path", "Project root path (.env file or directory). Defaults to current directory when omitted.");
        projectOption.AddAlias("--project");
        projectOption.AddAlias("-p");

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

            if (!string.IsNullOrWhiteSpace(options.Procedure))
            {
                Environment.SetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES", options.Procedure);
            }

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

            return NormalizeProjectPath(candidate);
        }

        string NormalizeProjectPath(string? value)
        {
            static bool LooksLikeEnv(string hint) => hint.EndsWith(".env", StringComparison.OrdinalIgnoreCase) || hint.EndsWith(".env.local", StringComparison.OrdinalIgnoreCase);

            var fallback = Directory.GetCurrentDirectory();
            if (string.IsNullOrWhiteSpace(value))
            {
                var resolvedDefault = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(fallback);
                Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", resolvedDefault);
                return resolvedDefault;
            }

            var trimmed = value.Trim();
            string candidate;
            try
            {
                candidate = Path.GetFullPath(trimmed);
            }
            catch
            {
                candidate = trimmed;
            }

            string DetermineRootForFilePath(string path)
            {
                var directory = Path.GetDirectoryName(path) ?? fallback;
                if (LooksLikeEnv(path))
                {
                    return Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(directory);
                }

                if (string.Equals(Path.GetFileName(path), ".xtraqconfig", StringComparison.OrdinalIgnoreCase))
                {
                    var redirected = Xtraq.Configuration.TrackableConfigManager.ResolveRedirectTargets(directory);
                    return redirected ?? directory;
                }

                return Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(directory);
            }

            string resolvedRoot;
            if (File.Exists(candidate))
            {
                resolvedRoot = DetermineRootForFilePath(candidate);
            }
            else if (Directory.Exists(candidate))
            {
                resolvedRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(candidate);
            }
            else if (LooksLikeEnv(candidate))
            {
                var directory = Path.GetDirectoryName(candidate) ?? fallback;
                resolvedRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(directory);
            }
            else
            {
                resolvedRoot = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(candidate);
            }

            Environment.SetEnvironmentVariable("XTRAQ_PROJECT_ROOT", resolvedRoot);
            return resolvedRoot;
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
                Procedure = invocationContext.ParseResult.GetValueForOption(procedureOption)?.Trim() ?? string.Empty,
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
                Procedure = context.ParseResult.GetValueForOption(procedureOption)?.Trim() ?? string.Empty,
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
                Procedure = context.ParseResult.GetValueForOption(procedureOption)?.Trim() ?? string.Empty,
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
                Procedure = context.ParseResult.GetValueForOption(procedureOption)?.Trim() ?? string.Empty,
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

                var effectivePath = NormalizeProjectPath(targetPath);
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

        var banner = ResolveProductBanner();
        console.RenderFiglet(banner);

        var metadataJson = BuildSessionMetadataJson(commandName, options, projectPath, environment, refreshRequested);
        console.RenderJsonPayload($"{(string.IsNullOrWhiteSpace(commandName) ? "session" : commandName)} metadata", metadataJson);
    }

    private static string BuildSessionMetadataJson(string commandName, CliCommandOptions options, string projectPath, string environment, bool refreshRequested)
    {
        var resolvedRoot = ProjectRootResolver.ResolveCurrent();
        var workingDirectory = NormalizePathSafe(Directory.GetCurrentDirectory());
        var normalizedProjectPath = NormalizePathSafe(projectPath);
        var normalizedProjectRoot = NormalizePathSafe(resolvedRoot);

        var metadata = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        {
            ["command"] = string.IsNullOrWhiteSpace(commandName) ? "default" : commandName,
            ["version"] = ResolveProductVersion(),
            ["timestampUtc"] = DateTimeOffset.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture),
            ["workingDirectory"] = workingDirectory,
            ["projectPath"] = normalizedProjectPath
        };

        if (!string.IsNullOrWhiteSpace(environment) && !string.Equals(environment, "Production", StringComparison.OrdinalIgnoreCase))
        {
            metadata["environment"] = environment;
        }

        if (!string.IsNullOrWhiteSpace(normalizedProjectRoot))
        {
            var differsFromPath = !string.Equals(normalizedProjectRoot, normalizedProjectPath, StringComparison.OrdinalIgnoreCase);
            if (differsFromPath || options.Verbose)
            {
                metadata["projectRoot"] = normalizedProjectRoot;
            }
        }

        var configDirectory = Xtraq.Configuration.TrackableConfigManager.LocateConfigDirectory(resolvedRoot);
        var configPath = Path.Combine(configDirectory, ".xtraqconfig");
        if (File.Exists(configPath))
        {
            var normalizedConfig = NormalizePathSafe(configPath);
            var defaultConfigPath = string.IsNullOrWhiteSpace(normalizedProjectRoot)
                ? string.Empty
                : NormalizePathSafe(Path.Combine(normalizedProjectRoot, ".xtraqconfig"));

            if (options.Verbose || !string.Equals(normalizedConfig, defaultConfigPath, StringComparison.OrdinalIgnoreCase))
            {
                metadata["configPath"] = normalizedConfig;
            }
        }

        var projectHint = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_ROOT");
        if (!string.IsNullOrWhiteSpace(projectHint))
        {
            var normalizedHint = NormalizePathSafe(projectHint);
            var differsFromRoot = !string.Equals(normalizedHint, normalizedProjectRoot, StringComparison.OrdinalIgnoreCase);
            if (differsFromRoot || options.Verbose)
            {
                metadata["projectRootHint"] = normalizedHint;
            }
        }

        if (!string.IsNullOrWhiteSpace(options.Procedure))
        {
            metadata["procedureFilter"] = options.Procedure;
        }

        if (options.NoCache)
        {
            metadata["noCache"] = true;
        }

        if (options.Telemetry)
        {
            metadata["telemetry"] = true;
        }

        if (options.JsonIncludeNullValues)
        {
            metadata["jsonIncludeNullValues"] = true;
        }

        if (options.CiMode)
        {
            metadata["ciMode"] = true;
        }

        if (refreshRequested)
        {
            metadata["refreshSnapshot"] = true;
        }

        var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        return json;
    }

    private static string ResolveProductBanner()
    {
        return "Xtraq";
    }

    private static string ResolveProductVersion()
    {
        var informational = typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrWhiteSpace(informational))
        {
            var plusIndex = informational.IndexOf('+');
            if (plusIndex > 0)
            {
                informational = informational[..plusIndex];
            }

            return informational!;
        }

        return typeof(Program).Assembly.GetName().Version?.ToString() ?? "1.0.0";
    }

    private static string[] NormalizeInvocationArguments(string[] args)
    {
        if (args == null)
        {
            return Array.Empty<string>();
        }

        if (args.Length == 0)
        {
            return args;
        }

        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (string.IsNullOrWhiteSpace(token))
            {
                continue;
            }

            if (token.StartsWith("--project-path=", StringComparison.OrdinalIgnoreCase) ||
                token.StartsWith("--project=", StringComparison.OrdinalIgnoreCase))
            {
                return args;
            }

            if (string.Equals(token, "--project-path", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(token, "--project", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(token, "-p", StringComparison.OrdinalIgnoreCase))
            {
                return args;
            }
        }

        var firstCandidateIndex = -1;
        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (string.IsNullOrWhiteSpace(token))
            {
                continue;
            }

            if (string.Equals(token, "--", StringComparison.Ordinal))
            {
                break;
            }

            if (token.StartsWith("-", StringComparison.Ordinal))
            {
                continue;
            }

            if (KnownCommandNames.Contains(token))
            {
                continue;
            }

            firstCandidateIndex = i;
            break;
        }

        if (firstCandidateIndex < 0)
        {
            return args;
        }

        var normalized = new string[args.Length + 1];
        var cursor = 0;
        for (int i = 0; i < args.Length; i++)
        {
            if (i == firstCandidateIndex)
            {
                normalized[cursor++] = "--project-path";
            }

            normalized[cursor++] = args[i];
        }

        return normalized;
    }

    private static string NormalizePathSafe(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        try
        {
            return Path.GetFullPath(value);
        }
        catch
        {
            return value;
        }
    }

    private static string? TryExtractProjectHint(string[] args)
    {
        if (args == null || args.Length == 0)
        {
            return null;
        }

        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (string.IsNullOrWhiteSpace(token))
            {
                continue;
            }

            if (string.Equals(token, "--", StringComparison.Ordinal))
            {
                break;
            }

            if (token.StartsWith("--project-path=", StringComparison.OrdinalIgnoreCase) ||
                token.StartsWith("--project=", StringComparison.OrdinalIgnoreCase))
            {
                var separatorIndex = token.IndexOf('=');
                if (separatorIndex > 0 && separatorIndex < token.Length - 1)
                {
                    return token[(separatorIndex + 1)..];
                }

                continue;
            }

            if (string.Equals(token, "--project-path", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(token, "--project", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(token, "-p", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 < args.Length)
                {
                    return args[i + 1];
                }

                break;
            }

            if (token.StartsWith("-", StringComparison.Ordinal))
            {
                continue;
            }

            if (KnownCommandNames.Contains(token))
            {
                continue;
            }

            return token;
        }

        return null;
    }

    private static (string configPath, string? projectRoot) NormalizeCliProjectHint(string rawInput)
    {
        if (string.IsNullOrWhiteSpace(rawInput))
        {
            return (string.Empty, null);
        }

        string fullPath;
        try
        {
            fullPath = Path.GetFullPath(rawInput.Trim());
        }
        catch
        {
            fullPath = rawInput.Trim();
        }

        static bool IsEnvFile(string value) => value.EndsWith(".env", StringComparison.OrdinalIgnoreCase) || value.EndsWith(".env.local", StringComparison.OrdinalIgnoreCase);

        if (Directory.Exists(fullPath))
        {
            return (fullPath, fullPath);
        }

        if (File.Exists(fullPath))
        {
            var fileName = Path.GetFileName(fullPath);
            if (IsEnvFile(fileName))
            {
                var root = Path.GetDirectoryName(fullPath) ?? Directory.GetCurrentDirectory();
                return (fullPath, root);
            }

            var fallbackRoot = Path.GetDirectoryName(fullPath) ?? Directory.GetCurrentDirectory();
            return (fallbackRoot, fallbackRoot);
        }

        if (IsEnvFile(fullPath))
        {
            var root = Path.GetDirectoryName(fullPath) ?? Directory.GetCurrentDirectory();
            return (fullPath, root);
        }

        return (fullPath, fullPath);
    }

    /// <summary>
    /// Default entry point for the CLI host.
    /// </summary>
    /// <param name="args">Command-line arguments forwarded by the runtime.</param>
    /// <returns>Exit code produced by <see cref="RunCliAsync(string[])"/>.</returns>
    public static Task<int> Main(string[] args) => RunCliAsync(args);
}

