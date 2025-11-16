using Microsoft.Data.SqlClient;
using Xunit;
using Xunit.Sdk;

namespace Xtraq.IntegrationTests;

/// <summary>
/// Runs the CLI against a disposable SQL Server container seeded with the sample catalogue.
/// Verifies snapshot/build orchestration, generator output, and cache reuse end-to-end.
/// </summary>
public sealed class CliEndToEndTests : IAsyncLifetime
{
    private const string SampleDatabaseName = "XtraqSample";
    private const string SampleSchema = "sample";
    private const string ContainerPassword = "Xtraq@12345";
    private static readonly TimeSpan CliExecutionTimeout = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ContainerStartTimeout = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan DatabaseReadyTimeout = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan ContainerCleanupTimeout = TimeSpan.FromSeconds(30);
    private const string LogPrefix = "[CliEndToEndTests]";
    private static readonly Dictionary<string, int> ScriptPriorityOverrides = new(StringComparer.OrdinalIgnoreCase)
    {
        ["tables/Users.sql"] = 0,
        ["tables/UserContacts.sql"] = 1,
        ["tables/Orders.sql"] = 2,
        ["tables/Payments.sql"] = 3,
        ["tables/AuditLog.sql"] = 4
    };

    private SqlServerContainer? _container;
    private bool _dockerAvailable;
    private string? _skipReason;

    [Fact]
    [Trait("Category", "CliE2E")]
    public async Task SnapshotAndBuild_Should_Generate_Procedures_And_DbContext()
    {
        if (!_dockerAvailable)
        {
            var reason = _skipReason ?? "Docker unavailable";
            Console.WriteLine($"{LogPrefix} Skipped: {reason}");
            throw SkipException.ForSkip(reason);
        }

        using var projectDirectory = TempDirectory.Create("xtraq-e2e-");
        using var envScope = new EnvironmentVariableScope(("XTRAQ_NO_UPDATE", "1"), ("XTRAQ_SKIP_UPDATE", "1"));

        Console.WriteLine($"{LogPrefix} Preparing project environment at {projectDirectory.Path}...");
        await WriteEnvAsync(projectDirectory.Path);

        var sampleProjectPath = Path.Combine(projectDirectory.Path, "samples", "restapi");
        var outputDirectory = Path.Combine(sampleProjectPath, "Xtraq");
        var originalCwd = Directory.GetCurrentDirectory();
        var solutionRoot = ResolveSolutionRoot();

        try
        {
            Directory.SetCurrentDirectory(solutionRoot);

            Console.WriteLine($"{LogPrefix} Running snapshot command...");
            var snapshotExit = await InvokeCliWithRetryAsync(sampleProjectPath, "snapshot", new[] { "--verbose" });
            Console.WriteLine($"{LogPrefix} Snapshot command exited with {snapshotExit}.");
            Assert.Equal(0, snapshotExit);

            var snapshotDirectory = Path.Combine(sampleProjectPath, ".xtraq", "snapshots");
            Assert.True(Directory.Exists(snapshotDirectory), "Snapshot directory was not created.");
            Assert.NotEmpty(Directory.GetFiles(snapshotDirectory, "*.json", SearchOption.TopDirectoryOnly));

            Console.WriteLine($"{LogPrefix} Running build command...");
            var buildExit = await InvokeCliWithRetryAsync(sampleProjectPath, "build", Array.Empty<string>());
            Console.WriteLine($"{LogPrefix} Build command exited with {buildExit}.");
            Assert.Equal(0, buildExit);

            Assert.True(Directory.Exists(outputDirectory), $"Expected output directory at {outputDirectory}.");

            var generatedProcedure = Path.Combine(outputDirectory, "Sample", "UserList.cs");
            Assert.True(File.Exists(generatedProcedure), $"Expected generated procedure artifact at {generatedProcedure}.");

            var procedureContent = await File.ReadAllTextAsync(generatedProcedure);
            Assert.Contains("partial class UserList", procedureContent, StringComparison.Ordinal);
            Assert.Contains("public static class UserListProcedure", procedureContent, StringComparison.Ordinal);

            var dbContextPath = Path.Combine(outputDirectory, "XtraqDbContext.cs");
            Assert.True(File.Exists(dbContextPath), "DbContext artifact not generated.");

            var generatedArtifacts = Directory.EnumerateFiles(outputDirectory, "*.cs", SearchOption.AllDirectories).ToArray();
            Assert.NotEmpty(generatedArtifacts);

            var tableTypeArtifacts = Directory.EnumerateFiles(outputDirectory, "*TableType.cs", SearchOption.AllDirectories)
                .Select(path => Path.GetRelativePath(outputDirectory, path).Replace(Path.DirectorySeparatorChar, '/'))
                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            Assert.Equal(new[]
            {
                "Sample/OrderImportTableType.cs",
                "Sample/UserContactTableType.cs",
                "Shared/AuditLogEntryTableType.cs"
            }, tableTypeArtifacts);
        }
        finally
        {
            Directory.SetCurrentDirectory(originalCwd);
        }
    }

    public async Task InitializeAsync()
    {
        try
        {
            Console.WriteLine($"{LogPrefix} Starting SQL Server container...");
            using var startCts = new CancellationTokenSource(ContainerStartTimeout);
            _container = await SqlServerContainer.StartAsync(ContainerPassword, startCts.Token).ConfigureAwait(false);

            Console.WriteLine($"{LogPrefix} Container started. Endpoint: {_container.Host}:{_container.Port}");

            try
            {
                await WaitForDatabaseAsync().ConfigureAwait(false);
            }
            catch (TimeoutException timeoutEx)
            {
                _skipReason = $"SQL Server readiness timed out: {timeoutEx.Message}";
                Console.WriteLine($"{LogPrefix} {_skipReason}");
                await SafeDisposeContainerAsync().ConfigureAwait(false);
                return;
            }

            Console.WriteLine($"{LogPrefix} SQL Server reported ready. Seeding sample database...");

            try
            {
                await ProvisionSampleDatabaseAsync().ConfigureAwait(false);
            }
            catch (Exception seedingEx)
            {
                _skipReason = $"Sample database provisioning failed: {seedingEx.Message}";
                Console.WriteLine($"{LogPrefix} {_skipReason}");
                await SafeDisposeContainerAsync().ConfigureAwait(false);
                return;
            }

            Console.WriteLine($"{LogPrefix} Sample database seeded successfully.");
            Console.WriteLine($"{LogPrefix} Validating sample database accessibility...");
            await EnsureSampleDatabaseAccessibleAsync().ConfigureAwait(false);
            Console.WriteLine($"{LogPrefix} Sample database connection validated.");
            _dockerAvailable = true;
        }
        catch (OperationCanceledException)
        {
            _skipReason = $"Docker container start timed out after {ContainerStartTimeout.TotalSeconds:F0} seconds.";
            Console.WriteLine($"{LogPrefix} {_skipReason}");
            await SafeDisposeContainerAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (IsDockerUnavailable(ex))
        {
            Console.WriteLine($"{LogPrefix} Docker availability check failed: {ex}");
            _skipReason = $"Docker not available: {ex.Message}";
            _dockerAvailable = false;
            await SafeDisposeContainerAsync().ConfigureAwait(false);
        }
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            try
            {
                await SafeDisposeContainerAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{LogPrefix} Container disposal failed: {ex.Message}");
            }
        }
    }

    private async Task ProvisionSampleDatabaseAsync()
    {
        var scriptsRoot = Path.Combine(ResolveSolutionRoot(), "samples", "sql");
        if (!Directory.Exists(scriptsRoot))
        {
            throw new InvalidOperationException($"Sample SQL scripts directory not found at '{scriptsRoot}'. ResolveSolutionRoot() returned an unexpected location.");
        }
        var phases = new[] { "schema", "types", "tables", "data", "functions", "procedures" };

        foreach (var phase in phases)
        {
            var phaseDirectory = Path.Combine(scriptsRoot, phase);
            if (!Directory.Exists(phaseDirectory))
            {
                continue;
            }

            foreach (var script in Directory.EnumerateFiles(phaseDirectory, "*.sql", SearchOption.TopDirectoryOnly)
                         .OrderBy(script => GetScriptPriority(phase, script))
                         .ThenBy(static file => file, StringComparer.OrdinalIgnoreCase))
            {
                await ExecuteScriptAsync(script);
            }
        }
    }

    private async Task ExecuteScriptAsync(string scriptPath)
    {
        var statements = SplitBatches(await File.ReadAllTextAsync(scriptPath));

        await using var connection = new SqlConnection(BuildConnectionString("master"));
        await connection.OpenAsync();

        foreach (var statement in statements)
        {
            if (string.IsNullOrWhiteSpace(statement))
            {
                continue;
            }

            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed class SqlServerContainer
    {
        private const string Image = "mcr.microsoft.com/mssql/server:2022-latest";
        private const string ContainerPortKey = "1433/tcp";

        private readonly string _containerName;
        private bool _disposed;

        private SqlServerContainer(string containerName, string password, int hostPort)
        {
            _containerName = containerName;
            Host = "127.0.0.1";
            Port = hostPort;

            var builder = new SqlConnectionStringBuilder
            {
                DataSource = $"{Host},{Port}",
                UserID = "sa",
                Password = password,
                TrustServerCertificate = true,
                Encrypt = false
            };

            ConnectionString = builder.ConnectionString;
        }

        public string Host { get; }

        public int Port { get; }

        public string ConnectionString { get; }

        public static async Task<SqlServerContainer> StartAsync(string password, CancellationToken cancellationToken)
        {
            await EnsureDockerAvailableAsync(cancellationToken).ConfigureAwait(false);

            var containerName = $"xtraq-cli-e2e-{Guid.NewGuid():N}";

            var runArguments = new[]
            {
                "run",
                "--rm",
                "-d",
                "--name",
                containerName,
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                $"MSSQL_SA_PASSWORD={password}",
                "-p",
                "0:1433",
                Image
            };

            var runResult = await RunDockerAsync(runArguments, TimeSpan.FromMinutes(5), cancellationToken).ConfigureAwait(false);
            if (runResult.ExitCode != 0)
            {
                throw new InvalidOperationException($"docker run failed: {runResult.StandardError}");
            }

            var inspectResult = await RunDockerAsync(new[] { "inspect", containerName }, TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
            if (inspectResult.ExitCode != 0)
            {
                await RunDockerAsync(new[] { "stop", containerName }, TimeSpan.FromSeconds(10), CancellationToken.None).ConfigureAwait(false);
                throw new InvalidOperationException($"docker inspect failed: {inspectResult.StandardError}");
            }

            using var document = System.Text.Json.JsonDocument.Parse(inspectResult.StandardOutput);
            if (document.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array || document.RootElement.GetArrayLength() == 0)
            {
                await RunDockerAsync(new[] { "stop", containerName }, TimeSpan.FromSeconds(10), CancellationToken.None).ConfigureAwait(false);
                throw new InvalidOperationException("docker inspect returned unexpected payload.");
            }

            var root = document.RootElement[0];
            if (!root.TryGetProperty("NetworkSettings", out var networkSettings) ||
                !networkSettings.TryGetProperty("Ports", out var ports) ||
                !ports.TryGetProperty(ContainerPortKey, out var portArray) ||
                portArray.ValueKind != System.Text.Json.JsonValueKind.Array ||
                portArray.GetArrayLength() == 0)
            {
                await RunDockerAsync(new[] { "stop", containerName }, TimeSpan.FromSeconds(10), CancellationToken.None).ConfigureAwait(false);
                throw new InvalidOperationException("Unable to resolve container port mapping.");
            }

            var hostPortValue = portArray[0].GetProperty("HostPort").GetString();
            if (string.IsNullOrWhiteSpace(hostPortValue) ||
                !int.TryParse(hostPortValue, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var hostPort))
            {
                await RunDockerAsync(new[] { "stop", containerName }, TimeSpan.FromSeconds(10), CancellationToken.None).ConfigureAwait(false);
                throw new InvalidOperationException($"Invalid host port value '{hostPortValue}'.");
            }

            return new SqlServerContainer(containerName, password, hostPort);
        }

        public async Task DisposeAsync(TimeSpan timeout)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            try
            {
                var stopResult = await RunDockerAsync(new[] { "stop", _containerName }, timeout, CancellationToken.None).ConfigureAwait(false);
                if (stopResult.ExitCode != 0 && !stopResult.StandardError.Contains("No such container", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException($"docker stop failed: {stopResult.StandardError}");
                }
            }
            finally
            {
                await RunDockerAsync(new[] { "rm", "-f", _containerName }, TimeSpan.FromSeconds(10), CancellationToken.None).ConfigureAwait(false);
            }
        }

        private static async Task EnsureDockerAvailableAsync(CancellationToken cancellationToken)
        {
            var infoResult = await RunDockerAsync(new[] { "info" }, TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
            if (infoResult.ExitCode != 0)
            {
                throw new InvalidOperationException($"docker info failed: {infoResult.StandardError}");
            }
        }

        private static async Task<DockerProcessResult> RunDockerAsync(IReadOnlyList<string> arguments, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var startInfo = new System.Diagnostics.ProcessStartInfo("docker")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            foreach (var argument in arguments)
            {
                startInfo.ArgumentList.Add(argument);
            }

            using var process = new System.Diagnostics.Process { StartInfo = startInfo };

            if (!process.Start())
            {
                throw new InvalidOperationException("Failed to start docker CLI process.");
            }

            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            linkedCts.CancelAfter(timeout);

            try
            {
                await process.WaitForExitAsync(linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                try
                {
                    if (!process.HasExited)
                    {
                        process.Kill(true);
                    }
                }
                catch
                {
                    // Ignore kill failures.
                }

                throw;
            }

            var stdout = await stdoutTask.ConfigureAwait(false);
            var stderr = await stderrTask.ConfigureAwait(false);

            return new DockerProcessResult(process.ExitCode, stdout.Trim(), stderr.Trim());
        }

        private readonly record struct DockerProcessResult(int ExitCode, string StandardOutput, string StandardError);
    }

    private async Task WaitForDatabaseAsync()
    {
        if (_container is null)
        {
            throw new InvalidOperationException("SQL Server container has not been initialized.");
        }

        var deadline = DateTime.UtcNow.Add(DatabaseReadyTimeout);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var connection = new SqlConnection(BuildConnectionString("master"));
                await connection.OpenAsync().ConfigureAwait(false);
                return;
            }
            catch (SqlException)
            {
                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        }

        throw new TimeoutException($"SQL Server container did not become ready within {DatabaseReadyTimeout.TotalSeconds:F0} seconds.");
    }

    private async Task EnsureSampleDatabaseAccessibleAsync()
    {
        if (_container is null)
        {
            throw new InvalidOperationException("SQL Server container has not been initialized.");
        }

        var masterConnectionString = BuildConnectionString("master");
        var sampleConnectionString = BuildConnectionString(SampleDatabaseName);
        Console.WriteLine($"{LogPrefix} Sample database connection string: {MaskConnectionString(sampleConnectionString)}");

        var deadline = DateTime.UtcNow.Add(DatabaseReadyTimeout);
        var lastReportedState = string.Empty;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var masterConnection = new SqlConnection(masterConnectionString);
                await masterConnection.OpenAsync().ConfigureAwait(false);

                await using var stateCommand = masterConnection.CreateCommand();
                stateCommand.CommandText = "SELECT state_desc FROM sys.databases WHERE name = @name";
                stateCommand.Parameters.Add("@name", System.Data.SqlDbType.NVarChar, 128).Value = SampleDatabaseName;

                var stateResult = await stateCommand.ExecuteScalarAsync().ConfigureAwait(false);
                var state = stateResult switch
                {
                    null => "UNKNOWN",
                    string stateString => stateString,
                    System.Data.SqlTypes.SqlString sqlState when !sqlState.IsNull => sqlState.Value,
                    _ => stateResult.ToString() ?? "UNKNOWN"
                };
                if (!string.Equals(state, lastReportedState, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"{LogPrefix} Database state for '{SampleDatabaseName}': {state}.");
                    lastReportedState = state;
                }

                if (!string.Equals(state, "ONLINE", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"{LogPrefix} Attempting to remediate database state '{state}'.");
                    await ForceDatabaseOnlineAsync(masterConnection, SampleDatabaseName).ConfigureAwait(false);
                    await EnsureDatabaseMultiUserAsync(masterConnection).ConfigureAwait(false);
                }

                await using var sampleConnection = new SqlConnection(sampleConnectionString);
                await sampleConnection.OpenAsync().ConfigureAwait(false);

                await using var pingCommand = sampleConnection.CreateCommand();
                pingCommand.CommandText = "SET NOCOUNT ON; SELECT 1";
                await pingCommand.ExecuteScalarAsync().ConfigureAwait(false);

                return;
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"{LogPrefix} Sample database not accessible yet: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }

        throw new TimeoutException($"Sample database did not become accessible within {DatabaseReadyTimeout.TotalSeconds:F0} seconds.");
    }

    private static async Task ForceDatabaseOnlineAsync(SqlConnection masterConnection, string databaseName)
    {
        await using var command = masterConnection.CreateCommand();
        command.CommandText = """
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName AND state_desc <> 'ONLINE')
            BEGIN
                DECLARE @setOnline nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@dbName) + N' SET ONLINE WITH ROLLBACK IMMEDIATE;';
                BEGIN TRY
                    EXEC(@setOnline);
                END TRY
                BEGIN CATCH
                    DECLARE @setOffline nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@dbName) + N' SET OFFLINE WITH ROLLBACK IMMEDIATE;';
                    DECLARE @setOnlineRetry nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@dbName) + N' SET ONLINE WITH ROLLBACK IMMEDIATE;';
                    EXEC(@setOffline);
                    EXEC(@setOnlineRetry);
                END CATCH
            END
            """;
        command.Parameters.Add("@dbName", System.Data.SqlDbType.NVarChar, 128).Value = databaseName;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task EnsureDatabaseMultiUserAsync(SqlConnection masterConnection)
    {
        await using var command = masterConnection.CreateCommand();
        command.CommandText = """
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName)
            BEGIN
                DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@dbName) + N' SET MULTI_USER WITH ROLLBACK IMMEDIATE;';
                EXEC(@sql);
                DECLARE @ownerSql nvarchar(max) = N'ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(@dbName) + N' TO sa;';
                EXEC(@ownerSql);
            END
            """;
        var parameter = command.Parameters.Add("@dbName", System.Data.SqlDbType.NVarChar, 128);
        parameter.Value = SampleDatabaseName;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static string MaskConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return connectionString;
        }

        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            Password = "***"
        };

        return builder.ConnectionString;
    }

    private string BuildConnectionString(string database)
    {
        if (_container is null)
        {
            throw new InvalidOperationException("SQL Server container has not been initialized.");
        }

        var builder = new SqlConnectionStringBuilder(_container.ConnectionString)
        {
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = false
        };

        if (builder.ConnectTimeout < 30)
        {
            builder.ConnectTimeout = 30;
        }

        return builder.ConnectionString;
    }

    private static IEnumerable<string> SplitBatches(string script)
    {
        var buffer = new System.Text.StringBuilder();
        using var reader = new StringReader(script);
        string? line;

        while ((line = reader.ReadLine()) is not null)
        {
            if (line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                if (buffer.Length > 0)
                {
                    yield return buffer.ToString();
                    buffer.Clear();
                }

                continue;
            }

            buffer.AppendLine(line);
        }

        if (buffer.Length > 0)
        {
            yield return buffer.ToString();
        }
    }

    private static int GetScriptPriority(string phase, string scriptPath)
    {
        var fileName = Path.GetFileName(scriptPath);
        var key = string.Create(phase.Length + fileName.Length + 1, (Phase: phase, File: fileName), static (span, state) =>
        {
            state.Phase.AsSpan().CopyTo(span);
            span[state.Phase.Length] = '/';
            state.File.AsSpan().CopyTo(span[(state.Phase.Length + 1)..]);
        });

        return ScriptPriorityOverrides.TryGetValue(key, out var priority) ? priority : 100;
    }

    private static string ResolveSolutionRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Xtraq.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return Directory.GetCurrentDirectory();
    }

    private async Task WriteEnvAsync(string projectRoot)
    {
        if (_container is null)
        {
            throw new InvalidOperationException("SQL Server container has not been initialized.");
        }

        Directory.CreateDirectory(projectRoot);
        var restApiProjectRoot = Path.Combine(projectRoot, "samples", "restapi");
        Directory.CreateDirectory(restApiProjectRoot);

        var builder = new SqlConnectionStringBuilder(_container.ConnectionString)
        {
            InitialCatalog = SampleDatabaseName,
            TrustServerCertificate = true,
            Encrypt = false
        };

        var envContent = new System.Text.StringBuilder()
            .AppendLine("XTRAQ_NAMESPACE=Xtraq.Sample")
            .AppendLine("XTRAQ_OUTPUT_DIR=Xtraq")
            .AppendLine($"XTRAQ_GENERATOR_DB={builder.ConnectionString}")
            .AppendLine($"XTRAQ_BUILD_SCHEMAS={SampleSchema}")
            .ToString();

        var envPath = Path.Combine(restApiProjectRoot, ".env");
        await File.WriteAllTextAsync(envPath, envContent).ConfigureAwait(false);

        await WriteTrackableConfigAsync(restApiProjectRoot).ConfigureAwait(false);
    }

    private static async Task WriteTrackableConfigAsync(string projectRoot)
    {
        var configContent = $$"""
                {
                    "Namespace": "Xtraq.Sample",
                    "OutputDir": "Xtraq",
                    "TargetFramework": "net10.0",
                    "BuildSchemas": [
                        "{SampleSchema}"
                    ]
                }
                """;

        await File.WriteAllTextAsync(Path.Combine(projectRoot, ".xtraqconfig"), configContent).ConfigureAwait(false);
    }

    private static Task<int> InvokeCliAsync(string projectRoot, string command, params string[] additionalArguments)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(command);

        var args = new List<string>(capacity: 3 + additionalArguments.Length)
        {
            command,
            projectRoot,
            "--ci"
        };

        if (additionalArguments.Length > 0)
        {
            args.AddRange(additionalArguments);
        }

        return Xtraq.Program.RunCliAsync(args.ToArray()).WaitAsync(CliExecutionTimeout);
    }

    private async Task SafeDisposeContainerAsync()
    {
        if (_container is null)
        {
            return;
        }

        Console.WriteLine($"{LogPrefix} Disposing SQL Server container...");

        try
        {
            await _container.DisposeAsync(ContainerCleanupTimeout).ConfigureAwait(false);
            Console.WriteLine($"{LogPrefix} Container disposed.");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"{LogPrefix} Container disposal timed out after {ContainerCleanupTimeout.TotalSeconds:F0} seconds.");
        }
        catch (Exception disposeEx)
        {
            Console.WriteLine($"{LogPrefix} Container disposal failed: {disposeEx.Message}");
        }
        finally
        {
            _container = null;
            _dockerAvailable = false;
        }
    }

    private static bool IsDockerUnavailable(Exception ex)
    {
        if (ex is InvalidOperationException invalid && invalid.Message.Contains("docker", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (ex is System.ComponentModel.Win32Exception win32 && (win32.NativeErrorCode == 2 || win32.NativeErrorCode == 5))
        {
            return true;
        }

        if (ex is AggregateException aggregate)
        {
            return aggregate.InnerExceptions.Any(IsDockerUnavailable);
        }

        return ex.InnerException is not null && IsDockerUnavailable(ex.InnerException);
    }

    private static async Task<int> InvokeCliWithRetryAsync(string projectRoot, string command, IReadOnlyList<string> additionalArguments, int maxAttempts = 5)
    {
        const int DatabaseUnavailableExitCode = 20;
        var attempt = 0;
        var delaySeconds = 5;

        while (true)
        {
            attempt++;
            var exitCode = await InvokeCliAsync(projectRoot, command, additionalArguments.ToArray()).ConfigureAwait(false);
            if (exitCode == 0)
            {
                return exitCode;
            }

            if (exitCode != DatabaseUnavailableExitCode || attempt >= maxAttempts)
            {
                return exitCode;
            }

            Console.WriteLine($"{LogPrefix} CLI command '{command}' failed with database unavailability (exit {exitCode}) on attempt {attempt}. Retrying in {delaySeconds} seconds...");
            await Task.Delay(TimeSpan.FromSeconds(delaySeconds)).ConfigureAwait(false);
            delaySeconds = Math.Min(delaySeconds * 2, 30);
        }
    }

    private sealed class TempDirectory : IDisposable
    {
        public string Path { get; }

        private TempDirectory(string path)
        {
            Path = path;
        }

        public static TempDirectory Create(string prefix)
        {
            var directory = Directory.CreateTempSubdirectory(prefix);
            return new TempDirectory(directory.FullName);
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, recursive: true);
                }
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    private readonly struct EnvironmentVariableScope : IDisposable
    {
        private readonly (string Name, string? Original)[] _entries;

        public EnvironmentVariableScope(params (string Name, string? Value)[] variables)
        {
            _entries = new (string, string?)[variables.Length];
            for (var i = 0; i < variables.Length; i++)
            {
                var (name, value) = variables[i];
                _entries[i] = (name, Environment.GetEnvironmentVariable(name));
                Environment.SetEnvironmentVariable(name, value);
            }
        }

        public void Dispose()
        {
            foreach (var (name, original) in _entries)
            {
                Environment.SetEnvironmentVariable(name, original);
            }
        }
    }
}
