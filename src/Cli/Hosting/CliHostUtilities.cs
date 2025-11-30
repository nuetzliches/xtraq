using Xtraq.Configuration;
using Xtraq.Utils;

namespace Xtraq.Cli.Hosting;

/// <summary>
/// Provides shared helpers for CLI environment bootstrapping, argument normalization, and metadata capture.
/// </summary>
internal static class CliHostUtilities
{
    private static readonly HashSet<string> KnownCommandNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "init",
        "snapshot",
        "build",
        "version",
        "update"
    };

    public static string[] NormalizeInvocationArguments(string[] args)
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

    public static string NormalizeProjectPath(string? value)
    {
        static bool LooksLikeEnv(string hint) => hint.EndsWith(".env", StringComparison.OrdinalIgnoreCase) || hint.EndsWith(".env.local", StringComparison.OrdinalIgnoreCase);

        var fallback = Directory.GetCurrentDirectory();
        if (string.IsNullOrWhiteSpace(value))
        {
            Environment.SetEnvironmentVariable(Xtraq.Configuration.TrackableConfigManager.ProjectRootLockEnvironmentVariableName, null);
            var resolvedDefault = Xtraq.Configuration.TrackableConfigManager.ResolveProjectRoot(fallback);
            UpdateProjectEnvironment(resolvedDefault);
            return resolvedDefault;
        }

        string NormalizeDirectory(string candidate)
        {
            try
            {
                return Path.GetFullPath(candidate);
            }
            catch
            {
                return candidate;
            }
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

        string ResolveExplicitRoot(string path)
        {
            if (File.Exists(path))
            {
                var directory = Path.GetDirectoryName(path) ?? fallback;
                return NormalizeDirectory(directory);
            }

            if (LooksLikeEnv(path))
            {
                var directory = Path.GetDirectoryName(path) ?? fallback;
                return NormalizeDirectory(directory);
            }

            if (Directory.Exists(path))
            {
                return NormalizeDirectory(path);
            }

            return NormalizeDirectory(path);
        }

        var resolvedRoot = ResolveExplicitRoot(candidate);
        Environment.SetEnvironmentVariable(Xtraq.Configuration.TrackableConfigManager.ProjectRootLockEnvironmentVariableName, resolvedRoot);
        UpdateProjectEnvironment(resolvedRoot);
        return resolvedRoot;
    }

    public static void UpdateProjectEnvironment(string? projectRoot)
    {
        if (string.IsNullOrWhiteSpace(projectRoot))
        {
            return;
        }

        var normalized = projectRoot;
        try
        {
            normalized = Path.GetFullPath(projectRoot);
        }
        catch
        {
        }

        Environment.SetEnvironmentVariable("XTRAQ_PROJECT_PATH", normalized);
        EnsureSnapshotRoot(normalized);
    }

    public static void EnsureSnapshotRoot(string projectRoot)
    {
        if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT")))
        {
            return;
        }

        try
        {
            var snapshotPath = Path.Combine(projectRoot, ".xtraq");
            Environment.SetEnvironmentVariable("XTRAQ_SNAPSHOT_ROOT", snapshotPath);
        }
        catch
        {
        }
    }

    public static string? TryExtractProjectHint(string[] args)
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

    public static (string configPath, string? projectRoot) NormalizeCliProjectHint(string rawInput)
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

    public static string NormalizeProcedureFilter(string? rawValue)
    {
        return TryNormalizeProcedureFilter(rawValue, out var normalized, out _) ? normalized ?? string.Empty : string.Empty;
    }

    public static bool TryNormalizeProcedureFilter(string? rawValue, out string? normalized, out string? error)
    {
        normalized = null;
        error = null;

        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return true;
        }

        var tokens = rawValue
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(static token => token.Trim())
            .Where(static token => token.Length > 0);

        var deduped = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var token in tokens)
        {
            if (!IsValidProcedureFilterToken(token))
            {
                error = $"Invalid procedure filter '{token}'. Use schema.name with optional '*' or '?' wildcards.";
                normalized = null;
                return false;
            }

            if (seen.Add(token))
            {
                deduped.Add(token);
            }
        }

        if (deduped.Count == 0)
        {
            normalized = null;
            return true;
        }

        normalized = string.Join(',', deduped);
        return true;
    }

    public static string BuildSessionMetadataJson(string commandName, CliCommandOptions options, string projectPath, string environment, bool refreshRequested)
    {
        var resolvedRoot = ProjectRootResolver.ResolveCurrent();
        var workingDirectory = NormalizePathSafe(Directory.GetCurrentDirectory());
        var normalizedProjectPath = NormalizePathSafe(projectPath);
        var normalizedProjectRoot = NormalizePathSafe(resolvedRoot);
        var commandValue = string.IsNullOrWhiteSpace(commandName) ? "default" : commandName;
        var metadata = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        {
            ["version"] = ResolveProductVersion()
        };

        var namespaceHint = TryResolveNamespaceHint(normalizedProjectPath);
        if (!string.IsNullOrWhiteSpace(namespaceHint))
        {
            metadata["namespace"] = namespaceHint;
        }

        if (!string.Equals(normalizedProjectPath, workingDirectory, StringComparison.OrdinalIgnoreCase))
        {
            var relativeProject = TryGetRelative(workingDirectory, normalizedProjectPath);
            metadata["projectPath"] = string.IsNullOrWhiteSpace(relativeProject) ? normalizedProjectPath : relativeProject;
        }

        if (!string.IsNullOrWhiteSpace(environment) && !string.Equals(environment, "Production", StringComparison.OrdinalIgnoreCase))
        {
            metadata["environment"] = environment;
        }

        metadata["command"] = commandValue;

        if (options.Verbose)
        {
            metadata["workingDirectory"] = workingDirectory;
            metadata["timestampUtc"] = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }

        if (!string.IsNullOrWhiteSpace(normalizedProjectRoot))
        {
            var differsFromPath = !string.Equals(normalizedProjectRoot, normalizedProjectPath, StringComparison.OrdinalIgnoreCase);
            if (options.Verbose && differsFromPath)
            {
                var relativeRoot = TryGetRelative(workingDirectory, normalizedProjectRoot);
                metadata["projectRoot"] = string.IsNullOrWhiteSpace(relativeRoot) ? normalizedProjectRoot : relativeRoot;
            }
        }

        var projectHint = Environment.GetEnvironmentVariable("XTRAQ_PROJECT_PATH");
        if (!string.IsNullOrWhiteSpace(projectHint))
        {
            var normalizedHint = NormalizePathSafe(projectHint);
            var differsFromRoot = !string.Equals(normalizedHint, normalizedProjectRoot, StringComparison.OrdinalIgnoreCase);
            if (options.Verbose && differsFromRoot)
            {
                var relativeHint = TryGetRelative(workingDirectory, normalizedHint);
                metadata["projectRootHint"] = string.IsNullOrWhiteSpace(relativeHint) ? normalizedHint : relativeHint;
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

        return JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
    }

    private static string? TryResolveNamespaceHint(string projectPath)
    {
        try
        {
            var cfg = XtraqConfiguration.Load(projectRoot: projectPath, requireGeneratorConnection: false);
            if (!string.IsNullOrWhiteSpace(cfg?.NamespaceRoot))
            {
                return cfg.NamespaceRoot!.Trim();
            }
        }
        catch
        {
            // Config not ready yet (xtraq init) or load failed; ignore.
        }

        return null;
    }

    public static string ResolveProductBanner() => "Xtraq";

    public static string ResolveProductVersion()
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

    public static string NormalizePathSafe(string? value)
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

    private static string? TryGetRelative(string basePath, string targetPath)
    {
        try
        {
            var relative = Path.GetRelativePath(basePath, targetPath);
            if (string.IsNullOrWhiteSpace(relative) || relative == ".")
            {
                return null;
            }

            return relative.Replace(Path.DirectorySeparatorChar, '/');
        }
        catch
        {
            return null;
        }
    }

    private static bool IsValidProcedureFilterToken(string token)
    {
        var separatorIndex = token.IndexOf('.');
        if (separatorIndex <= 0 || separatorIndex == token.Length - 1)
        {
            return false;
        }

        var schemaSpan = token.AsSpan(0, separatorIndex);
        var procedureSpan = token.AsSpan(separatorIndex + 1);
        return IsValidIdentifierSegment(schemaSpan) && IsValidIdentifierSegment(procedureSpan);
    }

    private static bool IsValidIdentifierSegment(ReadOnlySpan<char> segment)
    {
        if (segment.Length == 0)
        {
            return false;
        }

        foreach (var ch in segment)
        {
            if (char.IsLetterOrDigit(ch))
            {
                continue;
            }

            if (ch is '_' or '-' or '*' or '?')
            {
                continue;
            }

            return false;
        }

        return true;
    }
}
