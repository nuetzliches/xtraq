using Xtraq.Configuration; // for XtraqConfiguration & NamespaceResolver
using Xtraq.Engine;
using Xtraq.Metadata; // ProcedureDescriptor
using Xtraq.Services;
using Xtraq.Utils;
namespace Xtraq.Generators;

/// <summary>
/// Generates the DbContext related artifacts from templates (interface, context, options, DI extension, endpoints).
/// Runs unconditionally in the enforced next-only configuration (mode provider remains overridable for tests).
/// </summary>
internal sealed class DbContextGenerator : GeneratorBase
{
    private readonly OutputService _outputService;
    private readonly IConsoleService _console;
    private readonly Func<IReadOnlyList<ProcedureDescriptor>> _proceduresProvider;

    public DbContextGenerator(OutputService outputService, IConsoleService console, ITemplateRenderer renderer, ITemplateLoader? loader = null, Func<IReadOnlyList<ProcedureDescriptor>>? proceduresProvider = null)
        : base(renderer, loader, configuration: null)
    {
        _outputService = outputService;
        _console = console;
        _proceduresProvider = proceduresProvider ?? (() => Array.Empty<ProcedureDescriptor>());
    }

    public Task GenerateAsync(bool isDryRun) => GenerateInternalAsync(isDryRun);

    private async Task GenerateInternalAsync(bool isDryRun)
    {
        // Load .env configuration (primary source) with safe fallback to diagnostic defaults.
        var projectRoot = DirectoryUtils.GetWorkingDirectory();
        XtraqConfiguration cfg;
        try
        {
            cfg = XtraqConfiguration.Load(projectRoot: projectRoot);
        }
        catch (Exception ex)
        {
            _console.Warn("[dbctx] Failed to load .env configuration: " + ex.Message);
            cfg = new XtraqConfiguration();
        }

        // Resolve namespace from .env first, fall back to legacy configuration.
        string? explicitNs = cfg.NamespaceRoot?.Trim();

        string baseNs;
        if (!string.IsNullOrWhiteSpace(explicitNs))
        {
            baseNs = explicitNs!;
        }
        else
        {
            try
            {
                var resolver = new NamespaceResolver(cfg, msg => _console.Warn("[dbctx] ns-resolver: " + msg));
                baseNs = resolver.Resolve(projectRoot);
                _console.Verbose($"[dbctx] Derived namespace '{baseNs}' via resolver");
            }
            catch (Exception ex)
            {
                _console.Warn("[dbctx] Unable to determine namespace: " + ex.Message);
                return;
            }
        }

        // Determine output directory using XTRAQ_OUTPUT_DIR (defaults to Xtraq).
        string outputDir = string.IsNullOrWhiteSpace(cfg.OutputDir) ? "Xtraq" : cfg.OutputDir.Trim();
        string xtraqDir;
        if (Path.IsPathRooted(outputDir))
        {
            xtraqDir = Path.GetFullPath(outputDir);
        }
        else
        {
            xtraqDir = Path.GetFullPath(Path.Combine(projectRoot, outputDir));
        }

        if (!Directory.Exists(xtraqDir) && !isDryRun)
        {
            Directory.CreateDirectory(xtraqDir);
        }

        _console.Verbose($"[dbctx] output='{xtraqDir}' namespace='{baseNs}' cwd={Directory.GetCurrentDirectory()}");

        // Append .Xtraq suffix only if not already present.
        var finalNs = baseNs.EndsWith(".Xtraq", StringComparison.Ordinal) ? baseNs : baseNs + ".Xtraq";
        // Collect procedure descriptors (may be empty in legacy mode or early pipeline stages)
        var procedures = _proceduresProvider();
        // Build method metadata (naming + signatures) only if we have procedures
        var methodBlocksInterface = new StringBuilder();
        var methodBlocksImpl = new StringBuilder();
        if (procedures.Count > 0)
        {
            _console.Verbose($"[dbctx] Generating {procedures.Count} procedure methods");
            // Naming conflict resolution per spec:
            // 1. Preserve original procedure name part (after schema) converting invalid chars to '_'
            // 2. If collision across schemas, prefix with schema pascal case.
            var nameMap = new Dictionary<string, List<(ProcedureDescriptor Proc, string SchemaPascal, string ProcPart)>>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in procedures)
            {
                var op = p.OperationName; // e.g. dbo.GetUsers
                var schema = p.Schema ?? "dbo";
                var procPart = op.Contains('.') ? op[(op.IndexOf('.') + 1)..] : op; // raw procedure part
                var normalized = NormalizeProcedurePart(procPart);
                if (!nameMap.TryGetValue(normalized, out var list)) nameMap[normalized] = list = new();
                list.Add((p, ToPascalCase(schema), procPart));
            }
            // Determine final method names
            var finalNames = new Dictionary<ProcedureDescriptor, string>();
            foreach (var kv in nameMap)
            {
                if (kv.Value.Count == 1)
                {
                    finalNames[kv.Value[0].Proc] = kv.Key; // single -> no prefix
                }
                else
                {
                    // collision -> prefix schema pascal
                    foreach (var item in kv.Value)
                    {
                        var candidate = item.SchemaPascal + kv.Key; // Schema + NormalizedProc
                        finalNames[item.Proc] = candidate;
                    }
                }
            }
            // Emit method signatures
            foreach (var p in procedures.OrderBy(p => finalNames[p]))
            {
                var methodName = finalNames[p];
                var schemaPascal = ToPascalCase(p.Schema ?? "dbo");
                var schemaNamespace = finalNs + "." + schemaPascal;
                // Types built by ProceduresGenerator: <ProcPart>Input, <ProcPart>Result, <ProcPart> (static wrapper)
                var procPart = p.OperationName.Contains('.') ? p.OperationName[(p.OperationName.IndexOf('.') + 1)..] : p.OperationName;
                var procedureTypeName = NamePolicy.Procedure(procPart); // matches template
                var unifiedResultTypeName = NamePolicy.Result(procPart);
                var inputTypeName = NamePolicy.Input(procPart);
                var hasInput = p.InputParameters?.Count > 0;
                var resultReturnType = schemaNamespace + "." + unifiedResultTypeName;
                var inputParamSig = hasInput ? schemaNamespace + "." + inputTypeName + " input, " : string.Empty;
                // Interface method
                methodBlocksInterface.AppendLine($"    Task<{resultReturnType}> {methodName}Async({inputParamSig}CancellationToken cancellationToken = default);");
                // Implementation
                methodBlocksImpl.AppendLine("    /// <summary>Executes stored procedure '" + p.Schema + "." + p.ProcedureName + "'</summary>");
                methodBlocksImpl.Append("    public async Task<" + resultReturnType + "> " + methodName + "Async(");
                if (hasInput) methodBlocksImpl.Append(inputParamSig);
                methodBlocksImpl.Append("CancellationToken cancellationToken = default)\n");
                methodBlocksImpl.AppendLine("    {");
                methodBlocksImpl.AppendLine("        await using var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);");
                methodBlocksImpl.Append("        var result = await " + schemaNamespace + "." + procedureTypeName + ".ExecuteAsync(conn");
                if (hasInput) methodBlocksImpl.Append(", input");
                methodBlocksImpl.Append(", cancellationToken).ConfigureAwait(false);\n");
                methodBlocksImpl.AppendLine("        return result;");
                methodBlocksImpl.AppendLine("    }");
                methodBlocksImpl.AppendLine();
            }
        }

        var model = new { Namespace = finalNs, MethodsInterface = methodBlocksInterface.ToString(), MethodsImpl = methodBlocksImpl.ToString() };

        // Generate core artifacts (always)
        await WriteAsync(xtraqDir, "IXtraqDbContext.cs", Render("IXtraqDbContext", GetTemplate_Interface(finalNs, model.MethodsInterface), model), isDryRun);
        await WriteAsync(xtraqDir, "XtraqDbContextOptions.cs", Render("XtraqDbContextOptions", GetTemplate_Options(finalNs), model), isDryRun);
        await WriteAsync(xtraqDir, "XtraqDbContext.cs", Render("XtraqDbContext", GetTemplate_Context(finalNs, model.MethodsImpl), model), isDryRun);
        await WriteAsync(xtraqDir, "XtraqDbContextServiceCollectionExtensions.cs", Render("XtraqDbContextServiceCollectionExtensions", GetTemplate_Di(finalNs), model), isDryRun);

        // Endpoint generation is gated to net10 (forward feature). We evaluate XTRAQ_TFM or default major used by template loader.
        var tfm = Environment.GetEnvironmentVariable("XTRAQ_TFM");
        var major = ExtractTfmMajor(tfm);
        if (major == "net10")
        {
            await WriteAsync(xtraqDir, "XtraqDbContextEndpoints.cs", Render("XtraqDbContextEndpoints", GetTemplate_Endpoints(finalNs), model), isDryRun);
        }
        else
        {
            _console.Verbose($"[dbctx] Skip endpoints (TFM '{tfm ?? "<null>"}' → major '{major ?? "<none>"}' != net10)");
        }
    }

    private static string? ExtractTfmMajor(string? tfm)
    {
        if (string.IsNullOrWhiteSpace(tfm)) return null;
        tfm = tfm.Trim().ToLowerInvariant();
        if (!tfm.StartsWith("net")) return null;
        var digits = new string(tfm.Skip(3).TakeWhile(char.IsDigit).ToArray());
        if (digits.Length == 0) return null;
        return "net" + digits;
    }

    private string Render(string logicalName, string fallback, object model)
    {
        var templateName = Path.GetFileNameWithoutExtension(logicalName);
        if (!string.IsNullOrWhiteSpace(templateName) && Templates.TryLoad(templateName, out var tpl))
        {
            try
            {
                return Templates.RenderRawTemplate(tpl, model);
            }
            catch (Exception ex)
            {
                _console.Warn($"[dbctx] Template render failed for {logicalName}, using fallback. Error: {ex.Message}");
            }
        }

        return fallback;
    }

    private static string GetTemplate_Interface(string ns, string methods) =>
        "/// <summary>Generated interface for the database context abstraction.</summary>\n" +
        $"namespace {ns};\n\n" +
        "using System.Data.Common;\n\n\n\n" +
        "public interface IXtraqDbContext\n" +
        "{\n" +
        "    DbConnection OpenConnection();\n" +
        "    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);\n" +
        "    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);\n" +
        "    int CommandTimeout { get; }\n" +
        (string.IsNullOrWhiteSpace(methods) ? string.Empty : methods) +
        "}\n";

    private static string GetTemplate_Options(string ns) =>
        $"namespace {ns};\n\n" +
        "\n\n" +
        "public sealed class XtraqDbContextOptions\n{\n" +
        "    public string? ConnectionString { get; set; }\n" +
        "    public string? ConnectionStringName { get; set; }\n" +
    "    public int? CommandTimeout { get; set; }\n" +
        "    public int? MaxOpenRetries { get; set; }\n" +
        "    public int? RetryDelayMs { get; set; }\n" +
        "    public JsonSerializerOptions? JsonSerializerOptions { get; set; }\n" +
        "    public bool EnableDiagnostics { get; set; } = true;\n" +
        "}\n";

    private static string GetTemplate_Context(string ns, string methods) =>
        $"namespace {ns};\n\n" +
        "using System.Data.Common;\n\n\n\nusing Microsoft.Data.SqlClient;\n\n" +
        "public partial class XtraqDbContext : IXtraqDbContext\n" +
        "{\n" +
        "    private readonly XtraqDbContextOptions _options;\n" +
        "    public XtraqDbContext(XtraqDbContextOptions options)\n" +
        "    {\n" +
        "        if (string.IsNullOrWhiteSpace(options.ConnectionString)) throw new System.ArgumentException(\"ConnectionString must be provided\", nameof(options));\n" +
        "        _options = options;\n" +
    "        if (_options.CommandTimeout is null or <= 0) _options.CommandTimeout = 30;\n" +
        "    }\n" +
    "    public int CommandTimeout => _options.CommandTimeout ?? 30;\n" +
        "    public DbConnection OpenConnection()\n" +
        "    {\n" +
        "        var sw = _options.EnableDiagnostics ? Stopwatch.StartNew() : null;\n" +
        "        int attempt = 0; int max = _options.MaxOpenRetries.GetValueOrDefault(0); int delay = _options.RetryDelayMs.GetValueOrDefault(200);\n" +
        "        while (true) { try { var conn = new SqlConnection(_options.ConnectionString); conn.Open(); if (_options.EnableDiagnostics) { sw!.Stop(); System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnection latency=\" + sw.ElapsedMilliseconds + \"ms attempts=\" + (attempt+1)); } return conn; } catch (SqlException ex) when (attempt < max) { attempt++; if (_options.EnableDiagnostics) { System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnection retry \" + attempt + \"/\" + max + \" after error: \" + ex.Message); } Thread.Sleep(delay); continue; } catch (SqlException ex) { if (_options.EnableDiagnostics) { System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnection failed: \" + ex.Message); } throw; } }\n" +
        "    }\n" +
        "    public async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)\n" +
        "    {\n" +
        "        var sw = _options.EnableDiagnostics ? Stopwatch.StartNew() : null;\n" +
        "        int attempt = 0; int max = _options.MaxOpenRetries.GetValueOrDefault(0); int delay = _options.RetryDelayMs.GetValueOrDefault(200);\n" +
        "        while (true) { try { var conn = new SqlConnection(_options.ConnectionString); await conn.OpenAsync(cancellationToken).ConfigureAwait(false); if (_options.EnableDiagnostics) { sw!.Stop(); System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnectionAsync latency=\" + sw.ElapsedMilliseconds + \"ms attempts=\" + (attempt+1)); } return conn; } catch (SqlException ex) when (attempt < max) { attempt++; if (_options.EnableDiagnostics) { System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnectionAsync retry \" + attempt + \"/\" + max + \" after error: \" + ex.Message); } await Task.Delay(delay, cancellationToken).ConfigureAwait(false); continue; } catch (SqlException ex) { if (_options.EnableDiagnostics) { System.Diagnostics.Debug.WriteLine(\"[XtraqDbContext] OpenConnectionAsync failed: \" + ex.Message); } throw; } }\n" +
        "    }\n" +
        "    public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)\n" +
        "    {\n" +
        "        try { await using var conn = new SqlConnection(_options.ConnectionString); await conn.OpenAsync(cancellationToken).ConfigureAwait(false); return true; } catch { return false; }\n" +
        "    }\n" +
        (string.IsNullOrWhiteSpace(methods) ? string.Empty : methods) +
        "}\n";

    private static string GetTemplate_Di(string ns) =>
        $"namespace {ns};\n\n" +
        "\nusing Microsoft.Extensions.Configuration;\n\n\n" +
        "public static class XtraqDbContextServiceCollectionExtensions\n" +
        "{\n" +
        "    public static IServiceCollection AddXtraqDbContext(this IServiceCollection services, Action<XtraqDbContextOptions>? configure = null)\n" +
        "    {\n" +
        "        var explicitOptions = new XtraqDbContextOptions(); configure?.Invoke(explicitOptions);\n" +
    "        services.AddSingleton(provider => { var cfg = provider.GetService<IConfiguration>(); var name = explicitOptions.ConnectionStringName ?? \"DefaultConnection\"; var conn = explicitOptions.ConnectionString ?? cfg?.GetConnectionString(name); if (string.IsNullOrWhiteSpace(conn)) throw new InvalidOperationException($\"No connection string resolved for XtraqDbContext (options / IConfiguration:GetConnectionString('{name}')).\"); explicitOptions.ConnectionString = conn; if (explicitOptions.CommandTimeout is null or <= 0) explicitOptions.CommandTimeout = 30; if (explicitOptions.MaxOpenRetries is not null and < 0) throw new InvalidOperationException(\"MaxOpenRetries must be >= 0\"); if (explicitOptions.RetryDelayMs is not null and <= 0) throw new InvalidOperationException(\"RetryDelayMs must be > 0\"); return explicitOptions; });\n" +
        "        services.AddScoped<IXtraqDbContext>(sp => new XtraqDbContext(sp.GetRequiredService<XtraqDbContextOptions>()));\n" +
        "        return services;\n" +
        "    }\n" +
        "}\n";

    private static string GetTemplate_Endpoints(string ns) =>
        $"namespace {ns};\n\n" +
        "using Microsoft.AspNetCore.Builder;\nusing Microsoft.AspNetCore.Http;\n\n\n\n\n" +
        "public static class XtraqDbContextEndpointRouteBuilderExtensions\n" +
        "{\n" +
        "    public static IEndpointRouteBuilder MapXtraqDbContextEndpoints(this IEndpointRouteBuilder endpoints)\n" +
        "    {\n" +
        "        endpoints.MapGet(\"/xtraq/health/db\", async (IXtraqDbContext db, CancellationToken ct) => { var healthy = await db.HealthCheckAsync(ct).ConfigureAwait(false); return healthy ? Results.Ok(new { status = \"ok\" }) : Results.Problem(\"database unavailable\", statusCode: 503); });\n" +
        "        return endpoints;\n" +
        "    }\n" +
        "}\n";
    private async Task WriteAsync(string dir, string fileName, string source, bool isDryRun)
    {
        var path = Path.Combine(dir, fileName);
        await _outputService.WriteAsync(path, source, isDryRun);
    }

    private static string NormalizeProcedurePart(string procPart)
    {
        if (string.IsNullOrWhiteSpace(procPart)) return "Procedure";
        var sb = new StringBuilder(procPart.Length);
        foreach (var ch in procPart)
        {
            sb.Append(char.IsLetterOrDigit(ch) ? ch : '_');
        }
        var candidate = sb.ToString();
        if (char.IsDigit(candidate[0])) candidate = "N" + candidate; // ensure valid identifier start
        return candidate;
    }

    private static string ToPascalCase(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "Schema";
        var parts = input.Split(new[] { '-', '_', ' ', '.', '/', '\\' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .Select(p => char.ToUpperInvariant(p[0]) + (p.Length > 1 ? p.Substring(1).ToLowerInvariant() : string.Empty));
        var candidate = string.Concat(parts);
        candidate = new string(candidate.Where(ch => char.IsLetterOrDigit(ch) || ch == '_').ToArray());
        if (string.IsNullOrEmpty(candidate)) candidate = "Schema";
        if (char.IsDigit(candidate[0])) candidate = "N" + candidate;
        return candidate;
    }
}

