using System.Data;
using Xtraq.Configuration;
using Xtraq.Diagnostics;
using Xtraq.Engine;
using Xtraq.Metadata;
using Xtraq.Models;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Generators;

internal sealed record ProcedureGenerationResult(int TotalArtifacts, IReadOnlyDictionary<string, int> ArtifactsPerSchema);

internal sealed class ProceduresGenerator : GeneratorBase
{
    private readonly Func<IReadOnlyList<ProcedureDescriptor>> _provider;
    private readonly string _projectRoot;
    private readonly IJsonFunctionEnhancementService? _jsonEnhancementService;
    private readonly Func<string, string, FunctionJsonDescriptor?>? _functionJsonResolver;

    public ProceduresGenerator(
        ITemplateRenderer renderer,
        Func<IReadOnlyList<ProcedureDescriptor>> provider,
        ITemplateLoader? loader = null,
        string? projectRoot = null,
        XtraqConfiguration? cfg = null,
        IJsonFunctionEnhancementService? jsonEnhancementService = null,
        Func<string, string, FunctionJsonDescriptor?>? functionJsonResolver = null)
        : base(renderer, loader, cfg)
    {
        _provider = provider;
        _projectRoot = projectRoot ?? Directory.GetCurrentDirectory();
        _jsonEnhancementService = jsonEnhancementService;
        _functionJsonResolver = functionJsonResolver;
    }

    private static string? ComposeSchemaObjectRef(string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return null;
        var cleanName = name.Trim();
        if (cleanName.Length == 0) return null;
        var cleanSchema = string.IsNullOrWhiteSpace(schema) ? null : schema.Trim();
        return cleanSchema != null ? string.Concat(cleanSchema, ".", cleanName) : cleanName;
    }

    private static (string? Schema, string? Name) SplitSchemaObject(string? reference, string? fallbackSchema)
    {
        if (string.IsNullOrWhiteSpace(reference))
        {
            return (string.IsNullOrWhiteSpace(fallbackSchema) ? null : fallbackSchema?.Trim(), null);
        }
        var trimmed = reference.Trim();
        if (trimmed.Length == 0)
        {
            return (string.IsNullOrWhiteSpace(fallbackSchema) ? null : fallbackSchema?.Trim(), null);
        }
        var parts = trimmed.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return (string.IsNullOrWhiteSpace(fallbackSchema) ? null : fallbackSchema?.Trim(), null);
        }
        if (parts.Length == 1)
        {
            return (string.IsNullOrWhiteSpace(fallbackSchema) ? null : fallbackSchema?.Trim(), parts[0]);
        }
        var schema = string.IsNullOrWhiteSpace(parts[0]) ? (string.IsNullOrWhiteSpace(fallbackSchema) ? null : fallbackSchema?.Trim()) : parts[0];
        var name = parts.Length > 1 ? parts[1] : null;
        if (string.IsNullOrWhiteSpace(name))
        {
            return (schema, null);
        }
        return (schema, name);
    }

    private static string StripMinimalApiExtensions(string builderCode)
    {
        if (string.IsNullOrEmpty(builderCode))
        {
            return builderCode;
        }

        const string startMarker = "#if NET8_0_OR_GREATER && XTRAQ_MINIMAL_API";
        const string endMarker = "#endif";

        var startIndex = builderCode.IndexOf(startMarker, StringComparison.Ordinal);
        if (startIndex < 0)
        {
            return builderCode;
        }

        var endIndex = builderCode.IndexOf(endMarker, startIndex, StringComparison.Ordinal);
        if (endIndex < 0)
        {
            return builderCode;
        }

        var removalEnd = endIndex + endMarker.Length;
        while (removalEnd < builderCode.Length && (builderCode[removalEnd] == '\r' || builderCode[removalEnd] == '\n'))
        {
            removalEnd++;
        }

        return builderCode.Remove(startIndex, removalEnd - startIndex);
    }

    public ProcedureGenerationResult Generate(string ns, string baseOutputDir)
    {
        // Capture full unfiltered list (needed for cross-schema forwarding even if target schema excluded by allow-list)
        var allProcedures = _provider();
        var originalLookup = allProcedures
            .GroupBy(p => (p.Schema ?? "dbo") + "." + (p.ProcedureName ?? string.Empty), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
        // Work list subject to filters
        var procs = allProcedures.ToList();

        var written = 0;
        var artifactsPerSchema = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        ProcedureGenerationResult BuildResult()
            => new ProcedureGenerationResult(
                written,
                new Dictionary<string, int>(artifactsPerSchema, StringComparer.OrdinalIgnoreCase));

        var emitJsonIncludeNullValues = ShouldEmitJsonIncludeNullValues();
        var emitEntityFrameworkIntegration = ShouldEmitEntityFrameworkIntegration();

        // Check for explicit procedure filter first
        var buildProceduresRaw = Environment.GetEnvironmentVariable("XTRAQ_BUILD_PROCEDURES");
        var hasExplicitProcedures = !string.IsNullOrWhiteSpace(buildProceduresRaw);

        // 1) Positive allow-list: prefer XtraqConfiguration.BuildSchemas; fallback to direct env var only if cfg missing
        // Skip schema filtering if procedures are explicitly specified
        HashSet<string>? buildSchemas = null;
        if (!hasExplicitProcedures)
        {
            var cfg = Configuration;
            if (cfg?.BuildSchemas is { Count: > 0 })
            {
                buildSchemas = new HashSet<string>(cfg.BuildSchemas, StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                var buildSchemasRaw = Environment.GetEnvironmentVariable("XTRAQ_BUILD_SCHEMAS");
                if (!string.IsNullOrWhiteSpace(buildSchemasRaw))
                {
                    buildSchemas = new HashSet<string>(buildSchemasRaw!
                        .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(s => s.Trim())
                        .Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
                }
            }
            if (buildSchemas is { Count: > 0 })
            {
                var before = procs.Count;
                // Filter strictly by schema list
                procs = procs.Where(p => buildSchemas.Contains(p.Schema ?? "dbo")).ToList();
                var removed = before - procs.Count;
                try { Console.Out.WriteLine($"[xtraq] Info: BuildSchemas allow-list active -> {procs.Count} of {before} procedures retained. Removed: {removed}. Schemas: {string.Join(",", buildSchemas)}"); } catch { }
            }
        }

        // 1.5) Procedure-specific allow-list (XTRAQ_BUILD_PROCEDURES) 
        // DISABLED in Code Generation - procedure filtering is only for schema snapshots (snapshot command)
        // Code generation is controlled exclusively by XTRAQ_BUILD_SCHEMAS
        /*
        HashSet<string>? buildProcedures = null;
        if (!string.IsNullOrWhiteSpace(buildProceduresRaw))
        {
            buildProcedures = new HashSet<string>(buildProceduresRaw!
                .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
        }
        if (buildProcedures is { Count: > 0 })
        {
            var before = procs.Count;
            // Filter by procedure list (format: schema.procedurename or just procedurename)
            procs = procs.Where(p =>
            {
                var fullName = $"{p.Schema}.{p.ProcedureName}";
                var procedureName = p.ProcedureName;
                return buildProcedures.Contains(fullName) || buildProcedures.Contains(procedureName);
            }).ToList();
            var removed = before - procs.Count;
            try { Console.Out.WriteLine($"[xtraq] Info: BuildProcedures allow-list active -> {procs.Count} of {before} procedures retained. Removed: {removed}. Procedures: {string.Join(",", buildProcedures)}"); } catch { }
        }
        */

        if (procs.Count == 0)
        {
            try { Console.Out.WriteLine("[xtraq] Info: ProceduresGenerator skipped – provider returned 0 procedures."); } catch { }
            return BuildResult();
        }
        // Removed forwarding clone phase: ExecSource placeholders remain unchanged.
        // Expansion now happens during template mapping (generation time) instead of mutating result sets.
        try
        {
            if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug))
            {
                foreach (var fp in procs)
                {
                    Console.Out.WriteLine($"[proc-forward-debug-summary] {fp.Schema}.{fp.ProcedureName} sets={fp.ResultSets.Count} -> {string.Join(";", fp.ResultSets.Select(r => r.Name + ":" + r.Fields.Count))}");
                }
            }
        }
        catch { }
        var headerBlock = Templates.HeaderBlock;
        // Emit ExecutionSupport once (if template present and file missing or stale)
        if (Templates.TryLoad("ExecutionSupport", out var execTpl))
        {
            var execPath = Path.Combine(baseOutputDir, "ExecutionSupport.cs");
            bool write = !File.Exists(execPath);
            if (!write)
            {
                try
                {
                    var existing = File.ReadAllText(execPath);
                    // Rewrite when namespace mismatches or ExecutionSupport is missing updated helpers
                    if (!existing.Contains($"namespace {ns};", StringComparison.Ordinal) ||
                        !existing.Contains("TvpHelper", StringComparison.Ordinal) ||
                        !existing.Contains("ReaderUtil", StringComparison.Ordinal) ||
                        !existing.Contains("StreamResultSetAsync", StringComparison.Ordinal) ||
                        !existing.Contains("IXtraqProcedureInterceptorProvider", StringComparison.Ordinal))
                    {
                        write = true;
                    }
                }
                catch
                {
                    write = true;
                }
            }
            if (write)
            {
                var execModel = new { Namespace = ns, HEADER = headerBlock };
                var code = Templates.RenderRawTemplate(execTpl, execModel);
                File.WriteAllText(execPath, code);
            }
        }
        if (Templates.TryLoad("ProcedureInterceptors", out var interceptorTpl))
        {
            var interceptorPath = Path.Combine(baseOutputDir, "IXtraqProcedureInterceptorProvider.cs");
            bool writeInterceptor = !File.Exists(interceptorPath);
            if (!writeInterceptor)
            {
                try
                {
                    var existing = File.ReadAllText(interceptorPath);
                    if (!existing.Contains("IXtraqProcedureInterceptor", StringComparison.Ordinal) ||
                        !existing.Contains($"namespace {ns};", StringComparison.Ordinal))
                    {
                        writeInterceptor = true;
                    }
                }
                catch
                {
                    writeInterceptor = true;
                }
            }

            if (writeInterceptor)
            {
                var interceptorModel = new { Namespace = ns, HEADER = headerBlock };
                var code = Templates.RenderRawTemplate(interceptorTpl, interceptorModel);
                File.WriteAllText(interceptorPath, code);
            }
        }
        var adapterPath = Path.Combine(baseOutputDir, "ProcedureResultEntityAdapter.cs");
        if (emitEntityFrameworkIntegration && Templates.TryLoad("ProcedureResultEntityAdapter", out var adapterTpl))
        {
            bool writeAdapter = !File.Exists(adapterPath);
            if (!writeAdapter)
            {
                try
                {
                    var existing = File.ReadAllText(adapterPath);
                    if (!existing.Contains("ProcedureResultEntityAdapter", StringComparison.Ordinal) ||
                        !existing.Contains($"namespace {ns};", StringComparison.Ordinal))
                    {
                        writeAdapter = true;
                    }
                }
                catch
                {
                    writeAdapter = true;
                }
            }

            if (writeAdapter)
            {
                var adapterModel = new { Namespace = ns, HEADER = headerBlock };
                var adapterCode = Templates.RenderRawTemplate(adapterTpl, adapterModel);
                File.WriteAllText(adapterPath, adapterCode);
            }
        }
        else if (!emitEntityFrameworkIntegration && File.Exists(adapterPath))
        {
            try
            {
                File.Delete(adapterPath);
            }
            catch
            {
                // Ignore failures when cleaning up optional adapter artifacts.
            }
        }
        if (Templates.TryLoad("ProcedureBuilders", out var builderTpl))
        {
            var builderPath = Path.Combine(baseOutputDir, "ProcedureBuilders.cs");
            bool writeBuilder = !File.Exists(builderPath);
            if (!writeBuilder)
            {
                try
                {
                    var existing = File.ReadAllText(builderPath);
                    if (!existing.Contains("ProcedureCallBuilder", StringComparison.Ordinal) ||
                        !existing.Contains("ProcedureStreamBuilder", StringComparison.Ordinal) ||
                        !existing.Contains($"namespace {ns};", StringComparison.Ordinal))
                    {
                        writeBuilder = true;
                    }
                }
                catch
                {
                    writeBuilder = true;
                }
            }

            if (writeBuilder)
            {
                var builderProcedures = procs
                    .Select(proc =>
                    {
                        var schemaPart = proc.Schema ?? "dbo";
                        var operationPart = proc.OperationName ?? proc.ProcedureName ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(operationPart))
                        {
                            operationPart = proc.ProcedureName ?? string.Empty;
                        }

                        var schemaFromOperation = operationPart;
                        var schemaSeparatorIndex = schemaFromOperation.IndexOf('.');
                        if (schemaSeparatorIndex >= 0)
                        {
                            schemaPart = schemaFromOperation[..schemaSeparatorIndex];
                            operationPart = schemaSeparatorIndex + 1 < schemaFromOperation.Length
                                ? schemaFromOperation[(schemaSeparatorIndex + 1)..]
                                : operationPart;
                        }

                        var schemaPascal = ToPascalCase(schemaPart);
                        var procedureTypeName = NamePolicy.Procedure(operationPart);
                        var inputTypeName = NamePolicy.Input(operationPart);
                        var resultTypeName = NamePolicy.Result(operationPart);
                        var methodSuffix = procedureTypeName.Length > 0 && procedureTypeName[0] == '@'
                            ? procedureTypeName[1..]
                            : procedureTypeName;
                        if (string.IsNullOrEmpty(methodSuffix))
                        {
                            methodSuffix = "Procedure";
                        }

                        var methodPrefix = string.IsNullOrWhiteSpace(schemaPascal) ? string.Empty : schemaPascal;
                        var methodName = "With" + methodPrefix + methodSuffix + "Procedure";

                        var schemaQualifiedNamespace = string.IsNullOrWhiteSpace(schemaPascal)
                            ? ns
                            : string.Concat(ns, ".", schemaPascal);

                        return new
                        {
                            DisplayName = ComposeSchemaObjectRef(proc.Schema, proc.ProcedureName) ?? proc.OperationName ?? operationPart,
                            MethodName = methodName,
                            InputTypeName = string.Concat("global::", schemaQualifiedNamespace, ".", inputTypeName),
                            ResultTypeName = string.Concat("global::", schemaQualifiedNamespace, ".", resultTypeName),
                            ExtensionTypeName = string.Concat("global::", schemaQualifiedNamespace, ".", procedureTypeName, "Extensions"),
                            ProcedureMethodName = procedureTypeName + "Async"
                        };
                    })
                    .OrderBy(p => p.DisplayName, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var builderModel = new { Namespace = ns, HEADER = headerBlock, Procedures = builderProcedures };
                var builderCode = Templates.RenderRawTemplate(builderTpl, builderModel);
                if (!ShouldEmitMinimalApiExtensions())
                {
                    builderCode = StripMinimalApiExtensions(builderCode);
                }
                File.WriteAllText(builderPath, builderCode);
            }
        }
        if (Templates.TryLoad("TransactionOrchestrator", out var orchestratorTpl))
        {
            var orchestratorPath = Path.Combine(baseOutputDir, "TransactionOrchestrator.cs");
            var writeOrchestrator = !File.Exists(orchestratorPath);
            if (!writeOrchestrator)
            {
                try
                {
                    var existing = File.ReadAllText(orchestratorPath);
                    if (!existing.Contains("IXtraqTransactionOrchestrator", StringComparison.Ordinal) ||
                        !existing.Contains("XtraqTransactionScope", StringComparison.Ordinal) ||
                        !existing.Contains($"namespace {ns};", StringComparison.Ordinal))
                    {
                        writeOrchestrator = true;
                    }
                }
                catch
                {
                    writeOrchestrator = true;
                }
            }

            if (writeOrchestrator)
            {
                var orchestratorModel = new { Namespace = ns, HEADER = headerBlock };
                var orchestratorCode = Templates.RenderRawTemplate(orchestratorTpl, orchestratorModel);
                File.WriteAllText(orchestratorPath, orchestratorCode);
            }
        }
        if (Templates.TryLoad("TransactionExecutionPolicy", out var transactionPolicyTpl))
        {
            var policyPath = Path.Combine(baseOutputDir, "TransactionExecutionPolicy.cs");
            var writePolicy = !File.Exists(policyPath);
            if (!writePolicy)
            {
                try
                {
                    var existing = File.ReadAllText(policyPath);
                    if (!existing.Contains("TransactionScopeExecutionPolicy", StringComparison.Ordinal) ||
                        !existing.Contains("TransactionScopeExecutionPolicyFactory", StringComparison.Ordinal) ||
                        !existing.Contains($"namespace {ns};", StringComparison.Ordinal))
                    {
                        writePolicy = true;
                    }
                }
                catch
                {
                    writePolicy = true;
                }
            }

            if (writePolicy)
            {
                var policyModel = new { Namespace = ns, HEADER = headerBlock };
                var policyCode = Templates.RenderRawTemplate(transactionPolicyTpl, policyModel);
                File.WriteAllText(policyPath, policyCode);
            }
        }
        // StoredProcedure template no longer used after consolidation
        string? unifiedTemplateRaw = null;
        bool hasUnifiedTemplate = Templates.TryLoad("UnifiedProcedure", out unifiedTemplateRaw);
        if (!hasUnifiedTemplate)
        {
            try
            {
                Console.Out.WriteLine("[xtraq] Warn: UnifiedProcedure.xqt not found – generating fallback skeleton (check template path)");
                var names = string.Join(",", Templates.ListNames());
                Console.Out.WriteLine("[xtraq] Template loader names: " + (names.Length == 0 ? "<empty>" : names));
            }
            catch { }
        }
        foreach (var proc in procs.OrderBy(p => p.OperationName))
        {
            var op = proc.OperationName;
            string schemaPart = proc.Schema ?? "dbo";
            string procPart = op;
            var idx = op.IndexOf('.');
            if (idx > 0)
            {
                schemaPart = op.Substring(0, idx);
                procPart = op[(idx + 1)..];
            }
            var schemaPascal = ToPascalCase(schemaPart);
            var finalNs = ns + "." + schemaPascal;
            var schemaDir = Path.Combine(baseOutputDir, schemaPascal);
            Directory.CreateDirectory(schemaDir);
            var procedureTypeName = NamePolicy.Procedure(procPart);
            // Aggregate type: use <Proc>Aggregate without adding an extra 'Result' suffix
            // Align with existing tests expecting <Proc>Result as unified aggregate type
            var unifiedResultTypeName = NamePolicy.Result(procPart);
            var inputTypeName = NamePolicy.Input(procPart);
            var outputTypeName = NamePolicy.Output(procPart);
            // JSON type-correction tracking for this procedure (outside the template block so it stays available later)
            var jsonTypeCorrections = new List<string>();

            // Cross-Schema EXEC Forwarding stub:
            // ProcedureDescriptor currently lacks raw SQL text; forwarding requires SQL to detect EXEC-only wrappers.
            // No-op for now; real implementation will enrich metadata (SqlText) and then merge target result sets.
            // Placeholder log (verbose) can be enabled later via env flag XTRAQ_FORWARDING_DIAG.
            // if (Environment.GetEnvironmentVariable("XTRAQ_FORWARDING_DIAG") == "1")
            //     try { Console.Out.WriteLine($"[proc-forward-xschema][skip] Missing SqlText metadata for {proc.Schema}.{proc.ProcedureName}"); } catch { }

            // Remove legacy files (<Proc>Result.cs, <Proc>Input.cs, <Proc>Output.cs) before writing the new consolidated file
            try
            {
                var legacyFiles = new[]
                {
                    Path.Combine(schemaDir, procPart + "Result.cs"),
                    Path.Combine(schemaDir, procPart + "Input.cs"),
                    Path.Combine(schemaDir, procPart + "Output.cs")
                };
                foreach (var lf in legacyFiles)
                {
                    if (File.Exists(lf)) File.Delete(lf);
                }
            }
            catch { }
            string finalCode;
            if (hasUnifiedTemplate && unifiedTemplateRaw != null)
            {
                // Build dynamic using directives including cross-schema table-type references
                var usingSet = new HashSet<string>
                {
                    "",
                    "",
                    "",
                    "using System;",
                    "using System.Collections.Generic;",
                    "using System.Data;",
                    "using System.Data.Common;",
                    "using System.Linq;",
                    "using System.Threading;",
                    "using System.Threading.Tasks;",
                    "",
                    "",
                    $"using global::{ns};"
                };
                var requiredFunctionTypeNamespaces = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                // Add schema namespaces when input parameters reference table types from other schemas
                foreach (var ipParam in proc.InputParameters)
                {
                    if (ipParam.Attributes != null && ipParam.Attributes.Any(a => a.StartsWith("[TableTypeSchema(", StringComparison.Ordinal)))
                    {
                        var attr = ipParam.Attributes.First(a => a.StartsWith("[TableTypeSchema(", StringComparison.Ordinal));
                        var schemaNameRaw = attr.Substring("[TableTypeSchema(".Length);
                        schemaNameRaw = schemaNameRaw.TrimEnd(')', ' ');
                        if (!string.IsNullOrWhiteSpace(schemaNameRaw))
                        {
                            var schemaPascalX = ToPascalCase(schemaNameRaw);
                            // Skip if same schema as current proc
                            if (!schemaPascalX.Equals(schemaPascal, StringComparison.Ordinal))
                            {
                                usingSet.Add($"using global::{ns}.{schemaPascalX};");
                            }
                        }
                    }
                }
                // Structured metadata for template-driven record generation
                var rsMeta = new List<object>();
                int rsIdx = 0;
                var streamSuffixTracker = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                // jsonTypeCorrections already prepared above
                // Generation-time expansion: if a result set is just an ExecSource placeholder (no fields),
                // expand its target result sets virtually (inline) without mutating the original descriptor list.
                foreach (var rs in proc.ResultSets.OrderBy(r => r.Index))
                {
                    bool isExecPlaceholder = rs.Fields.Count == 0 && (!string.IsNullOrWhiteSpace(rs.ExecSourceProcedureName) || !string.IsNullOrWhiteSpace(rs.ProcedureRef));
                    if (isExecPlaceholder)
                    {
                        var targetRef = SplitSchemaObject(rs.ProcedureRef, rs.ExecSourceSchemaName ?? "dbo");
                        var targetSchema = targetRef.Schema ?? rs.ExecSourceSchemaName ?? "dbo";
                        var targetProcName = targetRef.Name ?? rs.ExecSourceProcedureName;
                        var targetKey = targetSchema + "." + targetProcName;
                        if (originalLookup.TryGetValue(targetKey, out var targetProc) && targetProc.ResultSets != null && targetProc.ResultSets.Count > 0)
                        {
                            if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug)) { try { Console.Out.WriteLine($"[proc-forward-expand] {proc.Schema}.{proc.ProcedureName} expanding placeholder -> {targetKey} sets={targetProc.ResultSets.Count}"); } catch { } }
                            foreach (var tSet in targetProc.ResultSets)
                            {
                                // Skip empty target ResultSets in virtual expansion
                                if (tSet.Fields.Count == 0)
                                {
                                    if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug)) { try { Console.Out.WriteLine($"[proc-virtual-skip] {proc.Schema}.{proc.ProcedureName} skipping empty target ResultSet {tSet.Name} from {targetKey}"); } catch { } }
                                    continue;
                                }

                                // Virtual projection: reuse target fields and JSON flags, retain placeholder ExecSource values
                                var baseName = string.IsNullOrWhiteSpace(tSet.Name) ? "ResultSet" : tSet.Name;
                                var forwardedName = !string.IsNullOrWhiteSpace(targetProcName) ? targetProcName + "_" + baseName : baseName;
                                var virtualRs = new ResultSetDescriptor(
                                    Index: rsIdx,
                                    Name: forwardedName,
                                    Fields: tSet.Fields,
                                    IsScalar: tSet.IsScalar,
                                    Optional: tSet.Optional,
                                    HasSelectStar: tSet.HasSelectStar,
                                    ExecSourceSchemaName: rs.ExecSourceSchemaName,
                                    ExecSourceProcedureName: rs.ExecSourceProcedureName,
                                    ProcedureRef: rs.ProcedureRef ?? ComposeSchemaObjectRef(targetSchema, targetProcName),
                                    JsonPayload: tSet.JsonPayload,
                                    JsonStructure: tSet.JsonStructure
                                );
                                // Continue processing as a regular result set (mapping and record emission)
                                AppendResultSetMeta(virtualRs);
                            }
                            continue; // do not emit the placeholder itself
                        }
                        else
                        {
                            if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug)) { try { Console.Out.WriteLine($"[proc-forward-expand][skip] target missing for {targetKey}"); } catch { } }
                            // Skip placeholders with missing targets to avoid emitting empty records
                            continue;
                        }
                    }
                    // Only process result sets that contain fields (skip any empty ones)
                    if (rs.Fields.Count == 0)
                    {
                        if (LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug)) { try { Console.Out.WriteLine($"[proc-skip-empty] {proc.Schema}.{proc.ProcedureName} skipping empty ResultSet {rs.Name}"); } catch { } }
                        continue;
                    }
                    AppendResultSetMeta(rs);
                }

                void AppendResultSetMeta(ResultSetDescriptor rs)
                {
                    static string ApplyNullability(string typeName, bool nullable)
                    {
                        var trimmed = string.IsNullOrWhiteSpace(typeName) ? string.Empty : typeName.Trim();
                        if (!nullable || trimmed.Length == 0)
                        {
                            return trimmed;
                        }

                        if (trimmed.EndsWith("?", StringComparison.Ordinal))
                        {
                            return trimmed;
                        }

                        if (string.Equals(trimmed, "string", StringComparison.Ordinal))
                        {
                            return "string?";
                        }

                        if (string.Equals(trimmed, "object", StringComparison.Ordinal))
                        {
                            return "object?";
                        }

                        if (string.Equals(trimmed, "byte[]", StringComparison.Ordinal))
                        {
                            return "byte[]?";
                        }

                        if (trimmed.StartsWith("System.Collections.Generic.List<", StringComparison.Ordinal) ||
                            trimmed.StartsWith("System.Collections.Generic.IReadOnlyList<", StringComparison.Ordinal))
                        {
                            return trimmed;
                        }

                        if (trimmed.EndsWith("[]", StringComparison.Ordinal))
                        {
                            return trimmed + "?";
                        }

                        return trimmed + "?";
                    }

                    string RenderRecordParameter(FieldDescriptor field, string propertyName, string typeLiteral, bool appendComma)
                    {
                        var builder = new System.Text.StringBuilder();

                        void AppendAttribute(string attribute)
                        {
                            if (string.IsNullOrWhiteSpace(attribute))
                            {
                                return;
                            }

                            builder.Append("    ");
                            builder.Append(attribute.Trim());
                            builder.AppendLine();
                        }

                        if (emitJsonIncludeNullValues && field.JsonIncludeNullValues == true)
                        {
                            AppendAttribute("[property: JsonIncludeNullValues]");
                        }

                        if (field.Attributes != null)
                        {
                            foreach (var attr in field.Attributes)
                            {
                                AppendAttribute(attr);
                            }
                        }

                        builder.Append("    ");
                        builder.Append(typeLiteral);
                        builder.Append(' ');
                        builder.Append(propertyName);
                        if (appendComma)
                        {
                            builder.Append(',');
                        }

                        return builder.ToString();
                    }

                    // JSON type correction: when ReturnsJson is active, recompute the field list using SQL-to-CLR mapping
                    IReadOnlyList<FieldDescriptor> effectiveFields = rs.Fields;
                    var deferredContainerNullability = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                    if (rs.JsonPayload != null && rs.Fields.Count > 0)
                    {
                        var remapped = new List<FieldDescriptor>(rs.Fields.Count);
                        foreach (var f in rs.Fields)
                        {
                            if (f.ReturnsUnknownJson == true)
                            {
                                remapped.Add(f);
                                continue;
                            }

                            var mapped = MapJsonSqlToClr(f.SqlTypeName, f.IsNullable);
                            if (!string.Equals(mapped, f.ClrType, StringComparison.Ordinal))
                            {
                                remapped.Add(new FieldDescriptor(
                                    f.Name,
                                    f.PropertyName,
                                    mapped,
                                    f.IsNullable,
                                    f.SqlTypeName,
                                    f.MaxLength,
                                    f.Documentation,
                                    f.Attributes,
                                    f.FunctionRef,
                                    f.DeferredJsonExpansion,
                                    f.ReturnsJson,
                                    f.ReturnsJsonArray,
                                    f.JsonRootProperty,
                                    f.ReturnsUnknownJson,
                                    JsonElementClrType: f.JsonElementClrType,
                                    JsonElementSqlType: f.JsonElementSqlType,
                                    JsonIncludeNullValues: f.JsonIncludeNullValues));
                                jsonTypeCorrections.Add($"{proc.OperationName}:{rs.Name}.{f.PropertyName} {f.ClrType}->{mapped}");
                            }
                            else
                            {
                                remapped.Add(f);
                            }
                        }
                        effectiveFields = remapped;
                    }

                    // JSON Function Enhancement: Improve JSON_QUERY function references when available
                    if (_jsonEnhancementService != null && effectiveFields.Count > 0)
                    {
                        try
                        {
                            var enhanced = _jsonEnhancementService.EnhanceResultSetColumns(effectiveFields, proc);
                            if (enhanced.Count != effectiveFields.Count ||
                                enhanced.Select((f, i) => new { Field = f, Index = i }).Any(x => !ReferenceEquals(x.Field, effectiveFields[x.Index])))
                            {
                                effectiveFields = enhanced;
                                jsonTypeCorrections.Add($"{proc.OperationName}:{rs.Name} - JSON function references enhanced");
                            }
                        }
                        catch (Exception ex)
                        {
                            // Log warning but continue with original fields
                            try { Console.Out.WriteLine($"[json-enhancement][warn] Failed to enhance procedure {proc.OperationName}: {ex.Message}"); } catch { }
                        }
                    }
                    // Generator phase: deferred JSON function expansion (RecordAsJson, etc.)
                    // Replace container columns with virtual dot-path fields such as record.<col>
                    try
                    {
                        var deferredContainers = effectiveFields.Where(f => f.DeferredJsonExpansion == true && !string.IsNullOrWhiteSpace(f.FunctionRef)).ToList();
                        if (deferredContainers.Count > 0)
                        {
                            var expanded = new List<FieldDescriptor>(effectiveFields);
                            bool changed = false;
                            foreach (var dc in deferredContainers)
                            {
                                var functionRef = SplitSchemaObject(dc.FunctionRef, proc.Schema ?? "dbo");
                                if (string.IsNullOrWhiteSpace(functionRef.Name))
                                {
                                    continue;
                                }

                                FunctionJsonDescriptor? jsonDescriptor = null;
                                if (_functionJsonResolver != null)
                                {
                                    try { jsonDescriptor = _functionJsonResolver(functionRef.Schema ?? proc.Schema ?? "dbo", functionRef.Name); } catch { jsonDescriptor = null; }
                                }

                                if (jsonDescriptor != null)
                                {
                                    deferredContainerNullability[dc.Name] = dc.IsNullable;
                                    expanded.Remove(dc);

                                    var containerNullable = deferredContainerNullability.TryGetValue(dc.Name, out var storedNullable) ? storedNullable : dc.IsNullable;
                                    var schemaSegment = string.IsNullOrWhiteSpace(jsonDescriptor.SchemaName) ? (functionRef.Schema ?? proc.Schema ?? "dbo") : jsonDescriptor.SchemaName;
                                    var schemaPascalLocal = ToPascalCase(schemaSegment);
                                    var functionNamespace = ns + "." + schemaPascalLocal;
                                    requiredFunctionTypeNamespaces.Add(functionNamespace);

                                    var baseTypeName = jsonDescriptor.RootTypeName;
                                    string clrType = jsonDescriptor.ReturnsJsonArray
                                        ? $"System.Collections.Generic.List<{baseTypeName}>"
                                        : baseTypeName;

                                    expanded.Add(new FieldDescriptor(
                                        Name: dc.Name,
                                        PropertyName: AliasToIdentifier(dc.Name),
                                        ClrType: clrType,
                                        IsNullable: containerNullable,
                                        SqlTypeName: dc.SqlTypeName,
                                        MaxLength: dc.MaxLength,
                                        Documentation: dc.Documentation,
                                        Attributes: dc.Attributes,
                                        FunctionRef: null,
                                        DeferredJsonExpansion: null,
                                        ReturnsJson: false,
                                        ReturnsJsonArray: jsonDescriptor.ReturnsJsonArray,
                                        JsonRootProperty: dc.JsonRootProperty,
                                        ReturnsUnknownJson: null,
                                        JsonElementClrType: null,
                                        JsonElementSqlType: null,
                                        JsonIncludeNullValues: jsonDescriptor.IncludeNullValues ? true : dc.JsonIncludeNullValues
                                    ));
                                    changed = true;
                                    continue;
                                }

                                if (StoredProcedureContentModel.ResolveFunctionJsonSet == null)
                                {
                                    continue;
                                }

                                (bool ReturnsJson, bool ReturnsJsonArray, string RootProperty, IReadOnlyList<string> ColumnNames) meta;
                                try { meta = StoredProcedureContentModel.ResolveFunctionJsonSet(functionRef.Schema ?? proc.Schema ?? "dbo", functionRef.Name); } catch { continue; }
                                if (!meta.ReturnsJson || meta.ColumnNames == null || meta.ColumnNames.Count == 0) continue;
                                // Remove the container column
                                deferredContainerNullability[dc.Name] = dc.IsNullable;
                                expanded.Remove(dc);
                                foreach (var colName in meta.ColumnNames)
                                {
                                    if (string.IsNullOrWhiteSpace(colName)) continue;
                                    var pathName = dc.Name + "." + colName.Trim();
                                    // Emit leaf nodes as string; later enrichers can tighten the types if needed
                                    expanded.Add(new FieldDescriptor(pathName, pathName, "string", true, "nvarchar", null, null, null));
                                }
                                changed = true;
                            }
                            if (changed)
                            {
                                // Stabilize ordering for deterministic output
                                effectiveFields = expanded.OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase).ToList();
                            }
                        }
                    }
                    catch { }
                    // Align record type naming with existing tests:
                    var jsonInfo = rs.JsonPayload;
                    var isJson = jsonInfo != null;
                    var isJsonArray = jsonInfo?.IsArray ?? false;
                    var rootProp = string.IsNullOrWhiteSpace(jsonInfo?.RootProperty) ? null : jsonInfo!.RootProperty;
                    var rsName = string.IsNullOrWhiteSpace(rootProp) ? rs.Name : rootProp;
                    // always use NamePolicy.ResultSet(procPart, rsName).
                    // The unified aggregate stays NamePolicy.Result(procPart) so it no longer collides with the first set.
                    string rsType = NamePolicy.ResultSet(procPart, rsName);
                    // Suffix correction is no longer required in the new schema
                    // Resolve alias-based property names first (required for JSON fallback handling)
                    var usedNames = new HashSet<string>(StringComparer.Ordinal);
                    var aliasProps = new List<string>();
                    foreach (var f in effectiveFields)
                    {
                        var candidate = AliasToIdentifier(f.Name);
                        if (!usedNames.Add(candidate))
                        {
                            var suffix = 1;
                            while (!usedNames.Add(candidate + suffix.ToString())) suffix++;
                            candidate = candidate + suffix.ToString();
                        }
                        aliasProps.Add(candidate);
                    }
                    var ordinalAssignments = effectiveFields.Select((f, idx) => $"int o{idx}=ReaderUtil.TryGetOrdinal(r, \"{f.Name}\");").ToList();
                    // Optional first-row dump tooling was removed (XTRAQ_DUMP_FIRST_ROW); code stays clean here.
                    // JSON aggregator fallback (FOR JSON PATH) triggers when all ordinals are < 0 yet metadata exists.
                    var allMissingCondition = rs.Fields.Count > 0 ? string.Join(" && ", rs.Fields.Select((f, i) => $"o{i} < 0")) : "false"; // applies only to non-JSON sets
                    // Use ReturnsJson flags from the snapshot (result set model exposes the properties)
                    // When ReturnsJson is true we deserialize a single NVARCHAR column produced by FOR JSON.
                    string ordinalDeclInline = string.Join(" ", ordinalAssignments); // classic mapping with cached ordinals (debug dump removed)
                    string ordinalDeclBlock = ordinalAssignments.Count == 0
                        ? string.Empty
                        : string.Join("\n", ordinalAssignments.Select(line => "            " + line));
                    string streamOrdinalDeclBlock = ConvertReaderVariable(ordinalDeclBlock);
                    if (isJson)
                    {
                        ordinalDeclInline = string.Empty;
                        ordinalDeclBlock = string.Empty;
                        streamOrdinalDeclBlock = string.Empty;
                    }
                    var fieldExprs = string.Join(", ", effectiveFields.Select((f, idx) => MaterializeFieldExpressionCached(f, idx)));
                    var streamFieldExprs = isJson ? string.Empty : ConvertReaderVariable(fieldExprs);
                    // Preserve property naming pattern Result, Result1, Result2 ... so existing tests stay unchanged
                    string propName;
                    if (isJson && !string.IsNullOrWhiteSpace(rootProp))
                    {
                        propName = NamePolicy.Sanitize(rootProp!);
                        if (string.IsNullOrWhiteSpace(propName))
                        {
                            propName = rsIdx == 0 ? "Result" : "Result" + rsIdx.ToString();
                        }
                    }
                    else
                    {
                        propName = rsIdx == 0 ? "Result" : "Result" + rsIdx.ToString();
                    }
                    var initializerExpr = $"rs.Length > {rsIdx} && rs[{rsIdx}] is object[] rows{rsIdx} ? Array.ConvertAll(rows{rsIdx}, o => ({rsType})o).ToList() : (rs.Length > {rsIdx} && rs[{rsIdx}] is System.Collections.Generic.List<object> list{rsIdx} ? Array.ConvertAll(list{rsIdx}.ToArray(), o => ({rsType})o).ToList() : Array.Empty<{rsType}>())";
                    string propType;
                    string propDefault;
                    string aggregateAssignment = initializerExpr;
                    var hasRaw = false;
                    string rawPropName = string.Empty;
                    const string rawPropDefault = "null";
                    string rawAggregateAssignment = "null";
                    if (isJson)
                    {
                        hasRaw = true;
                        rawPropName = propName + "RawJson";
                        var rawRowsVar = $"rows{rsIdx}_raw";
                        var rawEnvVar = $"env{rsIdx}_raw";
                        rawAggregateAssignment = $"rs.Length > {rsIdx} && rs[{rsIdx}] is object[] {rawRowsVar} && {rawRowsVar}.Length > 0 && {rawRowsVar}[0] is JsonResultEnvelope<{rsType}> {rawEnvVar} ? {rawEnvVar}.RawJson : null";

                        if (isJsonArray)
                        {
                            var rowsVar = $"rows{rsIdx}";
                            var envVar = $"env{rsIdx}";
                            aggregateAssignment = $"rs.Length > {rsIdx} && rs[{rsIdx}] is object[] {rowsVar} && {rowsVar}.Length > 0 && {rowsVar}[0] is JsonResultEnvelope<{rsType}> {envVar} ? {envVar}.Items : System.Array.Empty<{rsType}>()";
                            propType = $"IReadOnlyList<{rsType}>";
                            propDefault = $"Array.Empty<{rsType}>()";
                        }
                        else
                        {
                            var rowsVar = $"rows{rsIdx}";
                            var envVar = $"env{rsIdx}";
                            var valueVar = $"value{rsIdx}";
                            aggregateAssignment = $"rs.Length > {rsIdx} && rs[{rsIdx}] is object[] {rowsVar} && {rowsVar}.Length > 0 && {rowsVar}[0] is JsonResultEnvelope<{rsType}> {envVar} && {envVar}.TryGetFirst(out var {valueVar}) ? {valueVar} : ({rsType}?)null";
                            propType = rsType + "?";
                            propDefault = "null";
                        }
                    }
                    else
                    {
                        propType = $"IReadOnlyList<{rsType}>";
                        propDefault = $"Array.Empty<{rsType}>()";
                    }
                    var supportsStreaming = !isJson;
                    string? streamSuffix = null;
                    string? streamMethodName = null;
                    if (supportsStreaming)
                    {
                        var baseSuffix = NamePolicy.Sanitize(rs.Name).TrimStart('@');
                        if (string.IsNullOrWhiteSpace(baseSuffix))
                        {
                            baseSuffix = $"Set{rsIdx + 1}";
                        }

                        if (streamSuffixTracker.TryGetValue(baseSuffix, out var existingCount))
                        {
                            existingCount++;
                            streamSuffixTracker[baseSuffix] = existingCount;
                            baseSuffix += existingCount.ToString(CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            streamSuffixTracker[baseSuffix] = 0;
                        }

                        streamSuffix = baseSuffix;
                        streamMethodName = $"StreamResult{streamSuffix}Async";
                    }
                    // BodyBlock replaces the old template-if usage and contains the full lambda contents.
                    string bodyBlock;
                    if (isJson)
                    {
                        var sbInit = "var __sb = new System.Text.StringBuilder();\nwhile (await r.ReadAsync(ct).ConfigureAwait(false))\n{\n    if (!r.IsDBNull(0))\n    {\n        __sb.Append(r.GetString(0));\n    }\n}\nvar __raw = __sb.Length > 0 ? __sb.ToString() : null;";
                        var rootUnwrap = string.IsNullOrWhiteSpace(rootProp)
                            ? string.Empty
                            : $"if (__raw != null)\n{{\n    try\n    {{\n        using var __doc = System.Text.Json.JsonDocument.Parse(__raw);\n        if (__doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object && __doc.RootElement.TryGetProperty(\"{rootProp}\", out var __root))\n        {{\n            __raw = __root.GetRawText();\n        }}\n    }}\n    catch {{ }}\n}}\n";
                        if (isJsonArray)
                        {
                            bodyBlock = $"var list = new System.Collections.Generic.List<object>();\n{sbInit}\n{rootUnwrap}var __items = new System.Collections.Generic.List<{rsType}>();\nif (__raw != null)\n{{\n    try\n    {{\n        var __parsed = System.Text.Json.JsonSerializer.Deserialize<System.Collections.Generic.List<{rsType}?>>(__raw, JsonSupport.Options);\n        if (__parsed is not null)\n        {{\n            foreach (var __entry in __parsed)\n            {{\n                if (__entry is {{ }} __value)\n                {{\n                    __items.Add(__value);\n                }}\n            }}\n        }}\n    }}\n    catch\n    {{\n    }}\n}}\nlist.Add(JsonResultEnvelope<{rsType}>.Create(__items, __raw));\nreturn list;";
                        }
                        else
                        {
                            bodyBlock = $"var list = new System.Collections.Generic.List<object>();\n{sbInit}\n{rootUnwrap}var __items = new System.Collections.Generic.List<{rsType}>();\nif (__raw != null)\n{{\n    try\n    {{\n        var __parsed = System.Text.Json.JsonSerializer.Deserialize<{rsType}?>(__raw, JsonSupport.Options);\n        if (__parsed is {{ }} __value)\n        {{\n            __items.Add(__value);\n        }}\n    }}\n    catch\n    {{\n    }}\n}}\nlist.Add(JsonResultEnvelope<{rsType}>.Create(__items, __raw));\nreturn list;";
                        }
                    }
                    else
                    {
                        var whileLoop = $"while (await r.ReadAsync(ct).ConfigureAwait(false)) {{ list.Add(new {rsType}({fieldExprs})); }}";
                        bodyBlock = $"var list = new System.Collections.Generic.List<object>(); {ordinalDeclInline} {whileLoop} return list;";
                    }
                    // Nested JSON sub-struct generation (JSON sets): only '.' splits hierarchy - underscores remain literal
                    string nestedRecordsBlock = string.Empty;
                    if (isJson && effectiveFields.Any(f => f.Name.Contains('.')))
                    {
                        // Build a hierarchical tree for nested JSON fields and capture container nullability
                        var rootLeafFields = new List<FieldDescriptor>();
                        var groupOrder = new List<string>();
                        var groups = new Dictionary<string, List<FieldDescriptor>>(StringComparer.OrdinalIgnoreCase);
                        var groupNullability = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

                        foreach (var kv in deferredContainerNullability)
                        {
                            groupNullability[kv.Key] = kv.Value;
                        }

                        void TrackGroupNullability(FieldDescriptor field)
                        {
                            var parts = field.Name.Split('.', StringSplitOptions.RemoveEmptyEntries);
                            if (parts.Length <= 1)
                            {
                                return;
                            }

                            string path = string.Empty;
                            for (int i = 0; i < parts.Length - 1; i++)
                            {
                                path = string.IsNullOrEmpty(path) ? parts[i] : string.Concat(path, ".", parts[i]);
                                if (!groupNullability.TryGetValue(path, out var existing))
                                {
                                    groupNullability[path] = field.IsNullable;
                                }
                                else if (field.IsNullable && !existing)
                                {
                                    groupNullability[path] = true;
                                }
                            }
                        }

                        foreach (var f in effectiveFields)
                        {
                            if (!f.Name.Contains('.'))
                            {
                                // No '.' means it remains a leaf (underscores are not parsed as hierarchy)
                                rootLeafFields.Add(f);
                                continue;
                            }

                            TrackGroupNullability(f);

                            var parts = f.Name.Split('.', StringSplitOptions.RemoveEmptyEntries);
                            if (parts.Length <= 1)
                            {
                                rootLeafFields.Add(f);
                                continue;
                            }
                            var key = parts[0];
                            if (!groups.ContainsKey(key))
                            {
                                groups[key] = new List<FieldDescriptor>();
                                groupOrder.Add(key);
                            }

                            groups[key].Add(f);
                        }

                        string Pascal(string raw)
                        {
                            if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
                            var segs = raw.Split(new[] { '-', '_' }, StringSplitOptions.RemoveEmptyEntries);
                            var b = new System.Text.StringBuilder();
                            foreach (var seg in segs)
                            {
                                var clean = new string(seg.Where(char.IsLetterOrDigit).ToArray());
                                if (clean.Length == 0) continue;
                                b.Append(char.ToUpperInvariant(clean[0]) + (clean.Length > 1 ? clean.Substring(1) : string.Empty));
                            }
                            var res = b.ToString();
                            if (res.Length == 0) res = "Segment";
                            if (char.IsDigit(res[0])) res = "N" + res;
                            return res;
                        }

                        string BuildNestedTypeName(string root, string segment) => (root.EndsWith("Result", StringComparison.Ordinal) ? root[..^"Result".Length] : root) + Pascal(segment) + "Result";

                        bool IsArrayGroupPath(string? path)
                        {
                            if (string.IsNullOrWhiteSpace(path) || rs.JsonStructure == null || rs.JsonStructure.Count == 0)
                            {
                                return false;
                            }

                            return TryResolveNode(rs.JsonStructure, path!)?.IsArray == true;
                        }

                        static JsonFieldNode? TryResolveNode(IReadOnlyList<JsonFieldNode> nodes, string path)
                        {
                            foreach (var node in nodes)
                            {
                                if (string.Equals(node.Path, path, StringComparison.OrdinalIgnoreCase))
                                {
                                    return node;
                                }

                                if (node.Children.Count > 0 && path.StartsWith(node.Path + ".", StringComparison.OrdinalIgnoreCase))
                                {
                                    var nested = TryResolveNode(node.Children, path);
                                    if (nested != null)
                                    {
                                        return nested;
                                    }
                                }
                            }

                            return null;
                        }

                        string CombinePath(string? prefix, string segment)
                            => string.IsNullOrWhiteSpace(prefix) ? segment : (string.IsNullOrWhiteSpace(segment) ? prefix! : prefix + "." + segment);

                        // Recursively build nested types for groups (supports deeper levels e.g. sourceAccount_type_code)
                        var builtTypes = new List<(string TypeName, string Code)>();

                        List<(string TypeName, string Code)> BuildGroup(string rootTypeName, string groupName, List<FieldDescriptor> fields, string? parentPath)
                        {
                            var groupPath = CombinePath(parentPath, groupName);
                            // Partition fields into direct leaves (parts length ==2) and deeper
                            var leaves = new List<FieldDescriptor>();
                            var subGroups = new Dictionary<string, List<FieldDescriptor>>(StringComparer.OrdinalIgnoreCase);
                            foreach (var f in fields)
                            {
                                if (!f.Name.Contains('.'))
                                {
                                    leaves.Add(new FieldDescriptor(f.Name, AliasToIdentifier(f.Name), f.ClrType, f.IsNullable, f.SqlTypeName, f.MaxLength, f.Documentation, f.Attributes, f.FunctionRef, f.DeferredJsonExpansion, f.ReturnsJson, f.ReturnsJsonArray, f.JsonRootProperty, f.ReturnsUnknownJson, JsonElementClrType: f.JsonElementClrType, JsonElementSqlType: f.JsonElementSqlType, JsonIncludeNullValues: f.JsonIncludeNullValues));
                                    continue;
                                }
                                var parts = f.Name.Split('.', StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length == 2)
                                {
                                    var leafName = parts[1];
                                    leaves.Add(new FieldDescriptor(leafName, AliasToIdentifier(leafName), f.ClrType, f.IsNullable, f.SqlTypeName, f.MaxLength, f.Documentation, f.Attributes, f.FunctionRef, f.DeferredJsonExpansion, f.ReturnsJson, f.ReturnsJsonArray, f.JsonRootProperty, f.ReturnsUnknownJson, JsonElementClrType: f.JsonElementClrType, JsonElementSqlType: f.JsonElementSqlType, JsonIncludeNullValues: f.JsonIncludeNullValues));
                                }
                                else if (parts.Length > 2)
                                {
                                    var sub = parts[1];
                                    var remainder = string.Join('.', parts.Skip(1));
                                    var remainderProperty = AliasToIdentifier(remainder.Replace('.', '_'));
                                    var f2 = new FieldDescriptor(remainder, remainderProperty, f.ClrType, f.IsNullable, f.SqlTypeName, f.MaxLength, f.Documentation, f.Attributes, f.FunctionRef, f.DeferredJsonExpansion, f.ReturnsJson, f.ReturnsJsonArray, f.JsonRootProperty, f.ReturnsUnknownJson, JsonElementClrType: f.JsonElementClrType, JsonElementSqlType: f.JsonElementSqlType, JsonIncludeNullValues: f.JsonIncludeNullValues);
                                    if (!subGroups.ContainsKey(sub)) subGroups[sub] = new List<FieldDescriptor>();
                                    subGroups[sub].Add(f2);
                                }
                            }
                            var typeName = BuildNestedTypeName(rootTypeName, groupName);
                            var paramLines = new List<string>();
                            for (int i = 0; i < leaves.Count; i++)
                            {
                                var lf = leaves[i];
                                var needsComma = !(i == leaves.Count - 1 && subGroups.Count == 0);
                                var typeLiteral = ApplyNullability(lf.ClrType, lf.IsNullable);
                                paramLines.Add(RenderRecordParameter(lf, lf.PropertyName, typeLiteral, needsComma));
                            }
                            int sgIndex = 0;
                            foreach (var sg in subGroups)
                            {
                                var nestedList = BuildGroup(typeName, sg.Key, sg.Value, groupPath); // recursion
                                builtTypes.AddRange(nestedList);
                                var nestedTypeName = BuildNestedTypeName(typeName, sg.Key);
                                var nestedPath = CombinePath(groupPath, sg.Key);
                                bool nestedIsArray = IsArrayGroupPath(nestedPath);
                                var nestedPropertyType = nestedIsArray
                                    ? $"System.Collections.Generic.List<{nestedTypeName}>"
                                    : ApplyNullability(nestedTypeName, groupNullability.TryGetValue(nestedPath, out var nestedNullable) && nestedNullable);
                                var nestedPropName = AliasToIdentifier(sg.Key);
                                var line = $"    {nestedPropertyType} {nestedPropName}{(sgIndex == subGroups.Count - 1 ? string.Empty : ",")}";
                                paramLines.Add(line);
                                sgIndex++;
                            }
                            var code = $"public readonly record struct {typeName}(\n" + string.Join("\n", paramLines) + "\n);\n";
                            return new List<(string TypeName, string Code)> { (typeName, code) };
                        }

                        foreach (var g in groupOrder)
                        {
                            var nestedList = BuildGroup(rsType, g, groups[g], null);
                            builtTypes.AddRange(nestedList);
                        }
                        nestedRecordsBlock = string.Join("\n", builtTypes.Select(t => t.Code));

                        // Rebuild root fieldsBlock: keep original aliases (no PascalCase conversion; underscores stay intact)
                        var rootParams = new List<string>();
                        for (int i = 0; i < rootLeafFields.Count; i++)
                        {
                            var lf = rootLeafFields[i];
                            var aliasName = lf.Name;
                            if (aliasName.Contains('.')) aliasName = aliasName.Split('.', StringSplitOptions.RemoveEmptyEntries).Last();
                            aliasName = AliasToIdentifier(aliasName);
                            var typeLiteral = ApplyNullability(lf.ClrType, lf.IsNullable);
                            var needsComma = !(i == rootLeafFields.Count - 1 && groupOrder.Count == 0);
                            rootParams.Add(RenderRecordParameter(lf, aliasName, typeLiteral, needsComma));
                        }
                        // Group properties (escaped via AliasToIdentifier to protect reserved keywords like 'params')
                        for (int i = 0; i < groupOrder.Count; i++)
                        {
                            var g = groupOrder[i];
                            var gEsc = AliasToIdentifier(g);
                            var nestedTypeName = BuildNestedTypeName(rsType, g);
                            var gPath = CombinePath(null, g);
                            var comma = i == groupOrder.Count - 1 ? string.Empty : ",";
                            string groupPropertyType;
                            if (IsArrayGroupPath(gPath))
                            {
                                groupPropertyType = $"System.Collections.Generic.List<{nestedTypeName}>";
                            }
                            else
                            {
                                var groupNullable = groupNullability.TryGetValue(gPath, out var gNull) && gNull;
                                groupPropertyType = ApplyNullability(nestedTypeName, groupNullable);
                            }
                            rootParams.Add($"    {groupPropertyType} {gEsc}{comma}");
                        }
                        var rootFieldsBlock = string.Join(Environment.NewLine, rootParams);
                        rsMeta.Add(new
                        {
                            Name = rsName,
                            TypeName = rsType,
                            PropName = propName,
                            PropType = propType,
                            PropDefault = propDefault,
                            HasRaw = hasRaw,
                            RawPropName = rawPropName,
                            RawPropDefault = rawPropDefault,
                            RawAggregateAssignment = rawAggregateAssignment,
                            OrdinalDecls = ordinalDeclInline,
                            FieldExprs = fieldExprs,
                            Index = rsIdx,
                            AggregateAssignment = aggregateAssignment,
                            FieldsBlock = rootFieldsBlock,
                            ReturnsJson = isJson,
                            ReturnsJsonArray = isJsonArray,
                            BodyBlock = IndentBlock(bodyBlock, "                "),
                            NestedRecordsBlock = nestedRecordsBlock
                        });
                        rsIdx++;
                        return; // skip flat record path
                    }
                    // New: nested record generation now applies to non-JSON sets with dot aliases as well
                    // Previously only JSON sets emitted a nestedRecordsBlock; this generalizes the approach.
                    if (!isJson && effectiveFields.Any(f => f.Name.Contains('.')))
                    {
                        // Build a tree based on '.' segments (underscores remain literal)
                        var rootLeafFields = new List<FieldDescriptor>();
                        var groupOrder = new List<string>();
                        var groups = new Dictionary<string, List<FieldDescriptor>>(StringComparer.OrdinalIgnoreCase);
                        foreach (var f in effectiveFields)
                        {
                            if (!f.Name.Contains('.')) { rootLeafFields.Add(f); continue; }
                            var parts = f.Name.Split('.', StringSplitOptions.RemoveEmptyEntries);
                            var key = parts[0];
                            if (!groups.ContainsKey(key)) { groups[key] = new List<FieldDescriptor>(); groupOrder.Add(key); }
                            // Rebuild the remainder (without the first segment) as a dotted name for later resolution
                            var remainder = string.Join('.', parts.Skip(1));
                            groups[key].Add(new FieldDescriptor(remainder, remainder, f.ClrType, f.IsNullable, f.SqlTypeName, f.MaxLength, f.Documentation, f.Attributes, f.FunctionRef, f.DeferredJsonExpansion, f.ReturnsJson, f.ReturnsJsonArray, f.JsonRootProperty, f.ReturnsUnknownJson, JsonElementClrType: f.JsonElementClrType, JsonElementSqlType: f.JsonElementSqlType, JsonIncludeNullValues: f.JsonIncludeNullValues));
                        }
                        string Pascal(string raw)
                        {
                            if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
                            var segs = raw.Split(new[] { '-', '_' }, StringSplitOptions.RemoveEmptyEntries);
                            var b = new System.Text.StringBuilder();
                            foreach (var seg in segs)
                            {
                                var clean = new string(seg.Where(char.IsLetterOrDigit).ToArray());
                                if (clean.Length == 0) continue;
                                b.Append(char.ToUpperInvariant(clean[0]) + (clean.Length > 1 ? clean.Substring(1) : string.Empty));
                            }
                            var res = b.ToString();
                            if (res.Length == 0) res = "Segment";
                            if (char.IsDigit(res[0])) res = "N" + res;
                            return res;
                        }
                        string BuildNestedTypeName(string root, string segment) => (root.EndsWith("Result", StringComparison.Ordinal) ? root[..^"Result".Length] : root) + Pascal(segment) + "Result";

                        var builtTypes = new List<(string TypeName, string Code)>();
                        var exprLookup = effectiveFields.Select((f, idx) => (f, idx)).ToDictionary(t => t.f.Name, t => MaterializeFieldExpressionCached(t.f, t.idx), StringComparer.OrdinalIgnoreCase);

                        List<(string TypeName, string Code)> BuildGroup(string rootTypeName, string groupName, List<FieldDescriptor> fields, out string ctorExpr)
                        {
                            var leaves = new List<FieldDescriptor>();
                            var subGroups = new Dictionary<string, List<FieldDescriptor>>(StringComparer.OrdinalIgnoreCase);
                            foreach (var f in fields)
                            {
                                if (!f.Name.Contains('.')) { leaves.Add(f); continue; }
                                var parts = f.Name.Split('.', StringSplitOptions.RemoveEmptyEntries);
                                var sub = parts[0];
                                var remainder = string.Join('.', parts.Skip(1));
                                if (!subGroups.ContainsKey(sub)) subGroups[sub] = new List<FieldDescriptor>();
                                subGroups[sub].Add(new FieldDescriptor(remainder, remainder, f.ClrType, f.IsNullable, f.SqlTypeName, f.MaxLength, f.Documentation, f.Attributes, f.FunctionRef, f.DeferredJsonExpansion, f.ReturnsJson, f.ReturnsJsonArray, f.JsonRootProperty, f.ReturnsUnknownJson, JsonElementClrType: f.JsonElementClrType, JsonElementSqlType: f.JsonElementSqlType, JsonIncludeNullValues: f.JsonIncludeNullValues));
                            }
                            var typeNameNested = BuildNestedTypeName(rootTypeName, groupName);
                            var paramLines = new List<string>();
                            // Parameter list for this nested type
                            for (int i = 0; i < leaves.Count; i++)
                            {
                                var lf = leaves[i];
                                var appendComma = !(i == leaves.Count - 1 && subGroups.Count == 0);
                                // Use the last segment of the leaf name as the property identifier
                                var pName = lf.Name.Split('.', StringSplitOptions.RemoveEmptyEntries).Last();
                                pName = AliasToIdentifier(pName);
                                var typeLiteral = ApplyNullability(lf.ClrType, lf.IsNullable);
                                paramLines.Add(RenderRecordParameter(lf, pName, typeLiteral, appendComma));
                            }
                            // Recurse into subgroups
                            var subgroupCtorExprs = new List<string>();
                            int sgIdx = 0;
                            foreach (var sg in subGroups)
                            {
                                var nestedList = BuildGroup(typeNameNested, sg.Key, sg.Value, out var subCtor);
                                builtTypes.AddRange(nestedList);
                                var nestedTypeName = BuildNestedTypeName(typeNameNested, sg.Key);
                                var comma = sgIdx == subGroups.Count - 1 ? string.Empty : ",";
                                paramLines.Add($"    {nestedTypeName} {AliasToIdentifier(sg.Key)}{comma}");
                                subgroupCtorExprs.Add(subCtor);
                                sgIdx++;
                            }
                            var code = $"public readonly record struct {typeNameNested}(\n" + string.Join("\n", paramLines) + "\n);\n";
                            // Constructor expression for this group node: new Type(a, b, new Child(...))
                            var leafExprs = leaves.Select(l => exprLookup.TryGetValue(ComposeFullName(groupName, l.Name), out var ex) ? ex : "default");
                            var totalArgs = leafExprs.Concat(subgroupCtorExprs);
                            ctorExpr = $"new {typeNameNested}(" + string.Join(", ", totalArgs) + ")";
                            return new List<(string TypeName, string Code)> { (typeNameNested, code) };
                        }
                        string ComposeFullName(string root, string remainder) => string.IsNullOrEmpty(remainder) ? root : (remainder.Contains('.') ? root + "." + remainder : root + "." + remainder);

                        var topGroupCtorExprs = new List<string>();
                        foreach (var g in groupOrder)
                        {
                            var nestedList = BuildGroup(rsType, g, groups[g], out var gCtor);
                            builtTypes.AddRange(nestedList);
                            topGroupCtorExprs.Add(gCtor);
                        }
                        // Root fields block: root-level columns followed by group properties
                        var rootParams = new List<string>();
                        for (int i = 0; i < rootLeafFields.Count; i++)
                        {
                            var lf = rootLeafFields[i];
                            var paramName = lf.Name;
                            if (paramName.Contains('.'))
                            {
                                paramName = paramName.Split('.', StringSplitOptions.RemoveEmptyEntries).Last();
                            }
                            paramName = AliasToIdentifier(paramName);
                            var typeLiteral = ApplyNullability(lf.ClrType, lf.IsNullable);
                            var appendComma = !(i == rootLeafFields.Count - 1 && groupOrder.Count == 0);
                            rootParams.Add(RenderRecordParameter(lf, paramName, typeLiteral, appendComma));
                        }
                        for (int i = 0; i < groupOrder.Count; i++)
                        {
                            var g = groupOrder[i];
                            var gEsc = AliasToIdentifier(g);
                            var nestedTypeName = BuildNestedTypeName(rsType, g);
                            var comma = i == groupOrder.Count - 1 ? string.Empty : ",";
                            rootParams.Add($"    {nestedTypeName} {gEsc}{comma}");
                        }
                        var rootFieldsBlock = string.Join(Environment.NewLine, rootParams);
                        // Mapping arguments: root leaves followed by group constructor expressions (matching parameter order)
                        var rootLeafExprs = rootLeafFields.Select(f => exprLookup.TryGetValue(f.Name, out var ex) ? ex : "default");
                        var constructorArgs = string.Join(", ", rootLeafExprs.Concat(topGroupCtorExprs));
                        // Adjust BodyBlock for the streaming variant to instantiate new rsType(constructorArgs)
                        if (!isJson)
                        {
                            var ordinalDeclNested = ordinalDeclInline;
                            var whileLoopNested = $"while (await r.ReadAsync(ct).ConfigureAwait(false)) {{ list.Add(new {rsType}({constructorArgs})); }}";
                            bodyBlock = $"var list = new System.Collections.Generic.List<object>(); {ordinalDeclNested} {whileLoopNested} return list;";
                        }
                        nestedRecordsBlock = string.Join("\n", builtTypes.Select(t => t.Code));
                        streamFieldExprs = ConvertReaderVariable(constructorArgs);
                        var hasFieldExprs = !string.IsNullOrWhiteSpace(streamFieldExprs);
                        var hasOrdinalDecls = ordinalAssignments.Count > 0;
                        rsMeta.Add(new
                        {
                            Name = rsName,
                            TypeName = rsType,
                            PropName = propName,
                            PropType = propType,
                            PropDefault = propDefault,
                            HasRaw = hasRaw,
                            RawPropName = rawPropName,
                            RawPropDefault = rawPropDefault,
                            RawAggregateAssignment = rawAggregateAssignment,
                            OrdinalDecls = ordinalDeclInline,
                            FieldExprs = constructorArgs,
                            Index = rsIdx,
                            AggregateAssignment = aggregateAssignment,
                            FieldsBlock = rootFieldsBlock,
                            ReturnsJson = isJson,
                            ReturnsJsonArray = isJsonArray,
                            BodyBlock = IndentBlock(bodyBlock, "                "),
                            NestedRecordsBlock = nestedRecordsBlock,
                            SupportsStreaming = supportsStreaming,
                            StreamSuffix = streamSuffix,
                            StreamMethodName = streamMethodName,
                            StreamIndex = rsIdx,
                            StreamOrdinalDecls = streamOrdinalDeclBlock,
                            HasStreamOrdinals = hasOrdinalDecls,
                            StreamFieldExprs = streamFieldExprs,
                            HasFieldExpressions = hasFieldExprs,
                            HasInput = proc.InputParameters.Count > 0,
                            HasOutput = proc.OutputFields.Count > 0,
                            InputTypeName = inputTypeName,
                            OutputTypeName = outputTypeName,
                            ProcedureTypeName = procedureTypeName,
                            PlanTypeName = procedureTypeName + "Plan"
                        });
                        rsIdx++;
                    }
                    else
                    {
                        var hasFieldExprs = !string.IsNullOrWhiteSpace(streamFieldExprs);
                        var hasOrdinalDecls = ordinalAssignments.Count > 0;
                        var fieldsBlock = string.Join(Environment.NewLine, effectiveFields.Select((f, i) =>
                        {
                            var typeLiteral = ApplyNullability(f.ClrType, f.IsNullable);
                            var appendComma = i != effectiveFields.Count - 1;
                            return RenderRecordParameter(f, aliasProps[i], typeLiteral, appendComma);
                        }));
                        rsMeta.Add(new
                        {
                            Name = rsName,
                            TypeName = rsType,
                            PropName = propName,
                            PropType = propType,
                            PropDefault = propDefault,
                            HasRaw = hasRaw,
                            RawPropName = rawPropName,
                            RawPropDefault = rawPropDefault,
                            RawAggregateAssignment = rawAggregateAssignment,
                            OrdinalDecls = ordinalDeclInline,
                            FieldExprs = fieldExprs,
                            Index = rsIdx,
                            AggregateAssignment = aggregateAssignment,
                            FieldsBlock = fieldsBlock,
                            ReturnsJson = isJson,
                            ReturnsJsonArray = isJsonArray,
                            BodyBlock = IndentBlock(bodyBlock, "                "),
                            NestedRecordsBlock = nestedRecordsBlock,
                            SupportsStreaming = supportsStreaming,
                            StreamSuffix = streamSuffix,
                            StreamMethodName = streamMethodName,
                            StreamIndex = rsIdx,
                            StreamOrdinalDecls = streamOrdinalDeclBlock,
                            HasStreamOrdinals = hasOrdinalDecls,
                            StreamFieldExprs = streamFieldExprs,
                            HasFieldExpressions = hasFieldExprs,
                            HasInput = proc.InputParameters.Count > 0,
                            HasOutput = proc.OutputFields.Count > 0,
                            InputTypeName = inputTypeName,
                            OutputTypeName = outputTypeName,
                            ProcedureTypeName = procedureTypeName,
                            PlanTypeName = procedureTypeName + "Plan"
                        });
                        rsIdx++;
                    }
                }

                foreach (var funcNamespace in requiredFunctionTypeNamespaces)
                {
                    if (!string.IsNullOrWhiteSpace(funcNamespace))
                    {
                        usingSet.Add($"using global::{funcNamespace};");
                    }
                }

                var usingBlock = string.Join("\n", usingSet.OrderBy(u => u));

                // Parameters meta
                var paramLines = new List<string>();
                foreach (var ip in proc.InputParameters)
                {
                    var isTableType = ip.Attributes != null && ip.Attributes.Any(a => a.StartsWith("[TableType]", StringComparison.Ordinal));
                    if (isTableType)
                    {
                        // Use Object DbType placeholder; binder will override with SqlDbType.Structured
                        var typeNameLiteral = (ip.SqlTypeName ?? ip.Name).Replace("\"", "\\\"", StringComparison.Ordinal);
                        paramLines.Add($"new(\"@{ip.Name}\", System.Data.DbType.Object, null, false, false, \"{typeNameLiteral}\")");
                    }
                    else
                    {
                        paramLines.Add($"new(\"@{ip.Name}\", {MapDbType(ip.SqlTypeName)}, {EmitSize(ip)}, false, {ip.IsNullable.ToString().ToLowerInvariant()})");
                    }
                }
                foreach (var opf in proc.OutputFields)
                    paramLines.Add($"new(\"@{opf.Name}\", {MapDbType(opf.SqlTypeName)}, {EmitSize(opf)}, true, {opf.IsNullable.ToString().ToLowerInvariant()})");

                // Output factory args
                string outputFactoryArgs = proc.OutputFields.Count > 0 ? string.Join(", ", proc.OutputFields.Select(f => CastOutputValue(f))) : string.Empty;

                // Aggregate assignments
                var model = new
                {
                    Namespace = finalNs,
                    UsingDirectives = usingBlock,
                    HEADER = headerBlock,
                    HasParameters = proc.InputParameters.Count + proc.OutputFields.Count > 0,
                    HasInput = proc.InputParameters.Count > 0,
                    HasOutput = proc.OutputFields.Count > 0,
                    // Use rsMeta.Count (includes virtual expansion) instead of the original proc.ResultSets
                    HasResultSets = rsMeta.Count > 0,
                    HasMultipleResultSets = rsMeta.Count > 1,
                    InputParameters = proc.InputParameters.Select((p, i) => new { p.ClrType, p.PropertyName, Comma = i == proc.InputParameters.Count - 1 ? string.Empty : "," }).ToList(),
                    OutputFields = proc.OutputFields.Select((f, i) => new { f.ClrType, f.PropertyName, Comma = i == proc.OutputFields.Count - 1 ? string.Empty : "," }).ToList(),
                    Schema = proc.Schema ?? string.Empty,
                    Name = proc.ProcedureName ?? procPart,
                    // Pre-bracket (and escape) schema & procedure name so runtime does not need to normalize.
                    ProcedureFullName = "[" + (proc.Schema ?? string.Empty).Replace("]", "]]", StringComparison.Ordinal) + "].[" + (proc.ProcedureName ?? string.Empty).Replace("]", "]]", StringComparison.Ordinal) + "]",
                    ProcedureTypeName = procedureTypeName,
                    UnifiedResultTypeName = unifiedResultTypeName,
                    OutputTypeName = outputTypeName,
                    PlanTypeName = procedureTypeName + "Plan",
                    InputTypeName = inputTypeName,
                    ParameterLines = paramLines,
                    InputAssignments = proc.InputParameters.Select(ip =>
                    {
                        var isTableType = ip.Attributes != null && ip.Attributes.Any(a => a.StartsWith("[TableType]", StringComparison.Ordinal));
                        if (isTableType)
                        {
                            // Build SqlDataRecord collection via reflection helper (ExecutionSupport.TvpHelper)
                            var typeNameLiteral = (ip.SqlTypeName ?? ip.Name).Replace("\"", "\\\"", StringComparison.Ordinal);
                            return $"{{ var prm = cmd.Parameters[\"@{ip.Name}\"]; var tvp = TvpHelper.BuildRecords(input.{ip.PropertyName}) ?? Array.Empty<Microsoft.Data.SqlClient.Server.SqlDataRecord>(); prm.Value = tvp; if (prm is Microsoft.Data.SqlClient.SqlParameter sp) {{ sp.SqlDbType = System.Data.SqlDbType.Structured; sp.TypeName ??= \"{typeNameLiteral}\"; }} }}";
                        }
                        var valueExpr = ip.IsNullable ? $"(object?)input.{ip.PropertyName} ?? DBNull.Value" : $"input.{ip.PropertyName}";
                        return $"cmd.Parameters[\"@{ip.Name}\"].Value = {valueExpr};";
                    }).ToList(),
                    ResultSets = rsMeta,
                    OutputFactoryArgs = outputFactoryArgs,
                    HasAggregateOutput = proc.OutputFields.Count > 0,
                    // AggregateAssignments removed; template uses ResultSets[].AggregateAssignment directly
                };
                finalCode = Templates.RenderRawTemplate(unifiedTemplateRaw!, model);
            }
            else
            {
                // Fallback: original inline build
                var fileSb = new StringBuilder();
                fileSb.Append(headerBlock);
                fileSb.AppendLine($"namespace {finalNs};");
                fileSb.AppendLine();
                fileSb.AppendLine("\n\nusing System.Data;\nusing System.Data.Common;\n\n\nusing " + ns + ";");
                // (For brevity, we could replicate blocks, but template should normally exist now)
                finalCode = fileSb.ToString();
            }
            finalCode = NormalizeWhitespace(finalCode);
            File.WriteAllText(Path.Combine(schemaDir, procPart + ".cs"), finalCode);
            written++;
            if (!artifactsPerSchema.TryGetValue(schemaPascal, out var schemaCount))
            {
                artifactsPerSchema[schemaPascal] = 1;
            }
            else
            {
                artifactsPerSchema[schemaPascal] = schemaCount + 1;
            }

            // Aggregated warning for JSON type corrections (emitted at most once per procedure)
            if (jsonTypeCorrections.Count > 0)
            {
                try
                {
                    var verbose = LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug);
                    var explicitOptIn = EnvironmentHelper.IsTrue("XTRAQ_LOG_JSON_TYPE_MAPPING");

                    if (verbose || explicitOptIn)
                    {
                        var sample = string.Join(", ", jsonTypeCorrections.Take(5));
                        Console.Out.WriteLine($"[xtraq] JsonTypeMapping: {jsonTypeCorrections.Count} field(s) corrected for {proc.OperationName}. Examples: {sample}{(jsonTypeCorrections.Count > 5 ? ", ..." : string.Empty)}");
                    }
                }
                catch { /* ignore */ }
            }
        }
        // JSON Audit Hook: optional report generation if env var set (XTRAQ_JSON_AUDIT=1)
        try
        {
            if (EnvironmentHelper.IsTrue("XTRAQ_JSON_AUDIT"))
            {
                JsonResultSetAudit.WriteReport(_projectRoot, procs);
                Console.Out.WriteLine($"[xtraq] JsonAudit written: {Path.Combine(_projectRoot, "debug", "json-audit.txt")}");
            }
        }
        catch { /* ignore audit failures */ }
        try { Console.Out.WriteLine($"[xtraq] Generators succeeded (procedures written={written})"); } catch { }
        return BuildResult();
    }

    // Mapping helper tailored for JSON result sets (similar to MapSqlToClr but kept separate for future extensions)
    private static string MapJsonSqlToClr(string? sqlTypeName, bool nullable)
    {
        if (string.IsNullOrWhiteSpace(sqlTypeName)) return nullable ? "string?" : "string";
        var t = sqlTypeName.ToLowerInvariant();
        // Strip length/precision information (e.g. decimal(18,2))
        var parenIdx = t.IndexOf('(');
        if (parenIdx >= 0) t = t.Substring(0, parenIdx);
        string core = t switch
        {
            "int" => "int",
            "bigint" => "long",
            "smallint" => "short",
            "tinyint" => "byte",
            "bit" => "bool",
            "decimal" or "numeric" or "money" or "smallmoney" => "decimal",
            "float" => "double",
            "real" => "float",
            "date" or "datetime" or "datetime2" or "smalldatetime" => "DateTime",
            "datetimeoffset" => "DateTimeOffset",
            "time" => "TimeSpan",
            "uniqueidentifier" => "Guid",
            "varbinary" or "binary" or "image" or "rowversion" or "timestamp" => "byte[]",
            // JSON projections still map to nvarchar, treat them as string
            _ => "string"
        };
        if (core == "string")
        {
            return nullable ? "string?" : "string";
        }

        if (core == "byte[]")
        {
            return nullable ? "byte[]?" : "byte[]";
        }

        if (nullable)
        {
            core += "?";
        }

        return core;
    }

    private static string MapDbType(string? sqlType)
    {
        if (string.IsNullOrWhiteSpace(sqlType)) return "System.Data.DbType.String";
        var t = sqlType.ToLowerInvariant();
        // normalize common parentheses like nvarchar(50)
        if (t.Contains('(')) t = t[..t.IndexOf('(')];
        return t switch
        {
            "int" => "System.Data.DbType.Int32",
            "bigint" => "System.Data.DbType.Int64",
            "smallint" => "System.Data.DbType.Int16",
            "tinyint" => "System.Data.DbType.Byte",
            "bit" => "System.Data.DbType.Boolean",
            "decimal" or "numeric" or "money" or "smallmoney" => "System.Data.DbType.Decimal",
            "float" => "System.Data.DbType.Double",
            "real" => "System.Data.DbType.Single",
            "date" or "datetime" or "datetime2" or "smalldatetime" or "datetimeoffset" or "time" => "System.Data.DbType.DateTime2",
            "uniqueidentifier" => "System.Data.DbType.Guid",
            "varbinary" or "binary" or "image" => "System.Data.DbType.Binary",
            "xml" => "System.Data.DbType.Xml",
            // treat all character & text types as string
            "varchar" or "nvarchar" or "char" or "nchar" or "text" or "ntext" => "System.Data.DbType.String",
            _ => "System.Data.DbType.String"
        };
    }

    private static string EmitSize(FieldDescriptor f)
        => f.MaxLength.HasValue && f.MaxLength.Value > 0 ? f.MaxLength.Value.ToString() : "null";

    private static bool IsJsonScalarArray(FieldDescriptor f)
        => f.ReturnsJsonArray == true
           && !string.IsNullOrWhiteSpace(f.JsonElementClrType)
           && f.ClrType.Contains("[]", StringComparison.Ordinal);

    private static string GetArrayElementType(string clrType)
    {
        if (string.IsNullOrWhiteSpace(clrType))
        {
            return "object";
        }

        var type = clrType.Trim();
        if (type.EndsWith("?", StringComparison.Ordinal))
        {
            type = type[..^1];
        }

        if (type.EndsWith("[]", StringComparison.Ordinal))
        {
            type = type[..^2];
        }

        return type.Length == 0 ? "object" : type;
    }

    private static string ResolveArrayElementClrType(FieldDescriptor f)
    {
        if (!string.IsNullOrWhiteSpace(f.JsonElementClrType))
        {
            var candidate = f.JsonElementClrType!.Trim();
            if (candidate.Length > 0)
            {
                return candidate;
            }
        }

        return GetArrayElementType(f.ClrType);
    }

    private static string GetArrayDefaultExpression(FieldDescriptor f)
    {
        if (f.ClrType.EndsWith("?", StringComparison.Ordinal))
        {
            return "null";
        }

        var elementType = GetArrayElementType(f.ClrType);
        return $"System.Array.Empty<{elementType}>()";
    }

    private static string IndentBlock(string text, string indent)
    {
        if (string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        var lines = text.Split('\n');
        var sb = new System.Text.StringBuilder(text.Length + indent.Length * lines.Length);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].TrimEnd('\r');
            sb.Append(indent);
            sb.Append(line);
            if (i < lines.Length - 1)
            {
                sb.Append('\n');
            }
        }

        return sb.ToString();
    }

    private static string BuildJsonArrayMaterializer(FieldDescriptor f)
    {
        var defaultExpr = GetArrayDefaultExpression(f);
        var elementType = ResolveArrayElementClrType(f);
        var allowNullLiteral = f.ClrType.EndsWith("?", StringComparison.Ordinal) ? "true" : "false";
        var ordinalExpr = $"r.GetOrdinal(\"{f.Name}\")";
        return $"({ordinalExpr} is var __ord && __ord >= 0 ? (r.IsDBNull(__ord) ? {defaultExpr} : JsonSupport.DeserializeArray<{elementType}>(r.GetString(__ord), {allowNullLiteral})) : {defaultExpr})";
    }

    private static string BuildJsonArrayMaterializerCached(FieldDescriptor f, int ordinalIndex)
    {
        var defaultExpr = GetArrayDefaultExpression(f);
        var elementType = ResolveArrayElementClrType(f);
        var allowNullLiteral = f.ClrType.EndsWith("?", StringComparison.Ordinal) ? "true" : "false";
        var ordinalVar = $"o{ordinalIndex}";
        return $"{ordinalVar} < 0 ? {defaultExpr} : (r.IsDBNull({ordinalVar}) ? {defaultExpr} : JsonSupport.DeserializeArray<{elementType}>(r.GetString({ordinalVar}), {allowNullLiteral}))";
    }

    private static string MaterializeFieldExpression(FieldDescriptor f)
    {
        if (IsJsonScalarArray(f))
        {
            return BuildJsonArrayMaterializer(f);
        }

        if (string.Equals(f.ClrType, "System.Text.Json.Nodes.JsonNode?", StringComparison.Ordinal))
        {
            return $"ReadJsonNode(r, r.GetOrdinal(\"{f.Name}\"))";
        }

        var accessor = f.ClrType switch
        {
            "int" or "int?" => "GetInt32",
            "long" or "long?" => "GetInt64",
            "short" or "short?" => "GetInt16",
            "byte" or "byte?" => "GetByte",
            "bool" or "bool?" => "GetBoolean",
            "decimal" or "decimal?" => "GetDecimal",
            "double" or "double?" => "GetDouble",
            "float" or "float?" => "GetFloat",
            "DateTime" or "DateTime?" => "GetDateTime",
            "Guid" or "Guid?" => "GetGuid",
            _ => null
        };
        var prop = f.PropertyName;
        if (accessor == null)
        {
            // string, byte[], fallback
            if (f.ClrType.StartsWith("byte[]"))
                return $"r.IsDBNull(r.GetOrdinal(\"{f.Name}\")) ? System.Array.Empty<byte>() : (byte[])r[\"{f.Name}\"]";
            if (f.ClrType == "string")
                return $"r.IsDBNull(r.GetOrdinal(\"{f.Name}\")) ? string.Empty : r.GetString(r.GetOrdinal(\"{f.Name}\"))";
            if (f.ClrType == "string?")
                return $"r.IsDBNull(r.GetOrdinal(\"{f.Name}\")) ? null : r.GetString(r.GetOrdinal(\"{f.Name}\"))";
            return $"r[\"{f.Name}\"]"; // generic
        }
        var nullable = f.IsNullable && !f.ClrType.EndsWith("?") ? true : f.ClrType.EndsWith("?");
        if (nullable)
            return $"r.IsDBNull(r.GetOrdinal(\"{f.Name}\")) ? null : ({f.ClrType})r.{accessor}(r.GetOrdinal(\"{f.Name}\"))";
        return $"r.{accessor}(r.GetOrdinal(\"{f.Name}\"))";
    }

    private static string CastOutputValue(FieldDescriptor f)
    {
        var target = f.ClrType;
        var name = f.Name.TrimStart('@');
        return target switch
        {
            "string" => $"values.TryGetValue(\"{name}\", out var v_{name}) ? (string?)v_{name} ?? string.Empty : string.Empty",
            _ => $"values.TryGetValue(\"{name}\", out var v_{name}) ? ({target})v_{name} : default"
        };
    }

    private static string MaterializeFieldExpressionCached(FieldDescriptor f, int ordinalIndex)
    {
        if (IsJsonScalarArray(f))
        {
            return BuildJsonArrayMaterializerCached(f, ordinalIndex);
        }

        if (string.Equals(f.ClrType, "System.Text.Json.Nodes.JsonNode?", StringComparison.Ordinal))
        {
            return $"ReadJsonNode(r, o{ordinalIndex})";
        }

        var accessor = f.ClrType switch
        {
            "int" or "int?" => "GetInt32",
            "long" or "long?" => "GetInt64",
            "short" or "short?" => "GetInt16",
            "byte" or "byte?" => "GetByte",
            "bool" or "bool?" => "GetBoolean",
            "decimal" or "decimal?" => "GetDecimal",
            "double" or "double?" => "GetDouble",
            "float" or "float?" => "GetFloat",
            "DateTime" or "DateTime?" => "GetDateTime",
            "Guid" or "Guid?" => "GetGuid",
            _ => null
        };
        // Determine default fallback expression if ordinal not found
        string defaultExpr = f.ClrType switch
        {
            "string" => "string.Empty",
            "string?" => "null",
            var t when t == "byte[]" => "System.Array.Empty<byte>()",
            var t when t.EndsWith("[]?", StringComparison.Ordinal) => "null",
            var t when t.EndsWith("[]", StringComparison.Ordinal) => $"System.Array.Empty<{GetArrayElementType(t)}>()",
            var t when t.EndsWith("?", StringComparison.Ordinal) => "null",
            _ => $"default({f.ClrType})"
        };
        if (accessor == null)
        {
            if (f.ClrType.StartsWith("byte[]"))
                return $"o{ordinalIndex} < 0 ? {defaultExpr} : (r.IsDBNull(o{ordinalIndex}) ? System.Array.Empty<byte>() : (byte[])r.GetValue(o{ordinalIndex}))";
            if (f.ClrType == "string")
                return $"o{ordinalIndex} < 0 ? {defaultExpr} : (r.IsDBNull(o{ordinalIndex}) ? string.Empty : r.GetString(o{ordinalIndex}))";
            if (f.ClrType == "string?")
                return $"o{ordinalIndex} < 0 ? {defaultExpr} : (r.IsDBNull(o{ordinalIndex}) ? null : r.GetString(o{ordinalIndex}))";
            return $"o{ordinalIndex} < 0 ? {defaultExpr} : r.GetValue(o{ordinalIndex})";
        }
        var nullable = f.IsNullable && !f.ClrType.EndsWith("?") ? true : f.ClrType.EndsWith("?");
        if (nullable)
            return $"o{ordinalIndex} < 0 ? {defaultExpr} : (r.IsDBNull(o{ordinalIndex}) ? null : ({f.ClrType})r.{accessor}(o{ordinalIndex}))";
        return $"o{ordinalIndex} < 0 ? {defaultExpr} : r.{accessor}(o{ordinalIndex})";
    }

    private static string ConvertReaderVariable(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return raw;
        }

        var converted = raw.Replace("ReaderUtil.TryGetOrdinal(r", "ReaderUtil.TryGetOrdinal(reader", StringComparison.Ordinal);
        converted = Regex.Replace(converted, @"\br\.", "reader.");
        return converted;
    }

    private static System.Text.Json.Nodes.JsonNode? ReadJsonNode(System.Data.Common.DbDataReader reader, int ordinal)
    {
        if (ordinal < 0)
        {
            return null;
        }

        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        try
        {
            var raw = reader.GetString(ordinal);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            return System.Text.Json.Nodes.JsonNode.Parse(raw);
        }
        catch
        {
            return null;
        }
    }

    private static string ToPascalCase(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;
        // 1. Split input by common separators
        var rawParts = input.Split(new[] { '-', '_', ' ', '.', '/', '\\' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .ToList();
        if (rawParts.Count == 0) return string.Empty;

        // 2. Heuristic: keep existing uppercase sequences (CamelCase fragments) within each segment.
        //    Example: "WorkflowListAsJson" stays untouched; "workflowListAsJSON" becomes "WorkflowListAsJSON" (JSON remains uppercase when it has 2+ capital letters).
        string NormalizeSegment(string seg)
        {
            if (seg.Length == 0) return seg;
            // If the segment is fully lower case, apply classic capitalization
            if (seg.All(ch => char.IsLetter(ch) ? char.IsLower(ch) : true))
            {
                return char.ToUpperInvariant(seg[0]) + (seg.Length > 1 ? seg.Substring(1) : string.Empty);
            }
            // When the segment is entirely uppercase (<= 4 chars), keep it as an acronym (e.g., API, SQL, JSON)
            if (seg.All(ch => !char.IsLetter(ch) || char.IsUpper(ch)))
            {
                if (seg.Length <= 4) return seg.ToUpperInvariant();
                // For longer uppercase segments keep only the first letter uppercase (e.g., WORKFLOW -> Workflow)
                return char.ToUpperInvariant(seg[0]) + seg.Substring(1).ToLowerInvariant();
            }
            // Mixed pattern: keep existing uppercase sequences but ensure the first letter is uppercase.
            // Split at lower->upper transitions to surface internal tokens, then recombine with proper casing.
            var sb = new System.Text.StringBuilder();
            var token = new System.Text.StringBuilder();
            void FlushToken()
            {
                if (token.Length == 0) return;
                var t = token.ToString();
                if (t.Length <= 4 && t.All(ch => char.IsUpper(ch)))
                {
                    // Preserve acronym casing
                    sb.Append(t.ToUpperInvariant());
                }
                else
                {
                    sb.Append(char.ToUpperInvariant(t[0]) + (t.Length > 1 ? t.Substring(1) : string.Empty));
                }
                token.Clear();
            }
            for (int i = 0; i < seg.Length; i++)
            {
                var ch = seg[i];
                if (!char.IsLetterOrDigit(ch)) { FlushToken(); continue; }
                if (token.Length > 0)
                {
                    var prev = token[token.Length - 1];
                    // Transition: previous char lower, current upper -> start a new token (CamelCase boundary)
                    if (char.IsLetter(prev) && char.IsLower(prev) && char.IsLetter(ch) && char.IsUpper(ch))
                    {
                        FlushToken();
                    }
                    // Transition: multiple uppercase letters followed by lowercase -> break before the lowercase unless only one uppercase so far
                    else if (token.Length >= 2 && token.ToString().All(cc => char.IsUpper(cc)) && char.IsLetter(ch) && char.IsLower(ch))
                    {
                        // End of acronym
                        FlushToken();
                    }
                }
                token.Append(ch);
            }
            FlushToken();
            var result = sb.ToString();
            if (result.Length == 0)
                result = char.ToUpperInvariant(seg[0]) + (seg.Length > 1 ? seg.Substring(1).ToLowerInvariant() : string.Empty);
            return result;
        }

        var normalizedParts = rawParts.Select(NormalizeSegment).ToList();
        var candidate = string.Concat(normalizedParts);
        candidate = new string(candidate.Where(ch => char.IsLetterOrDigit(ch) || ch == '_').ToArray());
        if (string.IsNullOrEmpty(candidate)) candidate = "Schema";
        if (char.IsDigit(candidate[0])) candidate = "N" + candidate;
        return candidate;
    }

    private static readonly Regex MultiBlankLines = new("(\r?\n){3,}", RegexOptions.Compiled);
    private static string NormalizeWhitespace(string code)
    {
        if (string.IsNullOrEmpty(code)) return code;
        // Collapse 3+ consecutive newlines to exactly two (i.e., one blank line)
        code = MultiBlankLines.Replace(code, match => match.Value.StartsWith("\r\n\r\n") ? "\r\n\r\n" : "\n\n");
        // Ensure exactly one trailing newline
        if (!code.EndsWith("\n")) code += Environment.NewLine;
        return code;
    }

    // Build a result set record type name without the trailing 'Result' suffix.
    private static string SanitizeType(string procPart, string baseName)
    {
        // Use full ResultSet naming (includes 'Result' suffix) to align with existing tests expecting this suffix.
        return NamePolicy.ResultSet(procPart, baseName);
    }

    private static string AliasToIdentifier(string alias)
    {
        if (string.IsNullOrWhiteSpace(alias)) return "_";
        // Preserve original casing; replace invalid characters with '_'
        var sb = new System.Text.StringBuilder(alias.Length);
        for (int i = 0; i < alias.Length; i++)
        {
            var ch = alias[i];
            if (i == 0)
            {
                if (char.IsLetter(ch) || ch == '_') sb.Append(ch);
                else sb.Append('_');
            }
            else
            {
                if (char.IsLetterOrDigit(ch) || ch == '_') sb.Append(ch);
                else sb.Append('_');
            }
        }
        var ident = sb.ToString();
        if (string.IsNullOrEmpty(ident)) ident = "_";
        // Prefix with '_' if the first character is a digit
        if (char.IsDigit(ident[0])) ident = "_" + ident;
        // Escape C# reserved keywords with '@'
        if (IsCSharpKeyword(ident)) ident = "@" + ident;
        return ident;
    }

    private static readonly HashSet<string> CSharpKeywords = new(new[]
    {
        "abstract","as","base","bool","break","byte","case","catch","char","checked","class","const","continue","decimal","default","delegate","do","double","else","enum","event","explicit","extern","false","finally","fixed","float","for","foreach","goto","if","implicit","in","int","interface","internal","is","lock","long","namespace","new","null","object","operator","out","override","params","private","protected","public","readonly","ref","return","sbyte","sealed","short","sizeof","stackalloc","static","string","struct","switch","this","throw","true","try","typeof","uint","ulong","unchecked","unsafe","ushort","using","virtual","void","volatile","while"
    }, StringComparer.Ordinal);

    private static bool IsCSharpKeyword(string ident) => CSharpKeywords.Contains(ident);
}

