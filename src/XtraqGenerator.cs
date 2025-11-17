using Xtraq.Configuration;
using Xtraq.Engine;
using Xtraq.Generators;
using Xtraq.Metadata;
using Xtraq.Utils;

namespace Xtraq;

/// <summary>
/// Orchestrates future generation steps (placeholder implementation).
/// </summary>
public sealed class XtraqGenerator
{
    private readonly ITemplateRenderer _renderer;
    private readonly ITemplateLoader? _loader;
    private readonly Func<IReadOnlyList<InputDescriptor>> _inputs;
    private readonly Func<IReadOnlyList<OutputDescriptor>> _outputs;
    private readonly Func<IReadOnlyList<ResultDescriptor>> _results;
    private readonly Func<IReadOnlyList<ProcedureDescriptor>> _procedures;
    private readonly Func<ISchemaMetadataProvider>? _schemaProviderFactory;

    /// <summary>
    /// Creates a new generator instance with optional metadata providers and template services.
    /// </summary>
    /// <param name="renderer">Template renderer used for emitting artefacts.</param>
    /// <param name="loader">Optional template loader for on-demand retrieval.</param>
    /// <param name="inputsProvider">Delegate that returns input descriptors to emit.</param>
    /// <param name="outputsProvider">Delegate that returns output descriptors to emit.</param>
    /// <param name="resultsProvider">Delegate that returns known result descriptors.</param>
    /// <param name="proceduresProvider">Delegate that exposes stored procedure descriptors.</param>
    /// <param name="schemaProviderFactory">Factory building a metadata provider when richer schema access is required.</param>
    public XtraqGenerator(
        ITemplateRenderer renderer,
        ITemplateLoader? loader = null,
        Func<IReadOnlyList<InputDescriptor>>? inputsProvider = null,
        Func<IReadOnlyList<OutputDescriptor>>? outputsProvider = null,
        Func<IReadOnlyList<ResultDescriptor>>? resultsProvider = null,
        Func<IReadOnlyList<ProcedureDescriptor>>? proceduresProvider = null,
        Func<ISchemaMetadataProvider>? schemaProviderFactory = null)
    {
        _renderer = renderer;
        _loader = loader; // optional until full wiring
        _inputs = inputsProvider ?? (() => Array.Empty<InputDescriptor>());
        _outputs = outputsProvider ?? (() => Array.Empty<OutputDescriptor>());
        _results = resultsProvider ?? (() => Array.Empty<ResultDescriptor>());
        _procedures = proceduresProvider ?? (() => Array.Empty<ProcedureDescriptor>());
        _schemaProviderFactory = schemaProviderFactory;
    }

    /// <summary>
    /// Renders a demo template with the Xtraq name.
    /// </summary>
    public string RenderDemo() => _renderer.Render("// Demo {{ Name }}", new { Name = "Xtraq" });

    /// <summary>
    /// Generates a minimal DbContext template into the target directory.
    /// </summary>
    /// <param name="outputDir">Directory to place generated file.</param>
    /// <param name="namespaceRoot">Namespace root (fallback: Xtraq.Generated).</param>
    /// <param name="className">Class name (fallback: XtraqDbContext).</param>
    /// <returns>Path of generated file or null if template missing.</returns>
    public string? GenerateMinimalDbContext(string outputDir, string? namespaceRoot = null, string? className = null)
    {
        if (_loader == null)
            return null; // loader not provided yet

        if (!(_loader.TryLoad("XtraqDbContext", out var tpl) || _loader.TryLoad("DbContext", out tpl)))
            return null; // template not present

        var ns = string.IsNullOrWhiteSpace(namespaceRoot) ? "Xtraq.Generated" : namespaceRoot!;
        var cls = string.IsNullOrWhiteSpace(className) ? "XtraqDbContext" : className!;
        var rendered = _renderer.Render(tpl, new { Namespace = ns, ClassName = cls, GeneratorLabel = GeneratorBranding.Label });
        Directory.CreateDirectory(outputDir);
        var file = Path.Combine(outputDir, cls + ".cs");
        File.WriteAllText(file, rendered);
        return file;
    }

    /// <summary>
    /// Full generation pipeline for artifacts (idempotent per run).
    /// </summary>
    public int GenerateAll(XtraqConfiguration cfg, string? projectRoot = null)
    {
        projectRoot ??= Directory.GetCurrentDirectory();
        // Template cache-state check (hash .xqt templates). If changed, force metadata reload.
        TryApplyTemplateCacheState(projectRoot);
        // Derive namespace considering the configuration path (-p)
        var resolver = new NamespaceResolver(cfg, msg => Console.Out.WriteLine(msg));
        var nsBase = resolver.Resolve(projectRoot); // simplified: just directory of configPath or projectRoot
        // Compose final namespace: append output dir once
        var outSeg = string.IsNullOrWhiteSpace(cfg.OutputDir) ? "Xtraq" : cfg.OutputDir!.Trim('.');
        var ns = nsBase.EndsWith('.' + outSeg, StringComparison.OrdinalIgnoreCase) ? nsBase : nsBase + '.' + outSeg;
        var total = 0;

        // If a schema provider factory is supplied and no explicit delegates were provided, use it to populate metadata.
        if (_schemaProviderFactory != null)
        {
            var schema = _schemaProviderFactory();
            if (_inputs == null || ReferenceEquals(_inputs, (Func<IReadOnlyList<InputDescriptor>>)(() => Array.Empty<InputDescriptor>())))
            {
                // no op - existing delegate already returns empty; we can't reassign readonly field, so rely on direct usage below
            }
            // Instead of attempting to mutate delegates, we will bypass and instantiate generators directly with schema collections when factory is present.
        }

        // Determine whether to emit minimal DbContext stub:
        // If a full DbContext already exists (e.g., XtraqDbContext.cs with options), skip stub.
        bool dbContextAlreadyPresent = false;
        try
        {
            foreach (var file in Directory.EnumerateFiles(projectRoot, "XtraqDbContext.cs", SearchOption.AllDirectories))
            {
                // Heuristic: if sibling file 'XtraqDbContextOptions.cs' exists, treat as full context
                var dir = Path.GetDirectoryName(file)!;
                if (File.Exists(Path.Combine(dir, "XtraqDbContextOptions.cs")))
                {
                    dbContextAlreadyPresent = true;
                    break;
                }
            }
        }
        catch { }

        if (!dbContextAlreadyPresent)
        {
            string dbCtxOutDir;
            if (string.IsNullOrWhiteSpace(cfg.OutputDir))
            {
                dbCtxOutDir = Path.Combine(projectRoot, "Xtraq");
            }
            else if (Path.IsPathRooted(cfg.OutputDir))
            {
                dbCtxOutDir = cfg.OutputDir;
            }
            else
            {
                dbCtxOutDir = Path.Combine(projectRoot, cfg.OutputDir);
            }
            GenerateMinimalDbContext(dbCtxOutDir, ns, "XtraqDbContext");
        }
        else
        {
            Console.Out.WriteLine("[xtraq] Info: Skipping minimal DbContext generation (full DbContext already present).");
        }

        // Base output directory: ensure we point at .../Xtraq for sample so schema folders appear beneath it
        var baseStructuredOut = projectRoot.EndsWith(Path.DirectorySeparatorChar + "Xtraq", StringComparison.OrdinalIgnoreCase)
            ? projectRoot
            : Path.Combine(projectRoot, "Xtraq");
        Directory.CreateDirectory(baseStructuredOut);
        // Consolidated generation: delegate to ProceduresGenerator (future home for input/output/result aggregation) and handle table types separately when required
        if (_schemaProviderFactory is null)
        {
            var procsGen = new ProceduresGenerator(_renderer, _procedures, _loader, projectRoot, cfg);
            var procResult = procsGen.Generate(ns, baseStructuredOut);
            total += procResult.TotalArtifacts;
        }
        else
        {
            var schema = _schemaProviderFactory();
            try
            {
                var dbgInputs = schema.GetInputs().Count;
                var dbgOutputs = schema.GetOutputs().Count;
                var dbgProcs = schema.GetProcedures().Count;
                Console.Out.WriteLine($"[xtraq] descriptor counts: inputs={dbgInputs} outputs={dbgOutputs} procedures={dbgProcs}");
            }
            catch { }
            var functionDescriptors = schema.GetFunctionJsonDescriptors();
            if (functionDescriptors.Count == 0)
            {
                try { Console.Out.WriteLine("[xtraq] Info: no function JSON descriptors resolved."); } catch { }
            }
            else
            {
                try { Console.Out.WriteLine($"[xtraq] Info: function JSON descriptors resolved={functionDescriptors.Count}"); } catch { }
            }
            if (functionDescriptors.Count > 0)
            {
                var functionsGen = new FunctionJsonOutputGenerator(_renderer, _loader, cfg);
                total += functionsGen.Generate(ns, baseStructuredOut, functionDescriptors);
            }
            var procsGen = new ProceduresGenerator(_renderer, () => schema.GetProcedures(), _loader, projectRoot, cfg, functionJsonResolver: schema.TryGetFunctionJsonDescriptor);
            var procResult = procsGen.Generate(ns, baseStructuredOut);
            total += procResult.TotalArtifacts;
        }

        return total;
    }

    private void TryApplyTemplateCacheState(string projectRoot)
    {
        try
        {
            if (_loader == null) return; // no template loader injected
            // Attempt to locate canonical Templates directory relative to solution root
            var solutionRoot = Xtraq.Utils.ProjectRootResolver.GetSolutionRootOrCwd();
            var templatesDir = Path.Combine(solutionRoot, "src", "Templates");
            if (!Directory.Exists(templatesDir)) return;
            var manifest = Xtraq.Utils.DirectoryHasher.HashDirectory(templatesDir, p => p.EndsWith(".xqt", StringComparison.OrdinalIgnoreCase));
            var templatesHash = manifest.AggregateSha256;
            var cacheDir = Path.Combine(projectRoot, ".xtraq", "cache");
            Directory.CreateDirectory(cacheDir);
            var cacheFile = Path.Combine(cacheDir, "cache-state.json");
            CacheState? previous = null;
            if (File.Exists(cacheFile))
            {
                try { previous = System.Text.Json.JsonSerializer.Deserialize<CacheState>(File.ReadAllText(cacheFile)); } catch { }
            }
            var currentVersion = GeneratorBranding.GeneratorVersion;
            var state = new CacheState { TemplatesHash = templatesHash, GeneratorVersion = currentVersion, LastWriteUtc = DateTime.UtcNow };
            File.WriteAllText(cacheFile, System.Text.Json.JsonSerializer.Serialize(state, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
            bool changed = previous == null || previous.TemplatesHash != templatesHash || previous.GeneratorVersion != currentVersion;
            if (changed)
            {
                Xtraq.Utils.CacheControl.ForceReload = true;
                var reason = previous == null ? "initialization" : (previous.TemplatesHash != templatesHash ? "hash-diff" : "version-change");
                Console.Out.WriteLine($"[xtraq] Info: Template cache-state {reason}; hash={templatesHash.Substring(0, 8)} -> reload metadata. path={cacheFile}");
            }
            else
            {
                Console.Out.WriteLine($"[xtraq] Info: Template cache-state unchanged (hash={templatesHash.Substring(0, 8)}) path={cacheFile}.");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[xtraq] Warning: Template cache-state evaluation failed: {ex.Message}");
        }
    }

    private sealed class CacheState
    {
        public string TemplatesHash { get; set; } = string.Empty;
        public string GeneratorVersion { get; set; } = string.Empty;
        public DateTime LastWriteUtc { get; set; }
    }
}
