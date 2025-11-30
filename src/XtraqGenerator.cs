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
    private readonly Func<IReadOnlyList<ProcedureDescriptor>> _procedures;
    private readonly Func<ISchemaMetadataProvider>? _schemaProviderFactory;

    /// <summary>
    /// Creates a new generator instance with optional metadata providers and template services.
    /// </summary>
    /// <param name="renderer">Template renderer used for emitting artefacts.</param>
    /// <param name="loader">Optional template loader for on-demand retrieval.</param>
    /// <param name="proceduresProvider">Delegate that exposes stored procedure descriptors.</param>
    /// <param name="schemaProviderFactory">Factory building a metadata provider when richer schema access is required.</param>
    public XtraqGenerator(
        ITemplateRenderer renderer,
        ITemplateLoader? loader = null,
        Func<IReadOnlyList<ProcedureDescriptor>>? proceduresProvider = null,
        Func<ISchemaMetadataProvider>? schemaProviderFactory = null)
    {
        _renderer = renderer ?? throw new ArgumentNullException(nameof(renderer));
        _loader = loader; // optional until full wiring
        _procedures = proceduresProvider ?? (() => Array.Empty<ProcedureDescriptor>());
        _schemaProviderFactory = schemaProviderFactory;
    }

    /// <summary>
    /// Renders a demo template with the Xtraq name.
    /// </summary>
    public string RenderDemo() => _renderer.Render("// Demo {{ Name }}", new { Name = "Xtraq" });

    /// <summary>
    /// Full generation pipeline for artifacts (idempotent per run).
    /// </summary>
    public int GenerateAll(XtraqConfiguration cfg, string? projectRoot = null)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        projectRoot ??= Directory.GetCurrentDirectory();
        // Template cache-state check (hash .xqt templates). If changed, force metadata reload.
        TryApplyTemplateCacheState(projectRoot);
        // Derive namespace considering the configuration path (-p)
        var nsBase = cfg.NamespaceRoot ?? throw new InvalidOperationException("XTRAQ_NAMESPACE is not configured.");
        // Compose final namespace: append output dir once
        var outSeg = string.IsNullOrWhiteSpace(cfg.OutputDir) ? "Xtraq" : cfg.OutputDir!.Trim('.');
        var ns = nsBase.EndsWith('.' + outSeg, StringComparison.OrdinalIgnoreCase) ? nsBase : nsBase + '.' + outSeg;
        var baseStructuredOut = projectRoot.EndsWith(Path.DirectorySeparatorChar + "Xtraq", StringComparison.OrdinalIgnoreCase)
            ? projectRoot
            : Path.Combine(projectRoot, "Xtraq");
        Directory.CreateDirectory(baseStructuredOut);

        var total = 0;
        var schema = _schemaProviderFactory?.Invoke();

        if (schema is null)
        {
            var proceduresGenerator = new ProceduresGenerator(_renderer, _procedures, _loader, projectRoot, cfg);
            total += proceduresGenerator.Generate(ns, baseStructuredOut).TotalArtifacts;
            return total;
        }

        var functionDescriptors = schema.GetFunctionJsonDescriptors();
        if (functionDescriptors.Count > 0)
        {
            var functionsGenerator = new FunctionJsonResultGenerator(_renderer, _loader, cfg);
            total += functionsGenerator.Generate(ns, baseStructuredOut, functionDescriptors);
        }

        var schemaProceduresGenerator = new ProceduresGenerator(
            _renderer,
            schema.GetProcedures,
            _loader,
            projectRoot,
            cfg,
            functionJsonResolver: schema.TryGetFunctionJsonDescriptor);
        total += schemaProceduresGenerator.Generate(ns, baseStructuredOut).TotalArtifacts;

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
