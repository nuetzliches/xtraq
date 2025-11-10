
namespace Xtraq.Engine;

/// <summary>
/// Simple file system based template loader. Looks for *.spt (Xtraq Template) files in a root directory.
/// Naming convention base: <c>LogicalName.spt</c> (e.g. <c>DbContext.spt</c>).
/// Versioned override (TFM major) convention: <c>LogicalName.net10.spt</c>, <c>LogicalName.net9.spt</c>, and so on (higher preferred).
/// </summary>
public sealed class FileSystemTemplateLoader : ITemplateLoader
{
    private readonly string _root;
    private readonly Dictionary<string, Dictionary<string, string>> _byLogical; // logicalName -> variantKey -> content
    private readonly string _currentTfmMajor;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileSystemTemplateLoader"/> class using the specified template root directory.
    /// </summary>
    /// <param name="rootDirectory">The root directory that contains <c>.spt</c> template files and optional variant folders.</param>
    /// <exception cref="ArgumentException">Thrown when the provided directory path is null or whitespace.</exception>
    /// <exception cref="DirectoryNotFoundException">Thrown when the directory cannot be located on disk.</exception>
    public FileSystemTemplateLoader(string rootDirectory)
    {
        if (string.IsNullOrWhiteSpace(rootDirectory))
            throw new ArgumentException("Root directory required", nameof(rootDirectory));
        _root = Path.GetFullPath(rootDirectory);
        if (!Directory.Exists(_root))
            throw new DirectoryNotFoundException(_root);
        _currentTfmMajor = ResolveCurrentTfmMajor();
        _byLogical = new(StringComparer.OrdinalIgnoreCase);
        void AddFile(string file)
        {
            var fileName = Path.GetFileName(file);
            var logical = Path.GetFileNameWithoutExtension(fileName);
            // strip version suffix if pattern matches *.net<digits>
            string variantKey = "base";
            var idx = logical.LastIndexOf('.');
            if (idx > 0 && idx < logical.Length - 1)
            {
                var tail = logical.Substring(idx + 1);
                if (tail.StartsWith("net", StringComparison.OrdinalIgnoreCase) && tail.Length > 3 && tail.Skip(3).All(char.IsDigit))
                {
                    variantKey = tail.ToLowerInvariant();
                    logical = logical.Substring(0, idx); // remove suffix
                }
            }
            if (!_byLogical.TryGetValue(logical, out var variants))
            {
                variants = new(StringComparer.OrdinalIgnoreCase);
                _byLogical[logical] = variants;
            }
            variants[variantKey] = File.ReadAllText(file);
        }
        foreach (var f in Directory.EnumerateFiles(_root, "*.spt", SearchOption.TopDirectoryOnly)) AddFile(f);
        foreach (var subDir in Directory.EnumerateDirectories(_root))
            foreach (var f in Directory.EnumerateFiles(subDir, "*.spt", SearchOption.TopDirectoryOnly)) AddFile(f);
    }

    /// <summary>
    /// Attempts to load the template content associated with the specified logical name.
    /// </summary>
    /// <param name="name">The logical template name (e.g. <c>DbContext</c>).</param>
    /// <param name="content">The resolved template content when the method returns <c>true</c>; otherwise undefined.</param>
    /// <returns><c>true</c> when a matching template variant is discovered; otherwise <c>false</c>.</returns>
    public bool TryLoad(string name, out string content)
    {
        if (_byLogical.TryGetValue(name, out var variants))
        {
            // precedence: exact currentTfmMajor (e.g. net10) -> base
            if (variants.TryGetValue(_currentTfmMajor, out content!)) return true;
            if (variants.TryGetValue("base", out content!)) return true;
            // fallback: highest net* available (sorted desc)
            var netVariant = variants.Keys
                .Where(k => k.StartsWith("net"))
                .OrderByDescending(k => k.Length) // net10 > net9 (string length diff) safe for net10/net8
                .ThenByDescending(k => k)
                .FirstOrDefault();
            if (netVariant != null)
            {
                content = variants[netVariant];
                return true;
            }
        }
        content = null!;
        return false;
    }

    /// <summary>
    /// Enumerates the logical names for every template discovered in the file system.
    /// </summary>
    /// <returns>A sequence of logical template names.</returns>
    public IEnumerable<string> ListNames() => _byLogical.Keys;

    /// <summary>
    /// Resolves the targeted TFM major band (e.g. <c>net8</c>, <c>net10</c>) used to prioritize template variants.
    /// </summary>
    /// <returns>The normalized TFM major identifier.</returns>
    private static string ResolveCurrentTfmMajor()
    {
        // Attempt to detect via compiled assemblies (multi-target scenario: environment variable or constants usually used).
        // Simplify: inspect environment variable XTRAQ_TFM if provided else default net10 preferred for forward features.
        var tfm = Environment.GetEnvironmentVariable("XTRAQ_TFM");
        if (!string.IsNullOrWhiteSpace(tfm))
        {
            var major = ExtractMajor(tfm!);
            if (major != null) return major;
        }
        // Fallback: prefer latest known (net10) to allow advanced templates; can be overridden by env.
        return "net10";
    }

    /// <summary>
    /// Extracts the normalized major TFM moniker from the supplied target framework string.
    /// </summary>
    /// <param name="tfm">The raw target framework string.</param>
    /// <returns>The normalized <c>net*</c> identifier when parsing succeeds; otherwise <c>null</c>.</returns>
    private static string? ExtractMajor(string tfm)
    {
        tfm = tfm.Trim().ToLowerInvariant();
        if (tfm.StartsWith("net"))
        {
            // net8.0 / net9.0 / net10.0 => net8 / net9 / net10
            var digits = new string(tfm.Skip(3).TakeWhile(c => char.IsDigit(c)).ToArray());
            if (digits.Length > 0) return "net" + digits;
        }
        return null;
    }
}
