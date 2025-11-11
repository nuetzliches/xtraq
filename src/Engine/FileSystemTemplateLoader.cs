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

        _currentTfmMajor = TemplateLoaderSupport.ResolveCurrentTfmMajor();
        _byLogical = new(StringComparer.OrdinalIgnoreCase);

        foreach (var file in Directory.EnumerateFiles(_root, "*.spt", SearchOption.AllDirectories))
        {
            var logicalName = Path.GetFileNameWithoutExtension(file);
            TemplateLoaderSupport.AddTemplate(_byLogical, logicalName, File.ReadAllText(file));
        }
    }

    /// <inheritdoc />
    public bool TryLoad(string name, out string content)
    {
        if (_byLogical.TryGetValue(name, out var variants) && TemplateLoaderSupport.TryResolveTemplate(variants, _currentTfmMajor, out content!))
        {
            return true;
        }

        content = null!;
        return false;
    }

    /// <inheritdoc />
    public IEnumerable<string> ListNames() => _byLogical.Keys;
}
