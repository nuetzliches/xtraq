namespace Xtraq.Engine;

/// <summary>
/// Loads <c>.spt</c> templates embedded as manifest resources within the executing assembly.
/// </summary>
public sealed class EmbeddedResourceTemplateLoader : ITemplateLoader
{
    private readonly Dictionary<string, Dictionary<string, string>> _catalog;
    private readonly string _currentTfmMajor;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmbeddedResourceTemplateLoader"/> class.
    /// </summary>
    /// <param name="assembly">Assembly that hosts embedded templates.</param>
    /// <param name="resourcePrefix">Resource name prefix that identifies template entries (e.g. <c>Xtraq.Templates.</c>).</param>
    public EmbeddedResourceTemplateLoader(Assembly assembly, string resourcePrefix)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (string.IsNullOrWhiteSpace(resourcePrefix))
        {
            throw new ArgumentException("Resource prefix required", nameof(resourcePrefix));
        }

        if (!resourcePrefix.EndsWith(".", StringComparison.Ordinal))
        {
            resourcePrefix += '.';
        }

        _currentTfmMajor = TemplateLoaderSupport.ResolveCurrentTfmMajor();
        _catalog = new(StringComparer.OrdinalIgnoreCase);

        foreach (var resourceName in assembly.GetManifestResourceNames())
        {
            if (!resourceName.StartsWith(resourcePrefix, StringComparison.Ordinal) || !resourceName.EndsWith(".spt", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            using var stream = assembly.GetManifestResourceStream(resourceName);
            if (stream is null)
            {
                continue;
            }

            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: false);
            var content = reader.ReadToEnd();

            var logicalName = resourceName[resourcePrefix.Length..^4]; // strip prefix and .spt extension

            static bool IsVariantSegment(string segment)
            {
                if (segment.Length <= 3 || !segment.StartsWith("net", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                for (var i = 3; i < segment.Length; i++)
                {
                    if (!char.IsDigit(segment[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            var segments = logicalName.Split('.', StringSplitOptions.RemoveEmptyEntries);
            string simpleName;
            if (segments.Length == 0)
            {
                simpleName = logicalName;
            }
            else if (segments.Length >= 2 && IsVariantSegment(segments[^1]))
            {
                // Preserve variant suffix (e.g. UnifiedProcedure.net10)
                simpleName = string.Join('.', segments[^2], segments[^1]);
            }
            else
            {
                simpleName = segments[^1];
            }

            TemplateLoaderSupport.AddTemplate(_catalog, simpleName, content);
            if (!string.Equals(simpleName, logicalName, StringComparison.OrdinalIgnoreCase))
            {
                TemplateLoaderSupport.AddTemplate(_catalog, logicalName, content);
            }
        }
    }

    /// <inheritdoc />
    public bool TryLoad(string name, out string content)
    {
        if (_catalog.TryGetValue(name, out var variants) && TemplateLoaderSupport.TryResolveTemplate(variants, _currentTfmMajor, out content!))
        {
            return true;
        }

        content = null!;
        return false;
    }

    /// <inheritdoc />
    public IEnumerable<string> ListNames() => _catalog.Keys;
}
