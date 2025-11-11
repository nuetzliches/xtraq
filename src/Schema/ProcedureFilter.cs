using Xtraq.Services;

namespace Xtraq.Schema;

/// <summary>
/// Provides filtering support for stored procedures using exact identifiers and wildcard expressions.
/// </summary>
internal sealed class ProcedureFilter
{
    private readonly HashSet<string> _exactMatches;
    private readonly List<Regex> _wildcardPatterns;

    private ProcedureFilter(bool hasFilter, HashSet<string> exactMatches, List<Regex> wildcardPatterns)
    {
        HasFilter = hasFilter;
        _exactMatches = exactMatches;
        _wildcardPatterns = wildcardPatterns;
    }

    /// <summary>
    /// Gets a value indicating whether the filter limits the procedure set.
    /// </summary>
    public bool HasFilter { get; }

    /// <summary>
    /// Evaluates whether the provided fully-qualified procedure name matches the configured filter.
    /// </summary>
    /// <param name="fullyQualifiedName">Schema-qualified procedure name (schema.name).</param>
    /// <returns><c>true</c> when the filter matches the provided name; otherwise <c>false</c>.</returns>
    public bool Matches(string fullyQualifiedName)
    {
        if (!HasFilter)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(fullyQualifiedName))
        {
            return false;
        }

        if (_exactMatches.Contains(fullyQualifiedName))
        {
            return true;
        }

        foreach (var pattern in _wildcardPatterns)
        {
            if (pattern.IsMatch(fullyQualifiedName))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Applies the configured filter to the provided source sequence.
    /// </summary>
    /// <typeparam name="T">Element type of the source sequence.</typeparam>
    /// <param name="source">Sequence to filter.</param>
    /// <param name="nameSelector">Delegate extracting the fully-qualified name used for matching.</param>
    /// <returns>Filtered sequence when the filter is active; otherwise the original source sequence.</returns>
    public IEnumerable<T> Apply<T>(IEnumerable<T> source, Func<T, string> nameSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(nameSelector);

        if (!HasFilter)
        {
            return source;
        }

        return source.Where(item => Matches(nameSelector(item)));
    }

    /// <summary>
    /// Creates a new <see cref="ProcedureFilter"/> instance based on the provided filter expression.
    /// </summary>
    /// <param name="filterExpression">Comma- or semicolon-separated list of schema-qualified names or wildcard patterns.</param>
    /// <param name="console">Console service used for diagnostics.</param>
    /// <returns>The configured filter; <see cref="HasFilter"/> is <c>false</c> when no valid tokens were supplied.</returns>
    public static ProcedureFilter Create(string? filterExpression, IConsoleService console)
    {
        var exactMatches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var wildcardPatterns = new List<Regex>();

        if (!string.IsNullOrWhiteSpace(filterExpression))
        {
            var tokens = filterExpression.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(static token => token.Trim())
                .Where(static token => !string.IsNullOrEmpty(token));

            foreach (var token in tokens)
            {
                if (token.Contains('*') || token.Contains('?'))
                {
                    var escaped = Regex.Escape(token);
                    var pattern = "^" + escaped.Replace("\\*", ".*").Replace("\\?", ".") + "$";
                    try
                    {
                        wildcardPatterns.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant));
                    }
                    catch (ArgumentException ex)
                    {
                        console.Verbose($"[procedure-filter] Ignored invalid pattern '{token}': {ex.Message}");
                    }
                }
                else
                {
                    exactMatches.Add(token);
                }
            }
        }

        var hasFilter = exactMatches.Count + wildcardPatterns.Count > 0;
        return new ProcedureFilter(hasFilter, exactMatches, wildcardPatterns);
    }

    /// <summary>
    /// Returns an inactive filter instance.
    /// </summary>
    /// <returns>Filter with <see cref="HasFilter"/> set to <c>false</c>.</returns>
    public static ProcedureFilter None() => new(false, new HashSet<string>(StringComparer.OrdinalIgnoreCase), new List<Regex>());
}
