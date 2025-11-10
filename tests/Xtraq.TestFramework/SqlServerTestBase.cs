using System.Text.RegularExpressions;

namespace Xtraq.TestFramework;

/// <summary>
/// Helpers for SQL Server centric tests that rely on StoredProcedureContentModel parsing.
/// </summary>
public abstract class SqlServerTestBase : XtraqTestBase
{
    private static readonly Regex ProcedureHeader = new(
        @"CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+(?:(?<schema>\[[^\]]+\]|[A-Za-z0-9_]+)\.)?(?<name>\[[^\]]+\]|[A-Za-z0-9_]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Parses the given procedure definition and returns the resulting analysis.
    /// </summary>
    protected static ParsedProcedure ParseProcedure(string definition, string? defaultSchema = null)
    {
        if (string.IsNullOrWhiteSpace(definition))
        {
            throw new ArgumentException("SQL definition must not be null or empty.", nameof(definition));
        }

        var descriptor = ExtractDescriptor(definition, defaultSchema);
        var content = Xtraq.Models.StoredProcedureContentModel.Parse(definition, descriptor.Schema);
        return new ParsedProcedure(descriptor.Schema, descriptor.Name, content);
    }

    private static (string Schema, string Name) ExtractDescriptor(string definition, string? defaultSchema)
    {
        var match = ProcedureHeader.Match(definition);
        if (!match.Success)
        {
            throw new InvalidOperationException("Unable to extract procedure name from SQL definition.");
        }

        static string Unwrap(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return value.Trim().Trim('[', ']', '"');
        }

        var schema = Unwrap(match.Groups["schema"].Value);
        if (string.IsNullOrWhiteSpace(schema))
        {
            schema = string.IsNullOrWhiteSpace(defaultSchema) ? "dbo" : defaultSchema!;
        }

        var name = Unwrap(match.Groups["name"].Value);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException("Procedure name could not be resolved from SQL definition.");
        }

        return (schema, name);
    }

    /// <summary>
    /// Encapsulates the parsed procedure metadata returned by <see cref="ParseProcedure"/>.
    /// </summary>
    protected sealed class ParsedProcedure
    {
        private readonly Xtraq.Models.StoredProcedureContentModel _content;

        internal ParsedProcedure(string schema, string name, Xtraq.Models.StoredProcedureContentModel content)
        {
            Schema = schema;
            Name = name;
            _content = content ?? throw new ArgumentNullException(nameof(content));
            ResultSets = _content.ResultSets
                .Select(rs => new ResultSet(
                    rs.ReturnsJson,
                    rs.ReturnsJsonArray,
                    rs.JsonRootProperty,
                    rs.Columns?.Select(ToColumn).ToArray() ?? Array.Empty<ResultColumn>()))
                .ToArray();
        }

        public string Schema { get; }

        public string Name { get; }

        public int ParseErrorCount => _content.ParseErrorCount;

        public bool ContainsSelect => _content.ContainsSelect;

        public IReadOnlyList<ResultSet> ResultSets { get; }

        internal Xtraq.Models.StoredProcedureContentModel RawContent => _content;

        private static ResultColumn ToColumn(Xtraq.Models.StoredProcedureContentModel.ResultColumn column)
        {
            return new ResultColumn(
                column.Name,
                column.ReturnsJson,
                column.ReturnsJsonArray);
        }

        public sealed record ResultSet(bool ReturnsJson, bool ReturnsJsonArray, string? JsonRootProperty, IReadOnlyList<ResultColumn> Columns);

        public sealed record ResultColumn(string Name, bool? ReturnsJson, bool? ReturnsJsonArray);
    }
}
