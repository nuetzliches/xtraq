using Xtraq.Metadata;

namespace Xtraq.Diagnostics;

/// <summary>
/// Audits JSON result sets for potential weak typing (string placeholders where numeric/bool/datetime could be inferred).
/// Writes a report file under debug/json-audit.txt.
/// </summary>
public static class JsonResultSetAudit
{
    /// <summary>
    /// Captures a single suggestion produced by the JSON result set audit.
    /// </summary>
    /// <param name="Procedure">The logical operation name for the procedure containing the JSON field.</param>
    /// <param name="ResultSet">The name of the result set that exposed the weakly typed field.</param>
    /// <param name="Field">The CLR property name mapped to the JSON field.</param>
    /// <param name="SqlType">The source SQL type declared for the JSON expression.</param>
    /// <param name="ClrType">The CLR type inferred during metadata analysis.</param>
    /// <param name="Suggested">The recommended CLR type replacement for stronger typing.</param>
    public sealed record AuditFinding(string Procedure, string ResultSet, string Field, string SqlType, string ClrType, string Suggested);

    private static readonly HashSet<string> _numeric = new(StringComparer.OrdinalIgnoreCase)
    { "int","bigint","smallint","tinyint","decimal","numeric","money","smallmoney","float","real" };
    private static readonly HashSet<string> _datetime = new(StringComparer.OrdinalIgnoreCase)
    { "date","datetime","datetime2","smalldatetime","datetimeoffset","time" };
    private static readonly HashSet<string> _bool = new(StringComparer.OrdinalIgnoreCase) { "bit" };
    private static readonly HashSet<string> _guid = new(StringComparer.OrdinalIgnoreCase) { "uniqueidentifier" };

    /// <summary>
    /// Analyzes the provided procedures and returns a collection of weakly typed JSON field findings.
    /// </summary>
    /// <param name="procedures">The procedures to audit for JSON result metadata.</param>
    /// <returns>A list of audit findings highlighting candidate CLR type upgrades.</returns>
    public static IReadOnlyList<AuditFinding> Run(IEnumerable<ProcedureDescriptor> procedures)
    {
        var findings = new List<AuditFinding>();
        foreach (var p in procedures)
        {
            foreach (var rs in p.ResultSets.Where(r => r.JsonPayload != null))
            {
                foreach (var f in rs.Fields)
                {
                    var sql = f.SqlTypeName ?? string.Empty;
                    var clr = f.ClrType;
                    if (string.IsNullOrWhiteSpace(sql)) continue; // skip unknown
                    // normalize core type
                    var core = sql.ToLowerInvariant();
                    var paren = core.IndexOf('(');
                    if (paren >= 0) core = core.Substring(0, paren);
                    string? suggested = null;
                    if (_numeric.Contains(core)) suggested = core switch
                    {
                        "bigint" => "long",
                        "int" => "int",
                        "smallint" => "short",
                        "tinyint" => "byte",
                        "decimal" or "numeric" or "money" or "smallmoney" => "decimal",
                        "float" => "double",
                        "real" => "float",
                        _ => null
                    };
                    else if (_datetime.Contains(core)) suggested = core == "datetimeoffset" ? "DateTimeOffset" : core == "time" ? "TimeSpan" : "DateTime";
                    else if (_bool.Contains(core)) suggested = "bool";
                    else if (_guid.Contains(core)) suggested = "Guid";

                    if (suggested != null && clr.StartsWith("string", StringComparison.Ordinal))
                    {
                        findings.Add(new AuditFinding(p.OperationName, rs.Name, f.PropertyName, sql, clr, suggested));
                    }
                }
            }
        }
        return findings;
    }

    /// <summary>
    /// Generates a diagnostic report that summarizes JSON result set audit findings.
    /// </summary>
    /// <param name="rootDir">The root directory that hosts the <c>debug</c> folder.</param>
    /// <param name="procedures">The procedures to audit for JSON metadata.</param>
    public static void WriteReport(string rootDir, IEnumerable<ProcedureDescriptor> procedures)
    {
        var findings = Run(procedures);
        var path = Path.Combine(rootDir, "debug", "json-audit.txt");
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using var sw = new StreamWriter(path, false);
        sw.WriteLine("# JSON ResultSet Type Audit");
        sw.WriteLine("Generated: " + DateTime.UtcNow.ToString("u"));
        sw.WriteLine("Total Findings: " + findings.Count);
        foreach (var f in findings)
        {
            sw.WriteLine($"{f.Procedure}|{f.ResultSet}|{f.Field}|sql={f.SqlType}|clr={f.ClrType}|suggest={f.Suggested}");
        }
    }
}
