using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Xtraq.Metadata;

/// <summary>
/// Heuristic resolver that suggests result set names based on the original stored procedure T-SQL.
/// Non-blocking: returns a suggestion while letting the existing naming fallback handle the default.
/// Strategy:
/// 1. When FOR JSON PATH is detected, keep the default `ResultSet{index+1}` naming (future work may recognize explicit PATH aliases).
/// 2. Otherwise, pick the first base table or view found in the first SELECT statement.
/// 3. Fallback: return null so the caller uses the standard `ResultSet{index+1}` pattern.
/// </summary>
internal static class ResultSetNameResolver
{
    public static string? TryResolve(int index, string procedureSql)
    {
        if (string.IsNullOrWhiteSpace(procedureSql)) return null;
        try
        {
            // Fast pre-scan for dynamic SQL patterns — skip naming to avoid misleading table suggestions
            // Patterns: EXEC(@sql), sp_executesql, EXECUTE(@sql), concatenated assignment building dynamic T-SQL.
            // We intentionally keep this heuristic conservative: if any dynamic pattern is found we bail out.
            var lower = procedureSql.ToLowerInvariant();
            if (lower.Contains("sp_executesql") || lower.Contains("exec(@") || lower.Contains("execute(@") || lower.Contains("exec (") || lower.Contains("exec sp_executesql"))
            {
                return null; // force generic naming
            }
            var parser = new TSql150Parser(true);
            using var sr = new StringReader(procedureSql);
            var fragment = parser.Parse(sr, out IList<ParseError> errors);
            if (errors != null && errors.Count > 0) return null; // parsing unreliable
            // Locate the first SELECT statement
            var visitor = new FirstSelectVisitor();
            fragment.Accept(visitor);
            if (visitor.FirstSelect == null) return null;
            // Check for FOR JSON clauses
            if (visitor.FirstSelect.ForClause != null)
            {
                // Placeholder for future differentiated naming (e.g. from path); currently no alternative name.
                return null; // keep the default fallback convention
            }
            // Inspect table sources
            var table = TryGetFirstBaseTableFromSelect(visitor.FirstSelect);
            if (!string.IsNullOrWhiteSpace(table)) return table;
        }
        catch { /* Silent fallback */ }
        return null;
    }

    private static string? TryGetFirstBaseTableFromSelect(QuerySpecification qs)
    {
        foreach (var from in qs.FromClause?.TableReferences ?? new List<TableReference>())
        {
            if (from is NamedTableReference ntr)
            {
                return ntr.SchemaObject.BaseIdentifier?.Value;
            }
        }
        return null;
    }

    private sealed class FirstSelectVisitor : TSqlFragmentVisitor
    {
        public QuerySpecification? FirstSelect { get; private set; }
        public override void ExplicitVisit(QuerySpecification node)
        {
            if (FirstSelect == null) FirstSelect = node;
            // Do not traverse deeper — sufficient.
        }
    }
}

