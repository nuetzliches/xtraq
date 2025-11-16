using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Xtraq.Metadata;

/// <summary>
/// Heuristic resolver that suggests result set names based on the original stored procedure T-SQL.
/// Non-blocking: returns a suggestion while letting the existing naming fallback handle the default.
/// Strategy:
/// 1. When FOR JSON PATH is detected, reuse a declared ROOT alias when present to align JSON payload naming.
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

            var cteCollector = new CommonTableExpressionCollector();
            fragment.Accept(cteCollector);

            // Locate the first SELECT statement
            var visitor = new FirstSelectVisitor();
            fragment.Accept(visitor);
            if (visitor.FirstSelect == null) return null;
            // Check for FOR JSON clauses
            if (visitor.FirstSelect.ForClause != null)
            {
                var jsonRootAlias = TryResolveJsonRootAlias(visitor.FirstSelect.ForClause);
                if (!string.IsNullOrWhiteSpace(jsonRootAlias))
                {
                    return jsonRootAlias;
                }
                return null; // keep the default fallback convention when no alias is available
            }
            // Inspect table sources
            var table = TryGetFirstBaseTableFromSelect(visitor.FirstSelect, cteCollector.Expressions);
            if (!string.IsNullOrWhiteSpace(table)) return table;
        }
        catch { /* Silent fallback */ }
        return null;
    }

    /// <summary>
    /// Attempts to extract a ROOT alias from a FOR JSON PATH clause.
    /// Returns null when the clause does not specify a literal alias.
    /// </summary>
    /// <param name="forClause">The FOR clause attached to the SELECT statement.</param>
    private static string? TryResolveJsonRootAlias(ForClause forClause)
    {
        if (forClause is not JsonForClause jsonClause)
        {
            return null;
        }

        static object? GetPropertyValue(object target, params string[] candidateNames)
        {
            foreach (var name in candidateNames)
            {
                var property = target.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance);
                if (property != null)
                {
                    return property.GetValue(target);
                }
            }

            return null;
        }

        static string? ExtractLiteral(object? candidate)
        {
            switch (candidate)
            {
                case null:
                    return null;
                case StringLiteral stringLiteral when !string.IsNullOrWhiteSpace(stringLiteral.Value):
                    return stringLiteral.Value.Trim();
                case Identifier identifier when !string.IsNullOrWhiteSpace(identifier.Value):
                    return identifier.Value.Trim();
                case Literal literal when !string.IsNullOrWhiteSpace(literal.Value):
                    return literal.Value.Trim();
            }

            try
            {
                var valueProperty = candidate?.GetType().GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
                if (valueProperty != null && valueProperty.PropertyType == typeof(string))
                {
                    if (valueProperty.GetValue(candidate) is string str && !string.IsNullOrWhiteSpace(str))
                    {
                        return str.Trim();
                    }
                }
            }
            catch
            {
                // Reflection fallback failed; ignore and let the caller use the default naming.
            }

            return null;
        }

        var aliasCandidate = GetPropertyValue(jsonClause, "Root", "RootName", "JsonRoot");
        var alias = ExtractLiteral(aliasCandidate);

        if (!string.IsNullOrWhiteSpace(alias))
        {
            return alias;
        }

        // Some ScriptDom builds wrap the literal inside an object that exposes a nested Value property.
        alias = ExtractLiteral(GetPropertyValue(aliasCandidate ?? jsonClause, "Literal", "StringLiteral", "Identifier"));
        if (!string.IsNullOrWhiteSpace(alias))
        {
            return alias;
        }

        // Final fallback: attempt to expand nested properties up to one additional level.
        if (aliasCandidate != null)
        {
            foreach (var nested in aliasCandidate.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                alias = ExtractLiteral(nested.GetValue(aliasCandidate));
                if (!string.IsNullOrWhiteSpace(alias))
                {
                    return alias;
                }
            }
        }

        // Inspect clause options for a ROOT declaration (available on many ScriptDom builds).
        if (GetPropertyValue(jsonClause, "Options") is System.Collections.IEnumerable options)
        {
            foreach (var option in options)
            {
                if (option == null)
                {
                    continue;
                }

                var optionKind = option.GetType().GetProperty("OptionKind", BindingFlags.Public | BindingFlags.Instance)?.GetValue(option);
                if (optionKind == null)
                {
                    continue;
                }

                if (!string.Equals(optionKind.ToString(), "Root", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var value = option.GetType().GetProperty("Value", BindingFlags.Public | BindingFlags.Instance)?.GetValue(option);
                alias = ExtractLiteral(value);
                if (!string.IsNullOrWhiteSpace(alias))
                {
                    return alias;
                }
            }
        }

        return null;
    }

    private static string? TryGetFirstBaseTableFromSelect(QuerySpecification qs, IReadOnlyDictionary<string, QueryExpression> ctes)
    {
        if (qs.FromClause?.TableReferences is null)
        {
            return null;
        }

        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var tableReference in qs.FromClause.TableReferences)
        {
            var resolved = TryResolveBaseTable(tableReference, ctes, visited);
            if (!string.IsNullOrWhiteSpace(resolved))
            {
                return resolved;
            }
        }

        return null;
    }

    private static string? TryResolveBaseTable(TableReference table, IReadOnlyDictionary<string, QueryExpression> ctes, HashSet<string> visited)
    {
        switch (table)
        {
            case NamedTableReference named:
                return ResolveNamedTable(named, ctes, visited);
            case QualifiedJoin join:
                return TryResolveBaseTable(join.FirstTableReference, ctes, visited)
                       ?? TryResolveBaseTable(join.SecondTableReference, ctes, visited);
            case JoinParenthesisTableReference parenthesis:
                return TryResolveBaseTable(parenthesis.Join, ctes, visited);
            case PivotedTableReference pivot:
                return TryResolveBaseTable(pivot.TableReference, ctes, visited);
            case QueryDerivedTable derived:
                return TryResolveQueryExpression(derived.QueryExpression, ctes, visited);
            case SchemaObjectFunctionTableReference functionReference:
                return functionReference.SchemaObject.BaseIdentifier?.Value;
            default:
                return null;
        }
    }

    private static string? ResolveNamedTable(NamedTableReference named, IReadOnlyDictionary<string, QueryExpression> ctes, HashSet<string> visited)
    {
        var schemaObject = named.SchemaObject;
        if (schemaObject is null)
        {
            return null;
        }

        if (schemaObject.DatabaseIdentifier != null || schemaObject.SchemaIdentifier != null)
        {
            return schemaObject.BaseIdentifier?.Value;
        }

        var identifier = schemaObject.BaseIdentifier?.Value;
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return null;
        }

        if (ctes.TryGetValue(identifier, out var cteExpression))
        {
            if (!visited.Add(identifier))
            {
                return null;
            }

            var resolved = TryResolveQueryExpression(cteExpression, ctes, visited);
            visited.Remove(identifier);
            return resolved;
        }

        return identifier;
    }

    private static string? TryResolveQueryExpression(QueryExpression? query, IReadOnlyDictionary<string, QueryExpression> ctes, HashSet<string> visited)
    {
        switch (query)
        {
            case QuerySpecification qs:
                return TryGetFirstBaseTableFromSelect(qs, ctes);
            case BinaryQueryExpression binary:
                return TryResolveQueryExpression(binary.FirstQueryExpression, ctes, visited)
                       ?? TryResolveQueryExpression(binary.SecondQueryExpression, ctes, visited);
            case QueryParenthesisExpression parenthesis:
                return TryResolveQueryExpression(parenthesis.QueryExpression, ctes, visited);
            default:
                return null;
        }
    }

    private sealed class FirstSelectVisitor : TSqlFragmentVisitor
    {
        public QuerySpecification? FirstSelect { get; private set; }
        private int _cteDepth;

        public override void ExplicitVisit(CommonTableExpression node)
        {
            _cteDepth++;
            base.ExplicitVisit(node);
            _cteDepth--;
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            if (FirstSelect == null && _cteDepth == 0)
            {
                FirstSelect = node;
            }
        }
    }

    private sealed class CommonTableExpressionCollector : TSqlFragmentVisitor
    {
        private readonly Dictionary<string, QueryExpression> _expressions = new(StringComparer.OrdinalIgnoreCase);

        public IReadOnlyDictionary<string, QueryExpression> Expressions => _expressions;

        public override void ExplicitVisit(CommonTableExpression node)
        {
            if (!string.IsNullOrWhiteSpace(node.ExpressionName?.Value) && node.QueryExpression != null)
            {
                var name = node.ExpressionName.Value;
                if (!_expressions.ContainsKey(name))
                {
                    _expressions[name] = node.QueryExpression;
                }
            }

            base.ExplicitVisit(node);
        }
    }
}

