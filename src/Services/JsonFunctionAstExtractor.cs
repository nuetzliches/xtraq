using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Xtraq.Services;

// Finale saubere Implementierung
internal sealed class JsonFunctionAstExtractor
{
    private sealed class FastReader : TextReader
    { private readonly string _t; private int _p; public FastReader(string t) => _t = t ?? string.Empty; public override int Read(char[] b, int i, int c) { if (_p >= _t.Length) return 0; int k = Math.Min(c, _t.Length - _p); _t.CopyTo(_p, b, i, k); _p += k; return k; } public override int Read() => _p >= _t.Length ? -1 : _t[_p++]; }
    private sealed class QsCollectorVisitor : TSqlFragmentVisitor
    { private readonly Action<QuerySpecification> _on; public QsCollectorVisitor(Action<QuerySpecification> on) => _on = on; public override void Visit(TSqlFragment f) { if (f is QuerySpecification qs) _on(qs); base.Visit(f); } }
    private sealed class PartCollectorVisitor : TSqlFragmentVisitor
    {
        private readonly List<string> _parts;
        public PartCollectorVisitor(List<string> parts) => _parts = parts;
        public override void Visit(ColumnReferenceExpression node)
        {
            if (node?.MultiPartIdentifier != null)
            {
                foreach (var identifier in node.MultiPartIdentifier.Identifiers)
                {
                    var value = identifier?.Value;
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        _parts.Add(value!);
                    }
                }
            }
            base.Visit(node);
        }
        public override void Visit(VariableReference node)
        {
            var name = node?.Name;
            if (!string.IsNullOrWhiteSpace(name))
            {
                _parts.Add(name!.TrimStart('@'));
            }
            base.Visit(node);
        }
        public override void Visit(FunctionCall node)
        {
            var name = node?.FunctionName?.Value;
            if (!string.IsNullOrWhiteSpace(name))
            {
                _parts.Add(name!);
            }
            base.Visit(node);
        }
    }

    public JsonFunctionAstResult Parse(string sql)
    {
        var res = new JsonFunctionAstResult(); if (string.IsNullOrWhiteSpace(sql)) return res;
        var parser = new TSql160Parser(true); using var reader = new FastReader(sql); var fragment = parser.Parse(reader, out var errs); if (errs?.Count > 0) res.Errors.AddRange(errs.Select(e => e.Message));
        var specs = new List<QuerySpecification>(); fragment.Accept(new QsCollectorVisitor(q => specs.Add(q)));
        var jsonSpecs = specs.Where(q => GetForJsonClause(q) != null).ToList(); if (jsonSpecs.Count == 0) return res;
        QuerySpecification? root = jsonSpecs.FirstOrDefault(q => RootFragmentHasWithoutArrayWrapper(sql, q));
        root ??= jsonSpecs.OrderByDescending(q => q.SelectElements.OfType<SelectScalarExpression>().Count(se => se.Expression is not ScalarSubquery)).First();
        var forClause = GetForJsonClause(root)!;
        res.ReturnsJson = true;
        res.JsonRoot = GetRootName(forClause);
        var includeNullValues = GetIncludeNullValues(forClause) || RootFragmentHasIncludeNullValues(sql, root);
        res.IncludeNullValues = includeNullValues;
        bool withoutViaProperty = GetWithoutArrayWrapper(forClause);
        bool withoutViaRaw = RootFragmentHasWithoutArrayWrapper(sql, root);
        res.ReturnsJsonArray = !(withoutViaProperty || withoutViaRaw);
        foreach (var se in root.SelectElements.OfType<SelectScalarExpression>())
        {
            var alias = se.ColumnName?.Value ?? InferAlias(se.Expression);
            res.Columns.Add(BuildColumn(se.Expression, alias, 0));
        }
        if (sql.IndexOf("WITHOUT_ARRAY_WRAPPER", StringComparison.OrdinalIgnoreCase) >= 0 && res.ReturnsJsonArray)
            res.ReturnsJsonArray = false;
        return res;
    }

    private JsonFunctionAstColumn BuildColumn(ScalarExpression expr, string alias, int depth)
    {
        if (depth > 20) return new JsonFunctionAstColumn { Name = alias }; // Sicherheitsgrenze

        JsonFunctionAstColumn Make(string name, bool nested = false, bool returnsJson = false, bool? returnsJsonArray = null, bool? includeNullValues = null)
        {
            var col = new JsonFunctionAstColumn
            {
                Name = name,
                IsNestedJson = nested,
                ReturnsJson = returnsJson,
                ReturnsJsonArray = returnsJsonArray,
                JsonIncludeNullValues = includeNullValues,
                SourceSql = ExtractFragment(expr)
            };
            foreach (var p in ExtractParts(expr)) col.Parts.Add(p);
            return col;
        }

        if (expr is ScalarSubquery ss && ss.QueryExpression is QuerySpecification qs)
        {
            var fc = GetForJsonClause(qs);
            if (fc != null)
            {
                var includeNull = GetIncludeNullValues(fc);
                var col = Make(alias, nested: true, returnsJson: true, returnsJsonArray: !GetWithoutArrayWrapper(fc), includeNullValues: includeNull ? true : (bool?)null);
                foreach (var inner in qs.SelectElements.OfType<SelectScalarExpression>())
                {
                    var a = inner.ColumnName?.Value ?? InferAlias(inner.Expression);
                    col.Children.Add(BuildColumn(inner.Expression, a, depth + 1));
                }
                return col;
            }
        }
        if (expr is FunctionCall fcCall && fcCall.FunctionName?.Value?.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase) == true)
        {
            return Make(alias, nested: true, returnsJson: true);
        }
        return Make(alias);
    }

    private string? ExtractFragment(TSqlFragment frag)
    {
        // Originally, a fragment from the original SQL was extracted via an internal _currentSql field.
        // The field was removed (was unused / not set). For future use, the SQL could be passed as parameter.
        // Currently, we return no fragment to avoid warnings.
        return null; // intentionally disabled
    }

    private List<string> ExtractParts(ScalarExpression expr)
    {
        var parts = new List<string>();
        if (expr == null)
        {
            return parts;
        }

        var collector = new PartCollectorVisitor(parts);
        expr.Accept(collector);
        return parts;
    }

    private string InferAlias(ScalarExpression expr) => expr switch
    {
        ColumnReferenceExpression cr => cr.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value ?? "col",
        FunctionCall f => f.FunctionName?.Value ?? "col",
        ScalarSubquery => "col",
        _ => "col"
    };

    private object? GetForJsonClause(QuerySpecification qs) => qs?.GetType().GetProperty("ForClause")?.GetValue(qs);
    private bool GetWithoutArrayWrapper(object forJsonClause)
    { var p = forJsonClause.GetType().GetProperty("WithoutArrayWrapper"); if (p == null) return false; var v = p.GetValue(forJsonClause); return v is bool b && b; }
    private string? GetRootName(object forJsonClause)
    { var p = forJsonClause.GetType().GetProperty("RootName"); if (p == null) return null; var v = p.GetValue(forJsonClause); if (v == null) return null; var vp = v.GetType().GetProperty("Value"); return vp?.GetValue(v) as string; }
    private bool GetIncludeNullValues(object forJsonClause)
    {
        if (forJsonClause is JsonForClause jsonClause)
        {
            var options = jsonClause.Options ?? Array.Empty<JsonForClauseOption>();
            foreach (var option in options)
            {
                if (option?.OptionKind == JsonForClauseOptions.IncludeNullValues)
                {
                    return true;
                }
            }

            return false;
        }

        var optionsProperty = forJsonClause?.GetType().GetProperty("Options");
        if (optionsProperty?.GetValue(forJsonClause) is System.Collections.IEnumerable enumerable)
        {
            foreach (var option in enumerable)
            {
                var kind = option?.GetType().GetProperty("OptionKind")?.GetValue(option);
                if (kind != null && string.Equals(kind.ToString(), "IncludeNullValues", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }
    private bool RootFragmentHasWithoutArrayWrapper(string sql, QuerySpecification root)
    {
        if (root.StartOffset >= 0 && root.FragmentLength > 0 && root.StartOffset + root.FragmentLength <= sql.Length)
        {
            var frag = sql.Substring(root.StartOffset, root.FragmentLength);
            return frag.IndexOf("WITHOUT_ARRAY_WRAPPER", StringComparison.OrdinalIgnoreCase) >= 0;
        }
        return sql.IndexOf("WITHOUT_ARRAY_WRAPPER", StringComparison.OrdinalIgnoreCase) >= 0;
    }
    private bool RootFragmentHasIncludeNullValues(string sql, QuerySpecification root)
    {
        if (root.StartOffset >= 0 && root.FragmentLength > 0 && root.StartOffset + root.FragmentLength <= sql.Length)
        {
            var frag = sql.Substring(root.StartOffset, root.FragmentLength);
            return frag.IndexOf("INCLUDE_NULL_VALUES", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        return sql.IndexOf("INCLUDE_NULL_VALUES", StringComparison.OrdinalIgnoreCase) >= 0;
    }
}

internal sealed class JsonFunctionAstResult
{ public bool ReturnsJson { get; set; } public bool ReturnsJsonArray { get; set; } public bool IncludeNullValues { get; set; } public string? JsonRoot { get; set; } public List<JsonFunctionAstColumn> Columns { get; } = new(); public List<string> Errors { get; } = new(); }

internal sealed class JsonFunctionAstColumn
{
    public string Name { get; set; } = string.Empty;
    public bool IsNestedJson { get; set; }
    public bool ReturnsJson { get; set; }
    public bool? ReturnsJsonArray { get; set; }
    public bool? JsonIncludeNullValues { get; set; }
    public List<JsonFunctionAstColumn> Children { get; } = new();
    // Additional metadata for better resolution
    public string? SourceSql { get; set; }
    public List<string> Parts { get; } = new();
}
