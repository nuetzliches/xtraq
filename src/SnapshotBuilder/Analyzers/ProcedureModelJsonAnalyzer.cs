using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Derives JSON-specific metadata for <see cref="ProcedureModel"/> instances by re-parsing the SQL definition with ScriptDom.
/// </summary>
internal static class ProcedureModelJsonAnalyzer
{
    private sealed record JsonProjectionInfo(
        bool ReturnsJson,
        bool? ReturnsJsonArray,
        string? RootProperty,
        bool? IncludeNullValues,
        bool IsNested,
        bool IsJsonFunction,
        bool WithoutArrayWrapper,
        bool? SingleRowGuaranteed);

    public static void Apply(string? definition, ProcedureModel? model)
    {
        var fragment = ProcedureModelScriptDomParser.Parse(definition);
        Apply(fragment, model, definition);
    }

    public static void Apply(TSqlFragment? fragment, ProcedureModel? model)
    {
        Apply(fragment, model, null);
    }

    public static void Apply(TSqlFragment? fragment, ProcedureModel? model, string? definition)
    {
        if (fragment == null || model == null)
        {
            return;
        }

        var visitor = new JsonVisitor(definition);
        fragment.Accept(visitor);

        if (model.ResultSets.Count > 0 && visitor.TopLevelJson.Count > 0)
        {
            foreach (var (index, info) in visitor.TopLevelJson)
            {
                if (index < 0 || index >= model.ResultSets.Count)
                {
                    continue;
                }

                var resultSet = model.ResultSets[index];
                resultSet.ReturnsJson = info.ReturnsJson;
                if (info.ReturnsJsonArray.HasValue)
                {
                    resultSet.ReturnsJsonArray = info.ReturnsJsonArray.Value;
                }

                if (!string.IsNullOrWhiteSpace(info.RootProperty) && string.IsNullOrWhiteSpace(resultSet.JsonRootProperty))
                {
                    resultSet.JsonRootProperty = info.RootProperty;
                }

                if (info.IncludeNullValues == true)
                {
                    resultSet.JsonIncludeNullValues = true;
                }

                if (info.WithoutArrayWrapper)
                {
                    resultSet.JsonSingleRowGuaranteed = info.SingleRowGuaranteed;
                }
            }
        }

        if (visitor.NestedJson.Count == 0)
        {
            return;
        }

        foreach (var resultSet in model.ResultSets)
        {
            ApplyNested(resultSet.Columns, visitor.NestedJson);
        }
    }

    private static void ApplyNested(IReadOnlyList<ProcedureResultColumn> columns, IReadOnlyDictionary<string, JsonProjectionInfo> map)
    {
        if (columns == null)
        {
            return;
        }

        foreach (var column in columns)
        {
            if (column == null)
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(column.Name))
            {
                var normalized = NormalizeAlias(column.Name);
                if (map.TryGetValue(normalized, out var info))
                {
                    column.ReturnsJson = info.ReturnsJson;
                    if (info.IsNested)
                    {
                        column.IsNestedJson = true;
                    }

                    if (info.ReturnsJsonArray.HasValue)
                    {
                        column.ReturnsJsonArray = info.ReturnsJsonArray.Value;
                    }

                    if (!string.IsNullOrWhiteSpace(info.RootProperty) && string.IsNullOrWhiteSpace(column.JsonRootProperty))
                    {
                        column.JsonRootProperty = info.RootProperty;
                    }

                    if (info.IncludeNullValues == true)
                    {
                        column.JsonIncludeNullValues = true;
                    }

                    if (info.IsJsonFunction)
                    {
                        if (HasStructuredJsonMetadata(column) ||
                            (column.Reference != null &&
                             column.Reference.Kind == ProcedureReferenceKind.Function &&
                             !string.Equals(column.Reference.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase)))
                        {
                            column.ReturnsUnknownJson = false;
                        }
                        else if (!column.ReturnsUnknownJson.HasValue)
                        {
                            column.ReturnsUnknownJson = true;
                        }
                    }

                    if (info.WithoutArrayWrapper)
                    {
                        column.JsonSingleRowGuaranteed = info.SingleRowGuaranteed;
                    }
                }
            }

            if (column.Columns != null && column.Columns.Count > 0)
            {
                ApplyNested(column.Columns, map);
            }
        }
    }

    private static string NormalizeAlias(string alias)
    {
        if (string.IsNullOrWhiteSpace(alias))
        {
            return string.Empty;
        }

        var trimmed = alias.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '[' && trimmed[^1] == ']')
        {
            return trimmed.Substring(1, trimmed.Length - 2);
        }

        return trimmed;
    }

    private static bool HasStructuredJsonMetadata(ProcedureResultColumn? column)
    {
        return column?.Columns != null && column.Columns.Count > 0;
    }

    private sealed class JsonVisitor : TSqlFragmentVisitor
    {
        private static readonly Regex RootRegex = new("ROOT\\s*\\(\\s*'([^']+)'", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private readonly string? _definition;
        private int _queryDepth;
        private int _scalarSubqueryDepth;
        private int _topLevelQueryIndex;

        public JsonVisitor(string? definition)
        {
            _definition = definition;
        }

        public Dictionary<int, JsonProjectionInfo> TopLevelJson { get; } = new();
        public Dictionary<string, JsonProjectionInfo> NestedJson { get; } = new(StringComparer.OrdinalIgnoreCase);

        public override void ExplicitVisit(ScalarSubquery node)
        {
            _scalarSubqueryDepth++;
            base.ExplicitVisit(node);
            _scalarSubqueryDepth--;
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            var isTopLevel = _scalarSubqueryDepth == 0 && _queryDepth == 0;
            var producesResult = ProducesVisibleResult(node);
            _queryDepth++;

            if (isTopLevel && producesResult)
            {
                var info = ExtractForJson(node);
                if (info != null)
                {
                    TopLevelJson[_topLevelQueryIndex] = info;
                }

                _topLevelQueryIndex++;
            }
            else if (isTopLevel)
            {
                _ = ExtractForJson(node);
            }

            base.ExplicitVisit(node);
            _queryDepth--;
        }

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            var alias = node.ColumnName?.Value;
            if (!string.IsNullOrWhiteSpace(alias) && IsCurrentProjectionScope())
            {
                JsonProjectionInfo? info = null;

                if (node.Expression is ScalarSubquery subquery && subquery.QueryExpression is QuerySpecification qs)
                {
                    info = ExtractForJson(qs, isNested: true);
                    if (info != null)
                    {
                        info = info with { IsNested = true };
                    }
                }
                else if (node.Expression is FunctionCall call && IsJsonQuery(call))
                {
                    info = new JsonProjectionInfo(true, null, null, null, true, true, false, null);
                }

                if (info != null)
                {
                    var normalized = NormalizeAlias(alias);
                    if (!NestedJson.ContainsKey(normalized))
                    {
                        NestedJson[normalized] = info;
                    }
                }
            }

            base.ExplicitVisit(node);
        }

        private bool IsCurrentProjectionScope()
        {
            if (_queryDepth <= 0)
            {
                return false;
            }

            return _queryDepth == _scalarSubqueryDepth + 1;
        }

        private static bool IsJsonQuery(FunctionCall call)
        {
            return string.Equals(call?.FunctionName?.Value, "JSON_QUERY", StringComparison.OrdinalIgnoreCase);
        }

        private JsonProjectionInfo? ExtractForJson(QuerySpecification node, bool isNested = false)
        {
            if (node.ForClause is JsonForClause jsonClause)
            {
                bool withoutArrayWrapper = false;
                string? root = null;
                bool? returnsArray = null;
                var includeNulls = false;

                var options = jsonClause.Options ?? Array.Empty<JsonForClauseOption>();
                if (options.Count == 0)
                {
                    returnsArray = true;
                }

                foreach (var option in options)
                {
                    switch (option.OptionKind)
                    {
                        case JsonForClauseOptions.WithoutArrayWrapper:
                            withoutArrayWrapper = true;
                            break;
                        case JsonForClauseOptions.Root:
                            if (root == null && option.Value is Literal literal)
                            {
                                root = ExtractLiteralValue(literal);
                            }

                            break;
                        case JsonForClauseOptions.IncludeNullValues:
                            includeNulls = true;
                            break;
                        default:
                            returnsArray ??= true;
                            break;
                    }
                }

                JsonProjectionInfo? fallback = null;
                if (root == null || !returnsArray.HasValue || !includeNulls)
                {
                    fallback = ExtractViaSegment(node, isNested);
                }

                var array = withoutArrayWrapper
                    ? false
                    : (returnsArray ?? fallback?.ReturnsJsonArray ?? true);
                var effectiveRoot = root ?? fallback?.RootProperty;
                var includeNullValues = includeNulls ? true : fallback?.IncludeNullValues;
                var effectiveWithoutArray = withoutArrayWrapper || fallback?.WithoutArrayWrapper == true;
                bool? singleRowGuaranteed = null;
                if (effectiveWithoutArray)
                {
                    var guarantee = DetermineSingleRowGuarantee(node);
                    singleRowGuaranteed = guarantee ?? fallback?.SingleRowGuaranteed;
                }

                return new JsonProjectionInfo(true, array, effectiveRoot, includeNullValues, isNested, false, effectiveWithoutArray, singleRowGuaranteed);
            }

            return ExtractViaSegment(node, isNested);
        }

        private JsonProjectionInfo? ExtractViaSegment(TSqlFragment fragment, bool isNested)
        {
            if (string.IsNullOrEmpty(_definition))
            {
                return null;
            }

            var segment = GetSegment(fragment);
            if (string.IsNullOrWhiteSpace(segment))
            {
                return null;
            }

            if (segment.IndexOf("FOR JSON", StringComparison.OrdinalIgnoreCase) < 0)
            {
                return null;
            }

            var withoutArray = segment.IndexOf("WITHOUT_ARRAY_WRAPPER", StringComparison.OrdinalIgnoreCase) >= 0;
            string? root = null;
            var match = RootRegex.Match(segment);
            if (match.Success && match.Groups.Count > 1)
            {
                root = match.Groups[1].Value;
            }

            var includeNulls = segment.IndexOf("INCLUDE_NULL_VALUES", StringComparison.OrdinalIgnoreCase) >= 0;
            var returnsArray = withoutArray ? false : true;
            return new JsonProjectionInfo(true, returnsArray, root, includeNulls ? true : null, isNested, false, withoutArray, null);
        }

        private string GetSegment(TSqlFragment fragment)
        {
            if (fragment == null || fragment.StartOffset < 0 || fragment.FragmentLength <= 0 || string.IsNullOrEmpty(_definition))
            {
                return string.Empty;
            }

            var start = Math.Max(0, fragment.StartOffset);
            var end = fragment.StartOffset >= 0 && fragment.FragmentLength > 0
                ? Math.Min(_definition!.Length, fragment.StartOffset + fragment.FragmentLength + 200)
                : _definition!.Length;
            if (end <= start)
            {
                return string.Empty;
            }

            return _definition!.Substring(start, end - start);
        }

        private static bool? DetermineSingleRowGuarantee(QuerySpecification node)
        {
            if (node == null)
            {
                return null;
            }

            if (HasTopOne(node))
            {
                return true;
            }

            if (node.FromClause == null)
            {
                return true;
            }

            if (node.GroupByClause != null)
            {
                return false;
            }

            if (ContainsTopLevelAggregate(node))
            {
                return true;
            }

            return null;
        }

        private static bool ContainsSelectStar(QuerySpecification node)
        {
            if (node?.SelectElements == null)
            {
                return false;
            }

            foreach (var element in node.SelectElements)
            {
                if (element is SelectStarExpression)
                {
                    return true;
                }
            }

            return false;
        }

        private static bool HasTopOne(QuerySpecification node)
        {
            if (node.TopRowFilter is not TopRowFilter top || top.Percent || top.WithTies)
            {
                return false;
            }

            var value = EvaluateTopExpression(top.Expression);
            return value == 1;
        }

        private static int? EvaluateTopExpression(ScalarExpression? expression)
        {
            switch (expression)
            {
                case IntegerLiteral integerLiteral when int.TryParse(integerLiteral.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                    return parsed;
                case Literal literal when !string.IsNullOrWhiteSpace(literal.Value) && int.TryParse(literal.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var literalParsed):
                    return literalParsed;
                case ParenthesisExpression parenthesis:
                    return EvaluateTopExpression(parenthesis.Expression);
                case UnaryExpression unary when unary.UnaryExpressionType is UnaryExpressionType.Positive or UnaryExpressionType.Negative:
                    var inner = EvaluateTopExpression(unary.Expression);
                    if (!inner.HasValue)
                    {
                        return null;
                    }

                    return unary.UnaryExpressionType == UnaryExpressionType.Negative ? -inner.Value : inner.Value;
            }

            return null;
        }

        private static bool ContainsTopLevelAggregate(QuerySpecification node)
        {
            if (node?.SelectElements == null)
            {
                return false;
            }

            foreach (var element in node.SelectElements)
            {
                if (element is not SelectScalarExpression scalar)
                {
                    continue;
                }

                if (IsAggregateExpression(scalar.Expression))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsAggregateExpression(ScalarExpression? expression)
        {
            switch (expression)
            {
                case null:
                    return false;
                case FunctionCall functionCall when functionCall.OverClause == null && AggregateFunctionCatalog.IsAggregateName(functionCall.FunctionName?.Value):
                    return true;
                case FunctionCall functionCall:
                    if (functionCall.Parameters != null)
                    {
                        foreach (var parameter in functionCall.Parameters.OfType<ScalarExpression>())
                        {
                            if (IsAggregateExpression(parameter))
                            {
                                return true;
                            }
                        }
                    }

                    return false;
                case CastCall castCall:
                    return IsAggregateExpression(castCall.Parameter);
                case ConvertCall convertCall:
                    if (IsAggregateExpression(convertCall.Parameter))
                    {
                        return true;
                    }

                    return convertCall.Style is ScalarExpression style && IsAggregateExpression(style);
                case TryCastCall tryCastCall:
                    return IsAggregateExpression(tryCastCall.Parameter);
                case TryConvertCall tryConvertCall:
                    if (IsAggregateExpression(tryConvertCall.Parameter))
                    {
                        return true;
                    }

                    return tryConvertCall.Style is ScalarExpression tryConvertStyle && IsAggregateExpression(tryConvertStyle);
                case IIfCall iifCall:
                    return IsAggregateExpression(iifCall.ThenExpression) || IsAggregateExpression(iifCall.ElseExpression);
                case SimpleCaseExpression simpleCase:
                    if (simpleCase.InputExpression != null && IsAggregateExpression(simpleCase.InputExpression))
                    {
                        return true;
                    }

                    foreach (var clause in simpleCase.WhenClauses)
                    {
                        if (clause?.ThenExpression != null && IsAggregateExpression(clause.ThenExpression))
                        {
                            return true;
                        }
                    }

                    return simpleCase.ElseExpression != null && IsAggregateExpression(simpleCase.ElseExpression);
                case SearchedCaseExpression searchedCase:
                    foreach (var clause in searchedCase.WhenClauses)
                    {
                        if (clause?.ThenExpression != null && IsAggregateExpression(clause.ThenExpression))
                        {
                            return true;
                        }
                    }

                    return searchedCase.ElseExpression != null && IsAggregateExpression(searchedCase.ElseExpression);
                case BinaryExpression binaryExpression:
                    return IsAggregateExpression(binaryExpression.FirstExpression) || IsAggregateExpression(binaryExpression.SecondExpression);
                case CoalesceExpression coalesceExpression:
                    if (coalesceExpression.Expressions != null)
                    {
                        foreach (var expr in coalesceExpression.Expressions)
                        {
                            if (IsAggregateExpression(expr))
                            {
                                return true;
                            }
                        }
                    }

                    return false;
                case NullIfExpression nullIfExpression:
                    return IsAggregateExpression(nullIfExpression.FirstExpression) || IsAggregateExpression(nullIfExpression.SecondExpression);
                case ParenthesisExpression parenthesisExpression:
                    return IsAggregateExpression(parenthesisExpression.Expression);
                case ScalarSubquery:
                    return false;
                default:
                    return false;
            }
        }

        private static string? ExtractLiteralValue(Literal literal)
        {
            return literal switch
            {
                StringLiteral sl when !string.IsNullOrWhiteSpace(sl.Value) => sl.Value,
                IntegerLiteral il when !string.IsNullOrWhiteSpace(il.Value) => il.Value,
                NumericLiteral nl when !string.IsNullOrWhiteSpace(nl.Value) => nl.Value,
                _ => null
            };
        }

        private static bool ProducesVisibleResult(QuerySpecification? node)
        {
            if (node?.SelectElements == null || node.SelectElements.Count == 0)
            {
                return false;
            }

            foreach (var element in node.SelectElements)
            {
                if (element is SelectScalarExpression || element is SelectStarExpression)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
