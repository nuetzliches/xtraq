using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.Data.Models;
using Xtraq.SnapshotBuilder.Writers;

namespace Xtraq.SnapshotBuilder.Analyzers;

internal static class ProcedureModelParameterDefaultAnalyzer
{
    public static void Apply(TSqlFragment fragment, IReadOnlyList<StoredProcedureInput> parameters)
    {
        ArgumentNullException.ThrowIfNull(fragment);
        if (parameters == null || parameters.Count == 0)
        {
            return;
        }

        var visitor = new DefaultVisitor(parameters);
        fragment.Accept(visitor);
    }

    private sealed class DefaultVisitor : TSqlFragmentVisitor
    {
        private readonly Dictionary<string, StoredProcedureInput> _parameterMap;

        public DefaultVisitor(IReadOnlyList<StoredProcedureInput> parameters)
        {
            _parameterMap = new Dictionary<string, StoredProcedureInput>(StringComparer.OrdinalIgnoreCase);

            foreach (var prm in parameters)
            {
                if (prm == null || string.IsNullOrWhiteSpace(prm.Name))
                {
                    continue;
                }

                var raw = prm.Name.Trim();
                if (!_parameterMap.ContainsKey(raw))
                {
                    _parameterMap[raw] = prm;
                }

                var normalized = SnapshotWriterUtilities.NormalizeParameterName(raw);
                if (!string.IsNullOrWhiteSpace(normalized) && !_parameterMap.ContainsKey(normalized!))
                {
                    _parameterMap[normalized!] = prm;
                }

                if (raw.StartsWith("@", StringComparison.Ordinal) && raw.Length > 1)
                {
                    var withoutAt = raw.Substring(1);
                    if (!_parameterMap.ContainsKey(withoutAt))
                    {
                        _parameterMap[withoutAt] = prm;
                    }
                }
            }
        }

        public override void ExplicitVisit(CreateProcedureStatement node)
        {
            ApplyDefaults(node?.Parameters);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(AlterProcedureStatement node)
        {
            ApplyDefaults(node?.Parameters);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(CreateOrAlterProcedureStatement node)
        {
            ApplyDefaults(node?.Parameters);
            base.ExplicitVisit(node);
        }

        private void ApplyDefaults(IList<ProcedureParameter>? parameters)
        {
            if (parameters == null || parameters.Count == 0)
            {
                return;
            }

            foreach (var p in parameters)
            {
                if (p?.VariableName == null)
                {
                    continue;
                }

                var rawName = p.VariableName.Value;
                if (string.IsNullOrWhiteSpace(rawName))
                {
                    continue;
                }

                var normalized = SnapshotWriterUtilities.NormalizeParameterName(rawName);

                if (!TryGetTarget(rawName, normalized, out var target) || target == null)
                {
                    continue;
                }

                // ScriptDom uses Value for the optional default expression.
                if (p.Value == null)
                {
                    continue;
                }

                target.HasDefaultValue = true;

                if (TryEvaluateLiteralDefault(p.Value, out var literalValue, out var isLiteralNull))
                {
                    target.DefaultValue = isLiteralNull ? null : literalValue;
                }
            }
        }

        private bool TryGetTarget(string rawName, string? normalized, out StoredProcedureInput? target)
        {
            if (_parameterMap.TryGetValue(rawName, out target))
            {
                return true;
            }

            if (!string.IsNullOrWhiteSpace(normalized) && _parameterMap.TryGetValue(normalized!, out target))
            {
                return true;
            }

            if (rawName.StartsWith("@", StringComparison.Ordinal) && rawName.Length > 1)
            {
                var withoutAt = rawName.Substring(1);
                if (_parameterMap.TryGetValue(withoutAt, out target))
                {
                    return true;
                }
            }

            target = null;
            return false;
        }

        private static bool TryEvaluateLiteralDefault(ScalarExpression expression, out object? value, out bool isNull)
        {
            ArgumentNullException.ThrowIfNull(expression);

            isNull = false;
            value = null;

            switch (expression)
            {
                case NullLiteral:
                    isNull = true;
                    return true;

                case IntegerLiteral integerLiteral:
                    if (int.TryParse(integerLiteral.Value, out var intValue))
                    {
                        value = intValue;
                        return true;
                    }

                    if (long.TryParse(integerLiteral.Value, out var longValue))
                    {
                        value = longValue;
                        return true;
                    }

                    return false;

                case NumericLiteral numericLiteral:
                    if (decimal.TryParse(numericLiteral.Value, System.Globalization.NumberStyles.Number, System.Globalization.CultureInfo.InvariantCulture, out var decimalValue))
                    {
                        value = decimalValue;
                        return true;
                    }

                    return false;

                case MoneyLiteral moneyLiteral:
                    if (decimal.TryParse(moneyLiteral.Value, System.Globalization.NumberStyles.Number, System.Globalization.CultureInfo.InvariantCulture, out var moneyValue))
                    {
                        value = moneyValue;
                        return true;
                    }

                    return false;

                case StringLiteral stringLiteral:
                    value = stringLiteral.Value;
                    return true;

                case UnaryExpression unaryExpression:
                    // Common pattern: = -1
                    if (unaryExpression.UnaryExpressionType == UnaryExpressionType.Negative
                        && unaryExpression.Expression != null
                        && TryEvaluateLiteralDefault(unaryExpression.Expression, out var inner, out var innerIsNull)
                        && !innerIsNull)
                    {
                        if (inner is int i)
                        {
                            value = -i;
                            return true;
                        }

                        if (inner is long l)
                        {
                            value = -l;
                            return true;
                        }

                        if (inner is decimal d)
                        {
                            value = -d;
                            return true;
                        }
                    }

                    return false;

                default:
                    return false;
            }
        }
    }
}
