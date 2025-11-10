using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Applies lightweight normalizations to the intermediate procedure model until dedicated analyzers replace the legacy parser.
/// </summary>
internal static class ProcedureModelPostProcessor
{

    public static void Apply(ProcedureModel? model)
    {
        if (model == null)
        {
            return;
        }

        foreach (var resultSet in model.ResultSets)
        {
            if (resultSet?.Columns == null) continue;
            foreach (var column in resultSet.Columns)
            {
                NormalizeColumn(column);
            }
        }
    }

    private static void NormalizeColumn(ProcedureResultColumn? column)
    {
        if (column == null)
        {
            return;
        }

        if (column.Columns != null && column.Columns.Count > 0)
        {
            foreach (var child in column.Columns)
            {
                NormalizeColumn(child);
            }
        }

        EnsureAggregate(column);
    }

    private static void EnsureAggregate(ProcedureResultColumn column)
    {
        var aggregateFunction = NormalizeAggregateName(column.AggregateFunction);
        var isAggregate = column.IsAggregate || aggregateFunction != null;

        if (!isAggregate)
        {
            aggregateFunction = DetectAggregate(column.RawExpression) ?? aggregateFunction;
            if (aggregateFunction != null)
            {
                isAggregate = true;
            }
        }
        else if (aggregateFunction == null)
        {
            aggregateFunction = DetectAggregate(column.RawExpression);
        }

        column.IsAggregate = isAggregate;
        if (aggregateFunction != null)
        {
            column.AggregateFunction = aggregateFunction;
        }

        if (column.IsAggregate)
        {
            ApplyAggregateTypeHeuristics(column);
        }
    }

    private static string? NormalizeAggregateName(string? aggregate)
    {
        return AggregateFunctionCatalog.NormalizeName(aggregate);
    }

    private static string? DetectAggregate(string? rawExpression)
    {
        return AggregateFunctionCatalog.DetectInExpression(rawExpression);
    }

    private static void ApplyAggregateTypeHeuristics(ProcedureResultColumn column)
    {
        if (string.IsNullOrWhiteSpace(column.AggregateFunction))
        {
            return;
        }

        if (!string.IsNullOrWhiteSpace(column.SqlTypeName))
        {
            ApplyDefaultAggregateNullability(column);
            return;
        }

        switch (column.AggregateFunction)
        {
            case "count":
                column.SqlTypeName = "int";
                column.IsNullable ??= false;
                break;
            case "count_big":
                column.SqlTypeName = "bigint";
                column.IsNullable ??= false;
                break;
            case "exists":
                column.SqlTypeName = "bit";
                column.IsNullable ??= false;
                break;
            case "avg":
                if (TryApplyAggregateInference(column, AggregateTypeRules.InferAverage))
                {
                    column.IsNullable ??= true;
                    break;
                }

                column.SqlTypeName ??= "decimal(38,6)";
                column.CastTargetType ??= "decimal";
                column.CastTargetPrecision ??= 38;
                column.CastTargetScale ??= 6;
                column.IsNullable ??= true;
                break;
            case "sum":
                if (TryApplyAggregateInference(column, AggregateTypeRules.InferSum))
                {
                    column.IsNullable ??= true;
                    break;
                }

                if (column.HasIntegerLiteral && !column.HasDecimalLiteral)
                {
                    column.SqlTypeName ??= "int";
                    column.CastTargetType ??= "int";
                }
                else if (column.HasDecimalLiteral)
                {
                    column.SqlTypeName ??= "decimal(38,10)";
                    column.CastTargetType ??= "decimal";
                    column.CastTargetPrecision ??= 38;
                    column.CastTargetScale ??= 10;
                }
                else
                {
                    column.SqlTypeName ??= "decimal(38,10)";
                    column.CastTargetType ??= "decimal";
                    column.CastTargetPrecision ??= 38;
                    column.CastTargetScale ??= 10;
                }

                column.IsNullable ??= true;
                break;
            case "min":
            case "max":
                if (TryApplyAggregateInference(column, AggregateTypeRules.InferMinMax))
                {
                    break;
                }

                if (column.HasIntegerLiteral && !column.HasDecimalLiteral)
                {
                    column.SqlTypeName ??= "int";
                    column.CastTargetType ??= "int";
                }
                else if (column.HasDecimalLiteral)
                {
                    column.SqlTypeName ??= "decimal(38,6)";
                    column.CastTargetType ??= "decimal";
                    column.CastTargetPrecision ??= 38;
                    column.CastTargetScale ??= 6;
                }
                break;
        }
    }

    private static void ApplyDefaultAggregateNullability(ProcedureResultColumn column)
    {
        if (string.IsNullOrWhiteSpace(column.AggregateFunction))
        {
            return;
        }

        switch (column.AggregateFunction)
        {
            case "count":
            case "count_big":
            case "exists":
                column.IsNullable ??= false;
                break;
            case "sum":
            case "avg":
                column.IsNullable ??= true;
                break;
        }
    }

    private static bool TryApplyAggregateInference(
        ProcedureResultColumn column,
        Func<string?, string?, int?, int?, int?, AggregateTypeRules.AggregateSqlType> infer)
    {
        if (column == null)
        {
            return false;
        }

        var baseType = NormalizeType(column.CastTargetType);
        var precision = column.CastTargetPrecision;
        var scale = column.CastTargetScale;
        var length = column.CastTargetLength ?? column.MaxLength;
        var formattedOperand = FormatOperand(baseType, precision, scale, length);
        var inference = infer(baseType, formattedOperand, precision, scale, length);

        if (!inference.HasValue)
        {
            return false;
        }

        column.SqlTypeName ??= inference.FormattedType ?? inference.BaseType;

        if (!string.IsNullOrWhiteSpace(inference.BaseType))
        {
            column.CastTargetType ??= inference.BaseType;
        }

        if (inference.Precision.HasValue)
        {
            column.CastTargetPrecision ??= inference.Precision;
        }

        if (inference.Scale.HasValue)
        {
            column.CastTargetScale ??= inference.Scale;
        }

        if (inference.Length.HasValue)
        {
            column.CastTargetLength ??= inference.Length;
            column.MaxLength ??= inference.Length;
        }

        if (inference.ForceNullable)
        {
            column.IsNullable ??= true;
        }

        return true;
    }

    private static string? NormalizeType(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
        {
            return null;
        }

        var trimmed = typeName.Trim().ToLowerInvariant();
        var parenIndex = trimmed.IndexOf('(');
        return parenIndex > 0 ? trimmed[..parenIndex] : trimmed;
    }

    private static string? FormatOperand(string? baseType, int? precision, int? scale, int? length)
    {
        if (string.IsNullOrWhiteSpace(baseType))
        {
            return null;
        }

        return baseType switch
        {
            "decimal" or "numeric" => FormatDecimal(baseType, precision, scale),
            "float" => FormatFloat(baseType, length),
            _ => !length.HasValue || length.Value <= 0
                ? baseType
                : string.Concat(baseType, "(", length.Value.ToString(CultureInfo.InvariantCulture), ")")
        };
    }

    private static string FormatFloat(string baseType, int? length)
    {
        if (!length.HasValue || length.Value <= 0)
        {
            return baseType;
        }

        return string.Concat(baseType, "(", length.Value.ToString(CultureInfo.InvariantCulture), ")");
    }

    private static string? FormatDecimal(string baseType, int? precision, int? scale)
    {
        if (!precision.HasValue)
        {
            return null;
        }

        var effectiveScale = Math.Max(scale ?? 0, 0);
        return string.Concat(baseType, "(", precision.Value.ToString(CultureInfo.InvariantCulture), ",", effectiveScale.ToString(CultureInfo.InvariantCulture), ")");
    }
}
