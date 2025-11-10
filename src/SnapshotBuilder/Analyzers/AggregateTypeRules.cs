namespace Xtraq.SnapshotBuilder.Analyzers;

internal static class AggregateTypeRules
{
    internal readonly record struct AggregateSqlType(
        string? BaseType,
        string? FormattedType,
        int? Precision,
        int? Scale,
        int? Length,
        bool ForceNullable)
    {
        public bool HasValue => !string.IsNullOrWhiteSpace(FormattedType) || !string.IsNullOrWhiteSpace(BaseType);
    }

    internal static AggregateSqlType InferSum(string? baseType, string? formattedOperand, int? precision, int? scale, int? length)
    {
        if (string.IsNullOrWhiteSpace(baseType))
        {
            if (!string.IsNullOrWhiteSpace(formattedOperand))
            {
                return new AggregateSqlType(null, formattedOperand, precision, scale, length, true);
            }

            return default;
        }

        switch (baseType)
        {
            case "tinyint":
            case "smallint":
            case "int":
                return new AggregateSqlType("int", "int", null, null, null, true);
            case "bigint":
                return new AggregateSqlType("bigint", "bigint", null, null, null, true);
            case "decimal":
            case "numeric":
                {
                    var effectivePrecision = precision ?? 38;
                    var effectiveScale = scale ?? 0;
                    var formatted = string.Concat(baseType, "(", effectivePrecision.ToString(CultureInfo.InvariantCulture), ",", effectiveScale.ToString(CultureInfo.InvariantCulture), ")");
                    return new AggregateSqlType(baseType, formatted, effectivePrecision, effectiveScale, null, true);
                }
            case "money":
            case "smallmoney":
                return new AggregateSqlType(baseType, baseType, null, null, null, true);
            case "float":
                {
                    var effectiveLength = length.HasValue && length.Value > 0 ? length : null;
                    var formatted = effectiveLength.HasValue ? string.Concat("float(", effectiveLength.Value.ToString(CultureInfo.InvariantCulture), ")") : "float";
                    return new AggregateSqlType("float", formatted, null, null, effectiveLength, true);
                }
            case "real":
                return new AggregateSqlType("real", "real", null, null, null, true);
            default:
                return !string.IsNullOrWhiteSpace(formattedOperand)
                    ? new AggregateSqlType(baseType, formattedOperand, precision, scale, length, true)
                    : new AggregateSqlType(baseType, baseType, precision, scale, length, true);
        }
    }

    internal static AggregateSqlType InferAverage(string? baseType, string? formattedOperand, int? precision, int? scale, int? length)
    {
        if (string.IsNullOrWhiteSpace(baseType))
        {
            if (!string.IsNullOrWhiteSpace(formattedOperand))
            {
                return new AggregateSqlType(null, formattedOperand, precision, scale, length, true);
            }

            return default;
        }

        switch (baseType)
        {
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "decimal":
            case "numeric":
                return new AggregateSqlType("decimal", "decimal(38,6)", 38, 6, null, true);
            case "money":
            case "smallmoney":
                return new AggregateSqlType(baseType, baseType, null, null, null, true);
            case "float":
            case "real":
                return new AggregateSqlType("float", "float", null, null, null, true);
            default:
                return !string.IsNullOrWhiteSpace(formattedOperand)
                    ? new AggregateSqlType(baseType, formattedOperand, precision, scale, length, true)
                    : new AggregateSqlType(baseType, baseType, precision, scale, length, true);
        }
    }

    internal static AggregateSqlType InferMinMax(string? baseType, string? formattedOperand, int? precision, int? scale, int? length)
    {
        if (!string.IsNullOrWhiteSpace(formattedOperand))
        {
            return new AggregateSqlType(baseType, formattedOperand, precision, scale, length, false);
        }

        if (!string.IsNullOrWhiteSpace(baseType))
        {
            return new AggregateSqlType(baseType, baseType, precision, scale, length, false);
        }

        return default;
    }
}
