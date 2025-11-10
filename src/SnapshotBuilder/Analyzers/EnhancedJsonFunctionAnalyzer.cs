using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Enhanced JSON function analyzer that properly handles JSON_VALUE wrapper detection
/// and provides accurate function references for scalar function calls within JSON contexts.
/// </summary>
internal sealed class EnhancedJsonFunctionAnalyzer : TSqlFragmentVisitor
{
    private readonly Dictionary<string, JsonFunctionInfo> _jsonFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _processedColumns = new(StringComparer.OrdinalIgnoreCase);

    internal IReadOnlyDictionary<string, JsonFunctionInfo> JsonFunctions => _jsonFunctions;

    public override void ExplicitVisit(SelectElement node)
    {
        if (node is SelectScalarExpression scalarExpr)
        {
            AnalyzeSelectScalarExpression(scalarExpr);
        }

        base.ExplicitVisit(node);
    }

    private void AnalyzeSelectScalarExpression(SelectScalarExpression scalarExpr)
    {
        var columnName = GetColumnName(scalarExpr);
        if (string.IsNullOrEmpty(columnName) || _processedColumns.Contains(columnName))
        {
            return;
        }

        _processedColumns.Add(columnName);

        var functionInfo = AnalyzeExpression(scalarExpr.Expression);
        if (functionInfo != null)
        {
            functionInfo.ColumnName = columnName;
            _jsonFunctions[columnName] = functionInfo;
        }
    }

    private JsonFunctionInfo? AnalyzeExpression(ScalarExpression expression)
    {
        return expression switch
        {
            FunctionCall functionCall => AnalyzeFunctionCall(functionCall),
            ParenthesisExpression parenthesis => AnalyzeExpression(parenthesis.Expression),
            CastCall castCall => AnalyzeExpression(castCall.Parameter),
            ConvertCall convertCall => AnalyzeExpression(convertCall.Parameter),
            _ => null
        };
    }

    private JsonFunctionInfo? AnalyzeFunctionCall(FunctionCall functionCall)
    {
        var functionName = functionCall.FunctionName?.Value;
        if (string.IsNullOrEmpty(functionName))
        {
            return null;
        }

        // Handle JSON built-in functions
        if (IsJsonBuiltInFunction(functionName))
        {
            return AnalyzeJsonBuiltInFunction(functionCall);
        }

        // Handle user-defined functions that might return JSON
        if (functionCall.Parameters.Count > 0)
        {
            var info = new JsonFunctionInfo
            {
                FunctionName = functionName,
                SchemaName = ExtractSchemaFromFunction(functionCall),
                IsBuiltInJsonFunction = false,
                Parameters = ExtractParameters(functionCall)
            };

            return info;
        }

        return null;
    }

    private JsonFunctionInfo? AnalyzeJsonBuiltInFunction(FunctionCall functionCall)
    {
        var functionName = functionCall.FunctionName?.Value?.ToUpperInvariant();

        switch (functionName)
        {
            case "JSON_QUERY":
                return new JsonFunctionInfo
                {
                    FunctionName = "JSON_QUERY",
                    IsBuiltInJsonFunction = true,
                    InnerFunction = ExtractInnerFunctionFromJsonQuery(functionCall)
                };

            case "JSON_VALUE":
                return new JsonFunctionInfo
                {
                    FunctionName = "JSON_VALUE",
                    IsBuiltInJsonFunction = true,
                    JsonValuePath = ExtractJsonValuePathFromFunction(functionCall),
                    InnerFunction = ExtractInnerFunctionFromJsonValue(functionCall)
                };

            default:
                return null;
        }
    }

    private JsonFunctionReference? ExtractInnerFunctionFromJsonQuery(FunctionCall jsonQueryCall)
    {
        // JSON_QUERY typically has format: JSON_QUERY((SELECT ... FOR JSON), '$')
        if (jsonQueryCall.Parameters.Count < 1)
        {
            return null;
        }

        var firstParam = jsonQueryCall.Parameters[0];
        if (firstParam is ParenthesisExpression parenthesis &&
            parenthesis.Expression is ScalarSubquery scalarSubquery)
        {
            // Analyze the subquery for function calls
            var subqueryAnalyzer = new SubqueryFunctionAnalyzer();
            scalarSubquery.Accept(subqueryAnalyzer);
            return subqueryAnalyzer.ExtractedFunction;
        }

        return null;
    }

    private JsonFunctionReference? ExtractInnerFunctionFromJsonValue(FunctionCall jsonValueCall)
    {
        // JSON_VALUE typically has format: JSON_VALUE(expression, '$.path')
        if (jsonValueCall.Parameters.Count < 1)
        {
            return null;
        }

        var firstParam = jsonValueCall.Parameters[0];
        if (firstParam is FunctionCall innerFunction)
        {
            return new JsonFunctionReference
            {
                FunctionName = innerFunction.FunctionName?.Value ?? string.Empty,
                SchemaName = ExtractSchemaFromFunction(innerFunction),
                Parameters = ExtractParameters(innerFunction)
            };
        }

        return null;
    }

    private static bool IsJsonBuiltInFunction(string functionName)
    {
        return functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase) ||
               functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase) ||
               functionName.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase);
    }

    private static string? ExtractJsonValuePathFromFunction(FunctionCall jsonValueCall)
    {
        if (jsonValueCall.Parameters.Count >= 2 &&
            jsonValueCall.Parameters[1] is Literal pathLiteral)
        {
            return pathLiteral.Value?.Trim('\'', '"');
        }

        return null;
    }

    private static string? ExtractSchemaFromFunction(FunctionCall functionCall)
    {
        // Check if function call has a multi-part identifier (schema.function)
        if (functionCall.CallTarget is MultiPartIdentifierCallTarget target &&
            target.MultiPartIdentifier != null &&
            target.MultiPartIdentifier.Identifiers.Count >= 1)
        {
            var parts = target.MultiPartIdentifier.Identifiers
                .Select(id => id?.Value)
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .ToArray();

            if (parts.Length > 0)
            {
                return string.Join('.', parts);
            }
        }

        return null;
    }

    private static List<string> ExtractParameters(FunctionCall functionCall)
    {
        var parameters = new List<string>();
        foreach (var param in functionCall.Parameters)
        {
            if (param is Literal literal)
            {
                parameters.Add(literal.Value ?? string.Empty);
            }
            else if (param is ColumnReferenceExpression column)
            {
                parameters.Add(column.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value ?? string.Empty);
            }
            else
            {
                parameters.Add(param.GetType().Name);
            }
        }

        return parameters;
    }

    private static string GetColumnName(SelectScalarExpression scalarExpr)
    {
        // Get column alias or derive from expression
        if (scalarExpr.ColumnName != null)
        {
            return scalarExpr.ColumnName.Value;
        }

        // Try to derive from expression
        if (scalarExpr.Expression is ColumnReferenceExpression columnRef)
        {
            return columnRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value ?? string.Empty;
        }

        if (scalarExpr.Expression is FunctionCall functionCall)
        {
            return functionCall.FunctionName?.Value ?? string.Empty;
        }

        return string.Empty;
    }
}

/// <summary>
/// Information about a JSON function found in a column expression.
/// </summary>
internal sealed class JsonFunctionInfo
{
    public string ColumnName { get; set; } = string.Empty;
    public string FunctionName { get; set; } = string.Empty;
    public string? SchemaName { get; set; }
    public bool IsBuiltInJsonFunction { get; set; }
    public string? JsonValuePath { get; set; }
    public List<string> Parameters { get; set; } = new();
    public JsonFunctionReference? InnerFunction { get; set; }

    /// <summary>
    /// Gets the proper function reference for code generation.
    /// </summary>
    public string GetFunctionReference()
    {
        if (InnerFunction != null)
        {
            return InnerFunction.GetFullName();
        }

        if (IsBuiltInJsonFunction)
        {
            return FunctionName; // Keep built-in function names as-is
        }

        if (!string.IsNullOrEmpty(SchemaName))
        {
            return $"{SchemaName}.{FunctionName}";
        }

        return FunctionName;
    }
}

/// <summary>
/// Reference to a function within a JSON expression.
/// </summary>
internal sealed class JsonFunctionReference
{
    public string FunctionName { get; set; } = string.Empty;
    public string? SchemaName { get; set; }
    public List<string> Parameters { get; set; } = new();

    public string GetFullName()
    {
        if (!string.IsNullOrEmpty(SchemaName))
        {
            return $"{SchemaName}.{FunctionName}";
        }

        return FunctionName;
    }
}

/// <summary>
/// Helper class to analyze subqueries within JSON_QUERY calls.
/// </summary>
internal sealed class SubqueryFunctionAnalyzer : TSqlFragmentVisitor
{
    public JsonFunctionReference? ExtractedFunction { get; private set; }

    public override void ExplicitVisit(FunctionCall node)
    {
        if (ExtractedFunction == null && !IsJsonBuiltInFunction(node.FunctionName?.Value))
        {
            ExtractedFunction = new JsonFunctionReference
            {
                FunctionName = node.FunctionName?.Value ?? string.Empty,
                SchemaName = ExtractSchemaFromFunction(node),
                Parameters = ExtractParameters(node)
            };
        }

        base.ExplicitVisit(node);
    }

    private static bool IsJsonBuiltInFunction(string? functionName)
    {
        return functionName != null && (
            functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase) ||
            functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase) ||
            functionName.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase));
    }

    private static string? ExtractSchemaFromFunction(FunctionCall functionCall)
    {
        if (functionCall.CallTarget is MultiPartIdentifierCallTarget target &&
            target.MultiPartIdentifier != null &&
            target.MultiPartIdentifier.Identifiers.Count >= 1)
        {
            var parts = target.MultiPartIdentifier.Identifiers
                .Select(id => id?.Value)
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .ToArray();

            if (parts.Length > 0)
            {
                return string.Join('.', parts);
            }
        }

        return null;
    }

    private static List<string> ExtractParameters(FunctionCall functionCall)
    {
        var parameters = new List<string>();
        foreach (var param in functionCall.Parameters)
        {
            if (param is Literal literal)
            {
                parameters.Add(literal.Value ?? string.Empty);
            }
            else if (param is ColumnReferenceExpression column)
            {
                parameters.Add(column.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value ?? string.Empty);
            }
            else
            {
                parameters.Add(param.GetType().Name);
            }
        }

        return parameters;
    }
}
