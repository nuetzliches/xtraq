using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.Metadata;
using Xtraq.SnapshotBuilder.Analyzers;
using Xtraq.SnapshotBuilder.Models;

namespace Xtraq.Services;

/// <summary>
/// Service that enhances procedure result columns with improved JSON function analysis.
/// Detects missing JSON_VALUE wrappers and provides proper function references.
/// </summary>
internal interface IJsonFunctionEnhancementService
{
    /// <summary>
    /// Enhance procedure analysis results with improved JSON function information.
    /// </summary>
    void EnhanceProcedureResultColumns(ProcedureAnalysisResult analysisResult);

    /// <summary>
    /// Enhance a list of field descriptors with improved JSON function information.
    /// </summary>
    IReadOnlyList<FieldDescriptor> EnhanceResultSetColumns(IReadOnlyList<FieldDescriptor> fields, Metadata.ProcedureDescriptor procedure);

    /// <summary>
    /// Analyze SQL text and extract JSON function information.
    /// </summary>
    IReadOnlyDictionary<string, JsonFunctionInfo> AnalyzeSqlText(string sqlText);

    /// <summary>
    /// Check if a procedure has potential JSON function issues that need attention.
    /// </summary>
    bool HasJsonFunctionIssues(ProcedureAnalysisResult analysisResult);
}

internal sealed class JsonFunctionEnhancementService : IJsonFunctionEnhancementService
{
    private readonly IConsoleService _console;
    private readonly TSql160Parser _parser;

    public JsonFunctionEnhancementService(IConsoleService console)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _parser = new TSql160Parser(initialQuotedIdentifiers: true);
    }

    public void EnhanceProcedureResultColumns(ProcedureAnalysisResult analysisResult)
    {
        if (analysisResult?.Procedure?.ResultSets is not { Count: > 0 } resultSets)
        {
            return;
        }

        // For now, we need the SQL content to analyze.
        // This must be supplied once the analyzer exposes definition text through ProcedureAnalysisResult.

        try
        {
            var hasEnhancements = false;
            foreach (var resultSet in resultSets)
            {
                foreach (var column in resultSet.Columns)
                {
                    if (TryEnhanceColumn(column))
                    {
                        hasEnhancements = true;
                    }
                }
            }

            if (hasEnhancements)
            {
                var descriptorName = analysisResult.Descriptor?.Name ?? "<unknown>";
                _console.Verbose($"[json-enhancement] Enhanced JSON function analysis for {descriptorName}");
            }
        }
        catch (Exception ex)
        {
            var descriptorName = analysisResult?.Descriptor?.Name ?? "<unknown>";
            _console.Verbose($"[json-enhancement] Failed to enhance {descriptorName}: {ex.Message}");
        }
    }

    public IReadOnlyDictionary<string, JsonFunctionInfo> AnalyzeSqlText(string sqlText)
    {
        if (string.IsNullOrWhiteSpace(sqlText))
        {
            return new Dictionary<string, JsonFunctionInfo>();
        }

        try
        {
            var parsed = _parser.Parse(new System.IO.StringReader(sqlText), out var errors);
            if (errors?.Count > 0)
            {
                _console.Verbose($"[json-enhancement] SQL parsing warnings: {errors.Count}");
            }

            var analyzer = new EnhancedJsonFunctionAnalyzer();
            parsed.Accept(analyzer);

            return analyzer.JsonFunctions;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[json-enhancement] SQL analysis failed: {ex.Message}");
            return new Dictionary<string, JsonFunctionInfo>();
        }
    }

    public bool HasJsonFunctionIssues(ProcedureAnalysisResult analysisResult)
    {
        // For now, return false because we cannot inspect SQL text without procedure definitions being exposed.
        // Once the analyzer publishes definition text, revisit this guard to provide real diagnostics.
        return false;
    }

    private bool TryEnhanceColumn(ProcedureResultColumn column)
    {
        if (column?.Name == null)
        {
            return false;
        }

        var enhanced = false;

        // Check if this column has JSON_QUERY as function reference and needs improvement
        var reference = column.Reference;
        if (reference != null &&
            string.Equals(reference.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase) &&
            reference.Kind == ProcedureReferenceKind.Function)
        {
            // This indicates the old behavior where JSON_QUERY was used as a generic function reference
            // We should warn about this and suggest using proper function references
            _console.Verbose($"[json-enhancement] Column '{column.Name}' uses generic JSON_QUERY reference. " +
                           "Consider using specific function references for better code generation.");
            enhanced = true;
        }

        // Check for potential JSON function improvements
        if (column.ExpressionKind == ProcedureResultColumnExpressionKind.FunctionCall &&
            column.ReturnsJson == true)
        {
            // This is a JSON-returning function call that might benefit from enhanced analysis
            _console.Verbose($"[json-enhancement] Column '{column.Name}' identified as JSON function call candidate.");
            enhanced = true;
        }

        return enhanced;
    }

    /// <summary>
    /// Enhance a list of field descriptors with improved JSON function information.
    /// </summary>
    public IReadOnlyList<FieldDescriptor> EnhanceResultSetColumns(IReadOnlyList<FieldDescriptor> fields, Metadata.ProcedureDescriptor procedure)
    {
        if (fields == null || fields.Count == 0 || procedure == null)
            return fields ?? Array.Empty<FieldDescriptor>();

        foreach (var field in fields)
        {
            // Check if this field has a generic JSON_QUERY function reference that needs enhancement
            if (field.FunctionRef != null &&
                string.Equals(field.FunctionRef, "JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                _console.Verbose($"[json-enhancement] Field '{field.Name}' uses generic JSON_QUERY reference, enhancement needed");

                // Analyze if this field might need a JSON_VALUE wrapper for logging purposes
                bool needsJsonValueWrapper = AnalyzeJsonValueWrapperNeeded(field, procedure);
                if (needsJsonValueWrapper)
                {
                    _console.Verbose($"[json-enhancement] Field '{field.Name}' likely requires JSON_VALUE wrapper, manual review recommended");
                }
            }
        }

        return fields;
    }

    /// <summary>
    /// Analyze if a field might need a JSON_VALUE wrapper based on its characteristics.
    /// </summary>
    private bool AnalyzeJsonValueWrapperNeeded(FieldDescriptor field, Metadata.ProcedureDescriptor procedure)
    {
        // Fields that are non-nullable and have specific CLR types might need JSON_VALUE wrapping
        // when they come from JSON_QUERY functions to ensure proper null handling
        if (field is { IsNullable: false } &&
            (string.Equals(field.ClrType, "string", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(field.ClrType, "int", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(field.ClrType, "decimal", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(field.ClrType, "bool", StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        // If the field name suggests it's a scalar value from JSON (common patterns)
        if (field.Name != null &&
            (field.Name.EndsWith("Id", StringComparison.OrdinalIgnoreCase) ||
             field.Name.EndsWith("Name", StringComparison.OrdinalIgnoreCase) ||
             field.Name.EndsWith("Code", StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        return false;
    }
}

/// <summary>
/// Extension methods for ProcedureResultColumn to support JSON function enhancements.
/// These provide a foundation for future enhancements when the data model supports them.
/// </summary>
internal static class ProcedureResultColumnExtensions
{
    /// <summary>
    /// Check if this column represents a JSON function call that might need enhancement.
    /// </summary>
    public static bool IsJsonFunctionCandidate(this ProcedureResultColumn column)
    {
        if (column == null)
        {
            return false;
        }

        return column.ExpressionKind == ProcedureResultColumnExpressionKind.FunctionCall &&
               (column.ReturnsJson == true ||
                string.Equals(column.Reference?.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Check if this column uses the generic JSON_QUERY reference (legacy behavior).
    /// </summary>
    public static bool UsesGenericJsonQueryReference(this ProcedureResultColumn column)
    {
        if (column?.Reference is not { } reference)
        {
            return false;
        }

        return string.Equals(reference.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase) &&
            reference.Kind == ProcedureReferenceKind.Function;
    }
}
