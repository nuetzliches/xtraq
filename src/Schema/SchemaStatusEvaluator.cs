using Xtraq.Models;

namespace Xtraq.Schema;

/// <summary>
/// Calculates effective schema statuses for a configuration based on explicit overrides and allow-lists.
/// </summary>
internal sealed class SchemaStatusEvaluator
{
    /// <summary>
    /// Applies status rules to the provided schema list.
    /// </summary>
    /// <param name="context">Current schema-selection context.</param>
    /// <param name="schemas">Schemas discovered from the data source.</param>
    /// <returns>Evaluation result with ordered schemas and the active subset.</returns>
    public SchemaStatusEvaluation Evaluate(SchemaSelectionContext? context, List<SchemaModel> schemas)
    {
        ArgumentNullException.ThrowIfNull(schemas);

        context ??= new SchemaSelectionContext();

        var normalizedBuildSchemas = NormalizeSchemas(context.BuildSchemas);
        var explicitOverrides = context.ExplicitSchemaStatuses ?? Array.Empty<SchemaStatusOverride>();
        var hasOverrides = explicitOverrides.Count > 0;

        if (hasOverrides)
        {
            ApplyExplicitStatusOverrides(context.DefaultSchemaStatus, explicitOverrides, schemas);
        }
        else if (normalizedBuildSchemas.Count > 0)
        {
            ApplyBuildSchemasAllowList(normalizedBuildSchemas, schemas);
        }
        else
        {
            ApplyDefaultStatus(context.DefaultSchemaStatus, schemas);
        }

        var orderedSchemas = schemas
            .OrderByDescending(static schema => schema.Status)
            .ToList();

        var activeSchemas = orderedSchemas
            .Where(static schema => schema.Status != SchemaStatusEnum.Ignore)
            .ToList();

        var buildSchemas = NormalizeSchemas(activeSchemas.Select(static schema => schema.Name));
        var buildSchemasChanged = !buildSchemas.SequenceEqual(normalizedBuildSchemas, StringComparer.OrdinalIgnoreCase);

        return new SchemaStatusEvaluation(
            orderedSchemas,
            activeSchemas,
            buildSchemasChanged,
            buildSchemas);
    }

    private static void ApplyExplicitStatusOverrides(
        SchemaStatusEnum defaultStatus,
        IReadOnlyList<SchemaStatusOverride> overridesSource,
        List<SchemaModel> schemas)
    {
        var overrides = overridesSource
            .Where(static entry => !string.IsNullOrWhiteSpace(entry.Name))
            .GroupBy(static entry => entry.Name, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                static grouping => grouping.Key,
                static grouping => grouping.Last().Status,
                StringComparer.OrdinalIgnoreCase);

        foreach (var schema in schemas)
        {
            if (overrides.TryGetValue(schema.Name ?? string.Empty, out var status))
            {
                schema.Status = status;
            }
            else
            {
                schema.Status = defaultStatus;
            }
        }
    }

    private static void ApplyBuildSchemasAllowList(IReadOnlyCollection<string> normalizedAllowList, List<SchemaModel> schemas)
    {
        var allowList = new HashSet<string>(normalizedAllowList, StringComparer.OrdinalIgnoreCase);
        foreach (var schema in schemas)
        {
            var name = schema.Name;
            if (string.IsNullOrWhiteSpace(name))
            {
                schema.Status = SchemaStatusEnum.Ignore;
                continue;
            }

            schema.Status = allowList.Contains(name)
                ? SchemaStatusEnum.Build
                : SchemaStatusEnum.Ignore;
        }
    }

    private static void ApplyDefaultStatus(SchemaStatusEnum defaultStatus, List<SchemaModel> schemas)
    {
        foreach (var schema in schemas)
        {
            schema.Status = defaultStatus;
        }
    }

    private static List<string> NormalizeSchemas(IEnumerable<string?>? source)
    {
        if (source is null)
        {
            return new List<string>();
        }

        return source
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Select(static name => name!.Trim())
            .Where(static name => name.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}

internal sealed record SchemaStatusEvaluation(
    List<SchemaModel> Schemas,
    List<SchemaModel> ActiveSchemas,
    bool BuildSchemasChanged,
    IReadOnlyList<string> BuildSchemas);
