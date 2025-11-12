using Xtraq.Models;

namespace Xtraq.Schema;

/// <summary>
/// Encapsulates schema selection preferences for the metadata pipeline.
/// </summary>
internal sealed class SchemaSelectionContext
{
    public SchemaStatusEnum DefaultSchemaStatus { get; set; } = SchemaStatusEnum.Build;

    /// <summary>
    /// Positive allow-list of schemas to include when loading metadata.
    /// </summary>
    public List<string> BuildSchemas { get; set; } = new();

    /// <summary>
    /// Optional explicit schema status overrides sourced from configuration or CLI input.
    /// </summary>
    public IReadOnlyList<SchemaStatusOverride> ExplicitSchemaStatuses { get; set; } = Array.Empty<SchemaStatusOverride>();

    /// <summary>
    /// Controls verbosity for JSON type inference diagnostics.
    /// </summary>
    public JsonTypeLogLevel JsonTypeLogLevel { get; set; } = JsonTypeLogLevel.Detailed;

    /// <summary>
    /// Identifier used when deriving cache fingerprints (typically the configured namespace).
    /// </summary>
    public string ProjectNamespace { get; set; } = "UnknownProject";
}

/// <summary>
/// Represents an explicit schema status rule supplied by configuration.
/// </summary>
internal sealed record SchemaStatusOverride(string Name, SchemaStatusEnum Status);
