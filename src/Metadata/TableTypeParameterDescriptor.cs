namespace Xtraq.Metadata;

/// <summary>
/// Describes a table-valued parameter reference discovered while parsing stored procedure metadata.
/// Captures both the source parameter name and the resolved user-defined table type identity, supporting catalog-qualified references.
/// </summary>
/// <param name="ParameterName">The name of the procedure parameter (without leading @).</param>
/// <param name="TableTypeSchema">The schema owning the referenced table type.</param>
/// <param name="TableTypeName">The user-defined table type name.</param>
/// <param name="NormalizedTypeReference">The normalized schema-qualified (or catalog-schema-qualified) name used for lookups.</param>
/// <param name="TableTypeCatalog">The optional catalog (database) owning the table type.</param>
public sealed record TableTypeParameterDescriptor(
    string ParameterName,
    string TableTypeSchema,
    string TableTypeName,
    string? NormalizedTypeReference = null,
    string? TableTypeCatalog = null)
{
    /// <summary>
    /// Gets the fully qualified identity (catalog.schema.name) of the referenced table type where available.
    /// </summary>
    public string QualifiedName
    {
        get
        {
            var hasCatalog = !string.IsNullOrWhiteSpace(TableTypeCatalog);
            var hasSchema = !string.IsNullOrWhiteSpace(TableTypeSchema);

            if (hasCatalog && hasSchema)
            {
                return string.Concat(TableTypeCatalog, ".", TableTypeSchema, ".", TableTypeName);
            }

            if (hasCatalog)
            {
                return string.Concat(TableTypeCatalog, ".", TableTypeName);
            }

            return hasSchema
                ? string.Concat(TableTypeSchema, ".", TableTypeName)
                : TableTypeName;
        }
    }
}
