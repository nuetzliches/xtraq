namespace Xtraq.SnapshotBuilder.Models;

/// <summary>
/// Minimal identifier for a stored procedure. Additional metadata can be layered without touching stage contracts.
/// </summary>
internal sealed class ProcedureDescriptor
{
    public string? Catalog { get; init; }
    public string Schema { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;

    public override string ToString()
    {
        var schemaQualified = string.IsNullOrWhiteSpace(Schema) ? Name : string.Concat(Schema, ".", Name);
        if (string.IsNullOrWhiteSpace(Catalog))
        {
            return schemaQualified;
        }

        return string.Concat(Catalog, ".", schemaQualified);
    }
}
