
namespace Xtraq.SnapshotBuilder.Models;

internal enum ProcedureDependencyKind
{
    Unknown = 0,
    Procedure,
    Function,
    View,
    Table,
    UserDefinedTableType,
    UserDefinedType
}

internal sealed class ProcedureDependency
{
    public ProcedureDependencyKind Kind { get; init; } = ProcedureDependencyKind.Unknown;
    public string? Catalog { get; init; }
    public string Schema { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public DateTime? LastModifiedUtc { get; init; }

    public override string ToString()
    {
        var identifier = string.IsNullOrWhiteSpace(Schema) ? Name : string.Concat(Schema, ".", Name);
        if (!string.IsNullOrWhiteSpace(Catalog))
        {
            identifier = string.Concat(Catalog, ".", identifier);
        }

        return Kind == ProcedureDependencyKind.Unknown ? identifier : $"{Kind}:{identifier}";
    }
}
