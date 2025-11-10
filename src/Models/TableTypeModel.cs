using System.Text.Json.Serialization;
using Xtraq.Data.Models;

namespace Xtraq.Models;

internal sealed class TableTypeModel : IEquatable<TableTypeModel>
{
    private readonly TableType _item;
    private List<ColumnModel> _columns = new();

    public TableTypeModel()
    {
        _item = new TableType();
    }

    public TableTypeModel(TableType item, List<Column> columns)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(columns);
        _item = item;
        Columns = columns.Select(c => new ColumnModel(c)).ToList();
    }

    public string Name
    {
        get => _item.Name;
        set => _item.Name = value;
    }

    [JsonIgnore]
    public string SchemaName
    {
        get => _item.SchemaName;
        set => _item.SchemaName = value;
    }

    public IReadOnlyList<ColumnModel> Columns
    {
        get => _columns;
        set => _columns = value?.ToList() ?? new List<ColumnModel>();
    }

    public bool Equals(TableTypeModel? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(SchemaName, other.SchemaName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as TableTypeModel);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(SchemaName?.ToUpperInvariant(), Name?.ToUpperInvariant());
    }

    public override string ToString()
    {
        return $"[{SchemaName}].[{Name}]";
    }
}
