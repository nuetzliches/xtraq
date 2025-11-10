using System.Text.Json.Serialization;
using Xtraq.Data.Models;

namespace Xtraq.Models;

internal sealed class StoredProcedureModel : IEquatable<StoredProcedureModel>
{
    private readonly StoredProcedure _item;

    public StoredProcedureModel()
    {
        _item = new StoredProcedure();
    }

    public StoredProcedureModel(StoredProcedure item)
    {
        ArgumentNullException.ThrowIfNull(item);
        _item = item;
    }

    public string Name
    {
        get => _item.Name;
        set => _item.Name = value;
    }

    public DateTime Modified
    {
        get => _item.Modified;
        set => _item.Modified = value;
    }

    public long? ModifiedTicks { get; set; }

    [JsonIgnore]
    public string SchemaName
    {
        get => _item.SchemaName;
        set => _item.SchemaName = value;
    }

    private IReadOnlyList<StoredProcedureInput>? _input;
    public IReadOnlyList<StoredProcedureInput>? Input
    {
        get => _input != null && _input.Count > 0 ? _input : null;
        set => _input = value;
    }

    private StoredProcedureContentModel? _content;

    [JsonIgnore]
    public StoredProcedureContentModel? Content
    {
        get => _content;
        set => _content = value;
    }

    public IReadOnlyList<StoredProcedureContentModel.ResultSet>? ResultSets
        => Content?.ResultSets != null && Content.ResultSets.Count > 0 ? Content.ResultSets : null;

    public bool Equals(StoredProcedureModel? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(SchemaName, other.SchemaName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as StoredProcedureModel);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(SchemaName?.ToUpperInvariant(), Name?.ToUpperInvariant());
    }

    public override string ToString()
    {
        return "[SchemaName].[Name]";
    }
}
