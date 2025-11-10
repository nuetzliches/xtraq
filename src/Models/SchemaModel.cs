using DbSchema = Xtraq.Data.Models.Schema;

namespace Xtraq.Models;

internal sealed class SchemaModel
{
    private readonly DbSchema _item;

    public SchemaModel()
    {
        _item = new DbSchema();
    }

    public SchemaModel(DbSchema item)
    {
        ArgumentNullException.ThrowIfNull(item);
        _item = item;
    }

    public string Name
    {
        get => _item.Name;
        set => _item.Name = value;
    }

    public SchemaStatusEnum Status { get; set; } = SchemaStatusEnum.Build;

    private IEnumerable<StoredProcedureModel>? _storedProcedures;
    public IEnumerable<StoredProcedureModel>? StoredProcedures
    {
        get => _storedProcedures;
        set => _storedProcedures = value;
    }

    private IEnumerable<TableTypeModel>? _tableTypes;
    public IEnumerable<TableTypeModel>? TableTypes
    {
        get => _tableTypes;
        set => _tableTypes = value;
    }
}

internal enum SchemaStatusEnum
{
    Undefined,
    Snapshot,
    Build,
    Ignore
}
