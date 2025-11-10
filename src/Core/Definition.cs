using Xtraq.Data.Models;
using Xtraq.Extensions;
using Xtraq.Models;

namespace Xtraq.Core;

internal static class Definition
{
    public static Schema ForSchema(SchemaModel schema)
    {
        return new Schema(schema);
    }

    public static StoredProcedure ForStoredProcedure(StoredProcedureModel storedProcedure, Schema schema)
    {
        return new StoredProcedure(storedProcedure, schema);
    }

    public static TableType ForTableType(TableTypeModel tableType, Schema schema)
    {
        return new TableType(tableType, schema);
    }

    internal sealed class Schema
    {
        private readonly SchemaModel _schema;

        public Schema(SchemaModel schema)
        {
            ArgumentNullException.ThrowIfNull(schema);
            _schema = schema;
            Identifier = schema.Name;
            Name = schema.Name.ToPascalCase();
            Path = Name;
        }

        public string Identifier { get; }
        public string Name { get; }
        public string Path { get; }

        private IEnumerable<StoredProcedure>? _storedProcedures;
        public IEnumerable<StoredProcedure> StoredProcedures
            => _storedProcedures ??= (_schema.StoredProcedures ?? Enumerable.Empty<StoredProcedureModel>())
                .Select(i => ForStoredProcedure(i, this));

        private IEnumerable<TableType>? _tableTypes;
        public IEnumerable<TableType> TableTypes
            => _tableTypes ??= (_schema.TableTypes ?? Enumerable.Empty<TableTypeModel>())
                .Select(i => ForTableType(i, this));
    }

    internal sealed class StoredProcedure
    {
        private readonly StoredProcedureModel _storedProcedure;
        private readonly Schema _schema;
        private string? _sqlObjectName;
        private string? _name;

        public StoredProcedure(StoredProcedureModel storedProcedure, Schema schema)
        {
            ArgumentNullException.ThrowIfNull(storedProcedure);
            ArgumentNullException.ThrowIfNull(schema);
            _storedProcedure = storedProcedure;
            _schema = schema;
        }

        public string SqlObjectName => _sqlObjectName ??= $"[{_schema.Identifier}].[{Name}]";

        public string Name => _name ??= _storedProcedure.Name;

        public IReadOnlyList<StoredProcedureContentModel.ResultSet> ResultSets =>
            _storedProcedure.ResultSets ?? Array.Empty<StoredProcedureContentModel.ResultSet>();

        public IReadOnlyList<StoredProcedureInput> Input =>
            _storedProcedure.Input ?? Array.Empty<StoredProcedureInput>();

        public bool IsPureWrapper
        {
            get
            {
                var sets = _storedProcedure.ResultSets;
                if (sets == null || sets.Count != 1) return false;
                var rs = sets[0];
                bool hasExec = !string.IsNullOrEmpty(rs?.ExecSourceProcedureName);
                bool noCols = rs?.Columns == null || rs.Columns.Count == 0;
                bool notJson = rs is { ReturnsJson: false, ReturnsJsonArray: false };
                return hasExec && noCols && notJson;
            }
        }
    }

    internal sealed class TableType
    {
        private readonly TableTypeModel _tableType;
        private readonly Schema _schema;
        private string? _sqlObjectName;
        private string? _name;

        public TableType(TableTypeModel tableType, Schema schema)
        {
            ArgumentNullException.ThrowIfNull(tableType);
            ArgumentNullException.ThrowIfNull(schema);
            _tableType = tableType;
            _schema = schema;
        }

        public string SqlObjectName => _sqlObjectName ??= $"[{_schema.Name.ToLowerInvariant()}].[{Name}]";

        public string Name => _name ??= _tableType.Name;

        public IEnumerable<ColumnModel> Columns => _tableType.Columns;
    }
}
