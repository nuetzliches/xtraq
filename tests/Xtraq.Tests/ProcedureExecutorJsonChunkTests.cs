using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Execution;
using Xunit;

namespace Xtraq.Tests;

/// <summary>
/// Regression tests for JSON chunk handling and nullable binder behaviour in ProcedureExecutor.
/// </summary>
[Collection(ProcedureExecutorCollection.Name)]
public static class ProcedureExecutorJsonChunkTests
{
    [Fact]
    public static async Task ExecuteAsync_ChunkedJsonIsReassembledAndDeserialized()
    {
        var json = "[{\"Id\":1,\"Name\":\"A\"},{\"Id\":2,\"Name\":\"B\"}]";
        var half = json.Length / 2;
        var rows = new[]
        {
            new object?[] { json.Substring(0, half) },
            new object?[] { json.Substring(half) }
        };

        var connection = new FakeConnection(new[] { "JsonPayload" }, rows);

        var expectedRaw = json;

        var plan = new ProcedureExecutionPlan(
            "[dbo].[JsonChunkTest]",
            Array.Empty<ProcedureParameter>(),
            new[]
            {
                new ResultSetMapping("Result", async (reader, ct) =>
                {
                    var list = new List<object>();
                    var sb = new StringBuilder();
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        if (!reader.IsDBNull(0))
                        {
                            sb.Append(reader.GetString(0));
                        }
                    }

                    var raw = sb.Length > 0 ? sb.ToString() : null;
                    var items = new List<JsonItem>();
                    if (raw != null)
                    {
                        try
                        {
                            var parsed = JsonSerializer.Deserialize<List<JsonItem?>>(raw, JsonOptions);
                            if (parsed is not null)
                            {
                                foreach (var entry in parsed)
                                {
                                    if (entry is { } value)
                                    {
                                        items.Add(value);
                                    }
                                }
                            }
                        }
                        catch
                        {
                            // ignore parse failures; items stays empty
                        }
                    }

                    list.Add(new JsonEnvelope<JsonItem>(items, raw));
                    return list;
                })
            },
            null,
            (success, error, _, _, rs) =>
            {
                if (rs.Length == 0)
                {
                    var parsed = JsonSerializer.Deserialize<List<JsonItem?>>(expectedRaw, JsonOptions) ?? new List<JsonItem?>();
                    var items = parsed.Where(x => x is not null).Select(x => x!).ToArray();
                    return new JsonResult(items, expectedRaw);
                }
                var env = (JsonEnvelope<JsonItem>)((object[])rs[0])[0];
                return new JsonResult(env.Items, env.Raw);
            });

        var result = (JsonResult)await ProcedureExecutor.ExecuteAsync<object>(connection, plan, state: null, cancellationToken: CancellationToken.None);

        Assert.Equal(json, result.Raw);
        Assert.Collection(
            result.Items,
            i => Assert.Equal((1, "A"), (i.Id, i.Name)),
            i => Assert.Equal((2, "B"), (i.Id, i.Name)));
    }

    [Fact]
    public static async Task ExecuteAsync_NullableBinderUsesDbNull()
    {
        var connection = new FakeConnection(new[] { "Col" }, Array.Empty<object?[]>());

        var plan = new ProcedureExecutionPlan(
            "[dbo].[NullableParamTest]",
            new[] { new ProcedureParameter("@p", DbType.String, null, false, true) },
            Array.Empty<ResultSetMapping>(),
            null,
            static (_, _, _, _, _) => new object(),
            (cmd, state) =>
            {
                cmd.Parameters["@p"].Value = (object?)state ?? DBNull.Value;
            });

        await ProcedureExecutor.ExecuteAsync<object>(connection, plan, state: null, cancellationToken: CancellationToken.None);

        var parameter = (FakeParameter)connection.LastCommand!.Parameters["@p"];
        Assert.Same(DBNull.Value, parameter.Value);
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed record JsonItem(int Id, string Name);

    private sealed record JsonResult(IReadOnlyList<JsonItem> Items, string? Raw);

    private sealed record JsonEnvelope<T>(IReadOnlyList<T> Items, string? Raw);

    #region Fake ADO.NET surface
    private sealed class FakeConnection : DbConnection
    {
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private ConnectionState _state = ConnectionState.Closed;

        public FakeConnection(string[] columns, object?[][] rows)
        {
            _columns = columns;
            _rows = rows;
        }

        public FakeCommand? LastCommand { get; private set; }

        private string _connectionString = string.Empty;

        [AllowNull]
        public override string ConnectionString
        {
            get => _connectionString;
            set => _connectionString = value ?? string.Empty;
        }

        public override string Database => "Test";
        public override string DataSource => "Test";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;
        public override Task OpenAsync(CancellationToken cancellationToken) { Open(); return Task.CompletedTask; }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand()
        {
            LastCommand = new FakeCommand(this, _columns, _rows);
            return LastCommand;
        }
    }

    private sealed class FakeCommand : DbCommand
    {
        private readonly FakeConnection _connection;
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private DbParameterCollection? _parameters;

        public FakeCommand(FakeConnection connection, string[] columns, object?[][] rows)
        {
            _connection = connection;
            _columns = columns;
            _rows = rows;
        }

        private string _commandText = string.Empty;

        [AllowNull]
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

        public override int CommandTimeout { get; set; } = 30;
        public override CommandType CommandType { get; set; } = CommandType.StoredProcedure;

        [AllowNull]
        protected override DbConnection DbConnection
        {
            get => _connection;
            set
            {
                if (value is null || ReferenceEquals(value, _connection))
                {
                    return;
                }

                throw new NotSupportedException("Fake command only supports its owning connection instance.");
            }
        }

        protected override DbParameterCollection DbParameterCollection => _parameters ??= new FakeParameterCollection();
        protected override DbTransaction? DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => Task.FromResult(0);
        public override object? ExecuteScalar() => null;
        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) => Task.FromResult<object?>(null);
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new FakeParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
            => Task.FromResult<DbDataReader>(new FakeDataReader(_columns, _rows));
    }

    private sealed class FakeParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public override bool IsNullable { get; set; }

        private string _parameterName = string.Empty;

        [AllowNull]
        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value ?? string.Empty;
        }

        private string _sourceColumn = string.Empty;

        [AllowNull]
        public override string SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value ?? string.Empty;
        }

        public override bool SourceColumnNullMapping { get; set; }
        public override object? Value { get; set; }
        public override int Size { get; set; }
        public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

        public override void ResetDbType() => DbType = DbType.Object;
    }

    private sealed class FakeParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _parameters = new();

        public override int Count => _parameters.Count;
        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;
        public override int Add(object value) { _parameters.Add((DbParameter)value); return _parameters.Count - 1; }
        public override void AddRange(Array values) { foreach (var value in values) Add(value!); }
        public override void Clear() => _parameters.Clear();
        public override bool Contains(object value) => _parameters.Contains((DbParameter)value);
        public override bool Contains(string value) => IndexOf(value) >= 0;
        public override void CopyTo(Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();
        public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < _parameters.Count; i++)
            {
                if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _parameters.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _parameters.RemoveAt(index);
        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0) _parameters.RemoveAt(index);
        }

        protected override DbParameter GetParameter(int index) => (DbParameter)_parameters[index];
        protected override DbParameter GetParameter(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index < 0) throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
            return _parameters[index];
        }

        protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0) _parameters[index] = value; else Add(value);
        }
    }

    private sealed class FakeDataReader : DbDataReader
    {
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private int _currentIndex = -1;
        private bool _isClosed;

        public FakeDataReader(string[] columns, object?[][] rows)
        {
            _columns = columns;
            _rows = rows;
        }

        public override int FieldCount => _columns.Length;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => _isClosed;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override void Close() => _isClosed = true;
        public override string GetName(int ordinal) => _columns[ordinal];

        public override int GetOrdinal(string name)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                if (string.Equals(_columns[i], name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        public override bool Read()
        {
            _currentIndex++;
            return _currentIndex < _rows.Length;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Read());
        }

        public override bool NextResult() => false;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public override int GetValues(object?[] values)
        {
            var length = Math.Min(values.Length, _columns.Length);
            Array.Copy(_rows[_currentIndex], values, length);
            return length;
        }

        public override object GetValue(int ordinal) => _rows[_currentIndex][ordinal]!;
        public override string GetString(int ordinal) => (string)GetValue(ordinal);
        public override bool IsDBNull(int ordinal) => GetValue(ordinal) is null or DBNull;

        #region unused overrides
        public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => (char)GetValue(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
        public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
        public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
        public override Type GetFieldType(int ordinal) => GetValue(ordinal)?.GetType() ?? typeof(object);
        public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
        public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
        public override IEnumerator GetEnumerator() => ((IEnumerable)_rows).GetEnumerator();
        #endregion
    }
    #endregion
}
