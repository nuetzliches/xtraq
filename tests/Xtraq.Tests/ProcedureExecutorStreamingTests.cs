using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Execution;
using Xunit;

namespace Xtraq.Tests;

/// <summary>
/// Tests covering the streaming execution path in <see cref="ProcedureExecutor"/>.
/// </summary>
[Collection(ProcedureExecutorCollection.Name)]
public static class ProcedureExecutorStreamingTests
{
    [Xunit.Fact]
    public static async Task StreamResultSetAsync_StreamsRowsAndReturnsOutput()
    {
        var columns = new[] { "UserId", "DisplayName" };
        var rows = new[]
        {
            new object?[] { 1, "Anna" },
            new object?[] { 2, "Ben" }
        };

        var connection = new TestDbConnection(columns, rows, outputValue: 2);

        var plan = new ProcedureExecutionPlan(
            "[dbo].[UsersList]",
            new[]
            {
                new ProcedureParameter("@tenantId", DbType.Int32, null, false, false),
                new ProcedureParameter("@totalCount", DbType.Int32, null, true, false)
            },
            new[]
            {
                new ResultSetMapping("Users", async (reader, ct) =>
                {
                    var list = new List<object>();
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        // Drain rows for non-streamed sets (not used in this scenario).
                    }

                    return list;
                })
            },
            values => values.TryGetValue("totalCount", out var value) ? value : null,
            static (success, error, output, outputs, rs) => new object(),
            (command, state) =>
            {
                command.Parameters["@tenantId"].Value = state!;
            });

        var streamed = new List<(int Id, string Name)>();

        async Task StreamRowsAsync(DbDataReader reader, CancellationToken ct)
        {
            var idOrdinal = reader.GetOrdinal("UserId");
            var nameOrdinal = reader.GetOrdinal("DisplayName");

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var id = reader.GetInt32(idOrdinal);
                var name = reader.GetString(nameOrdinal);
                streamed.Add((id, name));
            }
        }

        var output = await ProcedureExecutor.StreamResultSetAsync(
            connection,
            plan,
            resultSetIndex: 0,
            StreamRowsAsync,
            state: 7,
            cancellationToken: CancellationToken.None);

        Xunit.Assert.Equal(new[] { (1, "Anna"), (2, "Ben") }, streamed);
        Xunit.Assert.Equal(7, ((TestDbParameter)connection.LastCommand!.Parameters["@tenantId"]).Value);
        Xunit.Assert.Equal(2, output);
    }

    private sealed class TestDbConnection : DbConnection
    {
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private readonly int _outputValue;
        private ConnectionState _state = ConnectionState.Closed;

        public TestDbConnection(string[] columns, object?[][] rows, int outputValue)
        {
            _columns = columns;
            _rows = rows;
            _outputValue = outputValue;
        }

        public TestDbCommand? LastCommand { get; private set; }

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

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            Open();
            return Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
        {
            LastCommand = new TestDbCommand(this, _columns, _rows, _outputValue);
            return LastCommand;
        }
    }

    private sealed class TestDbCommand : DbCommand
    {
        private readonly TestDbConnection _connection;
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private readonly int _outputValue;
        private DbParameterCollection? _parameters;

        public TestDbCommand(TestDbConnection connection, string[] columns, object?[][] rows, int outputValue)
        {
            _connection = connection;
            _columns = columns;
            _rows = rows;
            _outputValue = outputValue;
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

                throw new NotSupportedException("Test command only supports its owning connection instance.");
            }
        }

        protected override DbParameterCollection DbParameterCollection => _parameters ??= new TestDbParameterCollection();

        protected override DbTransaction? DbTransaction { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override void Cancel() { }

        public override int ExecuteNonQuery() => 0;

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => Task.FromResult(0);

        public override object? ExecuteScalar() => null;

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) => Task.FromResult<object?>(null);

        public override void Prepare() { }

        protected override DbParameter CreateDbParameter() => new TestDbParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotSupportedException("Synchronous execution is not supported in this test harness.");
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (TestDbParameter parameter in DbParameterCollection)
            {
                if (parameter.Direction.HasFlag(ParameterDirection.Output))
                {
                    parameter.Value = _outputValue;
                }
            }

            return await Task.FromResult<DbDataReader>(new TestDbDataReader(_columns, _rows)).ConfigureAwait(false);
        }
    }

    private sealed class TestDbParameter : DbParameter
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

    private sealed class TestDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _parameters = new();

        public override int Count => _parameters.Count;

        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        public override int Add(object value)
        {
            _parameters.Add((DbParameter)value);
            return _parameters.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value!);
            }
        }

        public override void Clear() => _parameters.Clear();

        public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

        public override bool Contains(string value)
        {
            foreach (var parameter in _parameters)
            {
                if (string.Equals(parameter.ParameterName, value, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

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
            if (index >= 0)
            {
                _parameters.RemoveAt(index);
            }
        }

        protected override DbParameter GetParameter(int index) => (DbParameter)_parameters[index];

        protected override DbParameter GetParameter(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index < 0)
            {
                throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
            }

            return _parameters[index];
        }

        protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _parameters[index] = value;
            }
            else
            {
                Add(value);
            }
        }

    }

    private sealed class TestDbDataReader : DbDataReader
    {
        private readonly string[] _columns;
        private readonly object?[][] _rows;
        private int _currentIndex = -1;
        private bool _isClosed;

        public TestDbDataReader(string[] columns, object?[][] rows)
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

        public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);

        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            var data = (byte[])GetValue(ordinal);
            if (buffer is null)
            {
                return data.Length;
            }

            var available = Math.Min(length, Math.Max(0, data.Length - (int)dataOffset));
            Array.Copy(data, (int)dataOffset, buffer, bufferOffset, available);
            return available;
        }

        public override char GetChar(int ordinal) => (char)GetValue(ordinal);

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var data = ((string)GetValue(ordinal)).ToCharArray();
            if (buffer is null)
            {
                return data.Length;
            }

            var available = Math.Min(length, Math.Max(0, data.Length - (int)dataOffset));
            Array.Copy(data, (int)dataOffset, buffer, bufferOffset, available);
            return available;
        }

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

        public override string GetString(int ordinal) => (string)GetValue(ordinal);

        public override object GetValue(int ordinal) => _rows[_currentIndex][ordinal]!;

        public override int GetValues(object?[] values)
        {
            var length = Math.Min(values.Length, _columns.Length);
            Array.Copy(_rows[_currentIndex], values, length);
            return length;
        }

        public override bool IsDBNull(int ordinal)
        {
            var value = GetValue(ordinal);
            return value is null or DBNull;
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

        public override bool NextResult() => false;

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

        public override IEnumerator GetEnumerator() => ((IEnumerable)_rows).GetEnumerator();
    }
}
