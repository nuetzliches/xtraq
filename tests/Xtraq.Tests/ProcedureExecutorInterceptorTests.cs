using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Xtraq.Execution;

namespace Xtraq.Tests;

public interface IXtraqDbContext
{
    DbConnection OpenConnection();
    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);
    int CommandTimeout { get; }
}

public sealed class ProcedureExecutorInterceptorTests
{
    [Xunit.Fact]
    public async Task ExecuteAsync_WithMultipleGlobalInterceptors_InvokesAll()
    {
        var events = new List<string>();
        var connection = new FakeConnection();
        var plan = CreatePlan();

        var interceptorA = new RecordingInterceptor(events, "A");
        var interceptorB = new RecordingInterceptor(events, "B");

        ProcedureExecutor.ClearInterceptors();
        ProcedureExecutor.AddInterceptor(interceptorA);
        ProcedureExecutor.AddInterceptor(interceptorB);

        try
        {
            await ProcedureExecutor.ExecuteAsync<bool>(connection, plan, state: null, CancellationToken.None);
        }
        finally
        {
            ProcedureExecutor.ClearInterceptors();
        }

        Xunit.Assert.Equal(new[] { "A-before", "B-before", "A-after", "B-after" }, events);
    }

    [Xunit.Fact]
    public async Task ExecuteAsync_WithScopedInterceptorProvider_AppendsScopedInterceptor()
    {
        var events = new List<string>();
        var connection = new FakeConnection();
        var plan = CreatePlan();

        var global = new RecordingInterceptor(events, "Global");
        ProcedureExecutor.ClearInterceptors();
        ProcedureExecutor.AddInterceptor(global);

        var context = new FakeContext(events);

        try
        {
            await ProcedureExecutor.ExecuteAsync<bool>(context, connection, plan, state: null, CancellationToken.None);
        }
        finally
        {
            ProcedureExecutor.ClearInterceptors();
        }

        Xunit.Assert.Equal(new[] { "Global-before", "Scoped-before", "Global-after", "Scoped-after" }, events);
    }

    private static ProcedureExecutionPlan CreatePlan()
    {
        return new ProcedureExecutionPlan(
            "[dbo].[noop]",
            Array.Empty<ProcedureParameter>(),
            Array.Empty<ResultSetMapping>(),
            outputFactory: null,
            aggregateFactory: static (success, error, output, outputs, resultSets) => success,
            inputBinder: null);
    }

    private sealed class RecordingInterceptor : IXtraqProcedureInterceptor
    {
        private readonly List<string> _events;
        private readonly string _name;

        public RecordingInterceptor(List<string> events, string name)
        {
            _events = events;
            _name = name;
        }

        public Task<object?> OnBeforeExecuteAsync(string procedureName, DbCommand command, object? state, CancellationToken cancellationToken)
        {
            _events.Add($"{_name}-before");
            return Task.FromResult<object?>(_name);
        }

        public Task OnAfterExecuteAsync(string procedureName, DbCommand command, bool success, string? error, TimeSpan duration, object? beforeState, object? aggregate, CancellationToken cancellationToken)
        {
            _events.Add($"{_name}-after");
            return Task.CompletedTask;
        }
    }

    private sealed class FakeContext : IXtraqDbContext, IXtraqProcedureInterceptorProvider
    {
        private readonly List<string> _events;

        public FakeContext(List<string> events)
        {
            _events = events;
        }

        public int CommandTimeout => 30;

        public IReadOnlyList<IXtraqProcedureInterceptor> GetInterceptors()
        {
            return new[] { new RecordingInterceptor(_events, "Scoped") };
        }

        public Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

        public DbConnection OpenConnection() => new FakeConnection();

        public Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default) => Task.FromResult<DbConnection>(new FakeConnection());
    }

    private sealed class FakeConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public override string ConnectionString { get; set; } = string.Empty;

        public override string Database => "Fake";

        public override string DataSource => "Fake";

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

        protected override DbCommand CreateDbCommand() => new FakeCommand(this);
    }

    private sealed class FakeCommand : DbCommand
    {
        private readonly FakeConnection _connection;
        private readonly DbParameterCollection _parameters = new FakeParameterCollection();

        public FakeCommand(FakeConnection connection)
        {
            _connection = connection;
        }

        public override string CommandText { get; set; } = string.Empty;

        public override int CommandTimeout { get; set; } = 30;

        public override CommandType CommandType { get; set; } = CommandType.StoredProcedure;

        protected override DbConnection DbConnection
        {
            get => _connection;
            set => throw new NotSupportedException();
        }

        protected override DbParameterCollection DbParameterCollection => _parameters;

        protected override DbTransaction? DbTransaction { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;

        public override void Cancel() { }

        public override int ExecuteNonQuery() => 0;

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => Task.FromResult(0);

        public override object? ExecuteScalar() => null;

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken) => Task.FromResult<object?>(null);

        public override void Prepare() { }

        protected override DbParameter CreateDbParameter() => new FakeParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new FakeReader();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return Task.FromResult<DbDataReader>(new FakeReader());
        }
    }

    private sealed class FakeParameter : DbParameter
    {
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; } = string.Empty;

        public override string SourceColumn { get; set; } = string.Empty;

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

        public override object SyncRoot => _parameters;

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

        public override bool Contains(string value) => _parameters.Exists(p => string.Equals(p.ParameterName, value, StringComparison.OrdinalIgnoreCase));

        public override void CopyTo(Array array, int index) => _parameters.ToArray().CopyTo(array, index);

        public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

        public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

        public override int IndexOf(string parameterName) => _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));

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

        protected override DbParameter GetParameter(int index) => _parameters[index];

        protected override DbParameter GetParameter(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index < 0)
            {
                throw new IndexOutOfRangeException();
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

    private sealed class FakeReader : DbDataReader
    {
        public override int Depth => 0;

        public override int FieldCount => 0;

        public override bool HasRows => false;

        public override bool IsClosed => false;

        public override int RecordsAffected => 0;

        public override object this[int ordinal] => throw new IndexOutOfRangeException();

        public override object this[string name] => throw new IndexOutOfRangeException();

        public override bool GetBoolean(int ordinal) => false;

        public override byte GetByte(int ordinal) => 0;

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;

        public override char GetChar(int ordinal) => default;

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;

        public override string GetDataTypeName(int ordinal) => string.Empty;

        public override DateTime GetDateTime(int ordinal) => DateTime.MinValue;

        public override decimal GetDecimal(int ordinal) => 0;

        public override double GetDouble(int ordinal) => 0;

        public override Type GetFieldType(int ordinal) => typeof(object);

        public override float GetFloat(int ordinal) => 0;

        public override Guid GetGuid(int ordinal) => Guid.Empty;

        public override short GetInt16(int ordinal) => 0;

        public override int GetInt32(int ordinal) => 0;

        public override long GetInt64(int ordinal) => 0;

        public override string GetName(int ordinal) => string.Empty;

        public override int GetOrdinal(string name) => -1;

        public override string GetString(int ordinal) => string.Empty;

        public override object GetValue(int ordinal) => DBNull.Value;

        public override int GetValues(object?[] values) => 0;

        public override bool IsDBNull(int ordinal) => true;

        public override bool NextResult() => false;

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

        public override bool Read() => false;

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(false);

        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
    }
}
