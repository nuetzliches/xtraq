using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Xtraq.Services;
using Xtraq.Telemetry;

namespace Xtraq.Data;

internal class DbContext(IConsoleService consoleService, IDatabaseTelemetryCollector telemetryCollector) : IDisposable
{
    private SqlConnection? _connection;
    private List<AppSqlTransaction>? _transactions;
    private string? _connectionString;

    public void SetConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));
        }
        _connectionString = connectionString;

        if (_transactions?.Count > 0)
        {
            foreach (var transaction in _transactions.ToArray())
            {
                try
                {
                    RollbackTransaction(transaction);
                }
                catch (Exception ex)
                {
                    consoleService.Verbose($"[dbctx] rollback during reconfigure failed: {ex.Message}");
                }
            }
        }

        if (_connection != null)
        {
            try
            {
                if (_connection.State != ConnectionState.Closed)
                {
                    _connection.Close();
                }
            }
            catch (Exception ex)
            {
                consoleService.Verbose($"[dbctx] close during reconfigure failed: {ex.Message}");
            }
            finally
            {
                _connection.Dispose();
            }
        }

        _transactions = [];
        _connection = new SqlConnection(connectionString);
    }

    public void Dispose()
    {
        if (_connection?.State == ConnectionState.Open)
        {
            if (_transactions?.Any() == true)
            {
                _transactions.ToList().ForEach(RollbackTransaction);
            }

            _connection.Close();
        }

        _connection?.Dispose();
    }

    public async Task<List<T>> ExecuteListAsync<T>(
        string procedureName,
        IReadOnlyList<SqlParameter>? parameters,
        CancellationToken cancellationToken = default,
        AppSqlTransaction? transaction = null,
        string? telemetryOperation = null,
        string? telemetryCategory = null,
        [CallerMemberName] string caller = "",
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = 0) where T : class, new()
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = new List<T>();
        var telemetryScope = telemetryCollector.StartQuery(new DatabaseQueryTelemetryMetadata(
            Operation: telemetryOperation ?? (string.IsNullOrWhiteSpace(caller) ? "ExecuteListAsync" : caller),
            Category: telemetryCategory ?? "DbContext.StoredProcedure",
            CommandText: procedureName,
            CommandType: CommandType.StoredProcedure,
            ParameterCount: parameters?.Count ?? 0,
            Caller: caller,
            FilePath: filePath,
            LineNumber: lineNumber == 0 ? null : lineNumber));
        var rows = 0;

        try
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection string not configured.");
            }

            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            using var command = new SqlCommand(procedureName, _connection)
            {
                CommandType = CommandType.StoredProcedure,
                Transaction = transaction?.Transaction ?? GetCurrentTransaction()?.Transaction
            };

            if (parameters is { Count: > 0 })
            {
                command.Parameters.AddRange(parameters.ToArray());
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                result.Add(reader.ConvertToObject<T>());
                rows++;
            }

            if ((_transactions?.Any() ?? false) == false)
            {
                _connection.Close();
            }

            telemetryScope.MarkCompleted(rows);
        }
        catch (Exception ex)
        {
            consoleService.Error($"Error in ExecuteListAsync for {procedureName}: {ex.Message}");
            telemetryScope.MarkFailed(rows, ex);
            throw;
        }
        finally
        {
            telemetryScope.Dispose();
        }

        return result;
    }

    public async Task<T?> ExecuteSingleAsync<T>(
        string procedureName,
        IReadOnlyList<SqlParameter>? parameters,
        CancellationToken cancellationToken = default,
        AppSqlTransaction? transaction = null,
        string? telemetryOperation = null,
        string? telemetryCategory = null,
        [CallerMemberName] string caller = "",
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = 0) where T : class, new()
    {
        var list = await ExecuteListAsync<T>(
            procedureName,
            parameters,
            cancellationToken,
            transaction,
            telemetryOperation,
            telemetryCategory,
            caller,
            filePath,
            lineNumber).ConfigureAwait(false);
        return list.SingleOrDefault();
    }

    public async Task<List<T>> ListAsync<T>(
        string queryString,
        IReadOnlyList<SqlParameter>? parameters,
        CancellationToken cancellationToken = default,
        AppSqlTransaction? transaction = null,
        string? telemetryOperation = null,
        string? telemetryCategory = null,
        [CallerMemberName] string caller = "",
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = 0) where T : class, new()
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = new List<T>();
        var telemetryScope = telemetryCollector.StartQuery(new DatabaseQueryTelemetryMetadata(
            Operation: telemetryOperation ?? (string.IsNullOrWhiteSpace(caller) ? "ListAsync" : caller),
            Category: telemetryCategory ?? "DbContext.Query",
            CommandText: queryString,
            CommandType: CommandType.Text,
            ParameterCount: parameters?.Count ?? 0,
            Caller: caller,
            FilePath: filePath,
            LineNumber: lineNumber == 0 ? null : lineNumber));
        var rows = 0;

        var intercepted = await OnListAsync<T>(queryString, parameters, cancellationToken, transaction).ConfigureAwait(false);
        if (intercepted != null)
        {
            telemetryScope.MarkIntercepted(intercepted.Count);
            telemetryScope.Dispose();
            return intercepted;
        }

        try
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection string not configured.");
            }

            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            using var command = new SqlCommand(queryString, _connection)
            {
                CommandType = CommandType.Text,
                Transaction = transaction?.Transaction ?? GetCurrentTransaction()?.Transaction
            };

            if (parameters is { Count: > 0 })
            {
                command.Parameters.AddRange(parameters.ToArray());
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                result.Add(reader.ConvertToObject<T>());
                rows++;
            }

            if ((_transactions?.Any() ?? false) == false)
            {
                _connection.Close();
            }

            telemetryScope.MarkCompleted(rows);
        }
        catch (Exception ex)
        {
            consoleService.Error($"Error in ListAsync for query: {ex.Message}");
            telemetryScope.MarkFailed(rows, ex);
            throw;
        }
        finally
        {
            telemetryScope.Dispose();
        }

        return result;
    }

    protected virtual Task<List<T>?> OnListAsync<T>(string queryString, IReadOnlyList<SqlParameter>? parameters, CancellationToken cancellationToken, AppSqlTransaction? transaction) where T : class, new()
    {
        return Task.FromResult<List<T>?>(null);
    }

    public async Task<T?> SingleAsync<T>(
        string queryString,
        IReadOnlyList<SqlParameter>? parameters,
        CancellationToken cancellationToken = default,
        AppSqlTransaction? transaction = null,
        string? telemetryOperation = null,
        string? telemetryCategory = null,
        [CallerMemberName] string caller = "",
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = 0) where T : class, new()
    {
        var list = await ListAsync<T>(
            queryString,
            parameters,
            cancellationToken,
            transaction,
            telemetryOperation,
            telemetryCategory,
            caller,
            filePath,
            lineNumber).ConfigureAwait(false);
        return list.SingleOrDefault();
    }

    public async Task<AppSqlTransaction> BeginTransactionAsync(string transactionName, CancellationToken cancellationToken = default)
    {
        if (_connection == null)
        {
            throw new InvalidOperationException("Connection string not configured.");
        }

        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }

        var transaction = new AppSqlTransaction { Transaction = _connection.BeginTransaction(transactionName) };
        _transactions ??= [];
        _transactions.Add(transaction);
        return transaction;
    }

    public void CommitTransaction(AppSqlTransaction transaction)
    {
        if (_transactions == null)
        {
            return;
        }

        var existing = _transactions.SingleOrDefault(t => t.Equals(transaction));
        if (existing == null)
        {
            return;
        }

        existing.Transaction.Commit();
        _transactions.Remove(existing);
    }

    public void RollbackTransaction(AppSqlTransaction transaction)
    {
        if (_transactions == null)
        {
            return;
        }

        var existing = _transactions.SingleOrDefault(t => t.Equals(transaction));
        if (existing == null)
        {
            return;
        }

        existing.Transaction.Rollback();
        _transactions.Remove(existing);
    }

    public AppSqlTransaction? GetCurrentTransaction()
    {
        return _transactions?.LastOrDefault();
    }

    internal string? GetConnectionString() => _connection?.ConnectionString ?? _connectionString;

    public static SqlDbType GetSqlDbType(object? value) => value switch
    {
        int => SqlDbType.Int,
        long => SqlDbType.BigInt,
        string => SqlDbType.NVarChar,
        bool => SqlDbType.Bit,
        DateTime => SqlDbType.DateTime2,
        Guid => SqlDbType.UniqueIdentifier,
        decimal => SqlDbType.Decimal,
        double => SqlDbType.Float,
        byte[] => SqlDbType.VarBinary,
        null => SqlDbType.NVarChar,
        _ => throw new ArgumentOutOfRangeException($"{nameof(DbContext)}.{nameof(GetSqlDbType)} - System.Type {value.GetType()} not defined!")
    };

    public sealed class AppSqlTransaction
    {
        public SqlTransaction Transaction { get; set; } = null!;
    }
}

internal static class DbContextServiceCollectionExtensions
{
    public static IServiceCollection AddDbContext(this IServiceCollection services)
    {
        services.AddSingleton(provider => new DbContext(
            provider.GetRequiredService<IConsoleService>(),
            provider.GetRequiredService<IDatabaseTelemetryCollector>()));
        return services;
    }
}
