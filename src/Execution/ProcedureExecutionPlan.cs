using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace Xtraq.Execution;

/// <summary>
/// Immutable description of how to execute a stored procedure and materialize its results.
/// </summary>
public sealed class ProcedureExecutionPlan
{
    // Fully qualified stored procedure name (schema + name). Generator now supplies an already bracketed, escaped identifier.
    /// <summary>
    /// Gets the fully qualified stored procedure name, including schema and identifier quoting.
    /// </summary>
    public string ProcedureName { get; }
    /// <summary>
    /// Gets the ordered parameters supplied to the stored procedure.
    /// </summary>
    public IReadOnlyList<ProcedureParameter> Parameters { get; }
    /// <summary>
    /// Gets the result set mappings used to materialize the procedure output.
    /// </summary>
    public IReadOnlyList<ResultSetMapping> ResultSets { get; }
    /// <summary>
    /// Gets the optional factory that produces an output object from collected output parameters.
    /// </summary>
    public Func<IReadOnlyDictionary<string, object?>, object?>? OutputFactory { get; }
    /// <summary>
    /// Gets the optional delegate that binds input parameter values onto the command.
    /// </summary>
    public Action<DbCommand, object?>? InputBinder { get; }
    /// <summary>
    /// Gets the delegate that aggregates execution results into the generated return type.
    /// </summary>
    public Func<bool, string?, object?, IReadOnlyDictionary<string, object?>, object[], object> AggregateFactory { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ProcedureExecutionPlan"/> class.
    /// </summary>
    /// <param name="procedureName">The fully qualified stored procedure name.</param>
    /// <param name="parameters">The parameters required by the stored procedure.</param>
    /// <param name="resultSets">The result set mappings that materialize the command results.</param>
    /// <param name="outputFactory">The optional factory used to build an output payload from output parameters.</param>
    /// <param name="aggregateFactory">The aggregate factory combining status, output, and result sets.</param>
    /// <param name="inputBinder">Optional binder that writes input parameter values to the command.</param>
    public ProcedureExecutionPlan(
        string procedureName,
        IReadOnlyList<ProcedureParameter> parameters,
        IReadOnlyList<ResultSetMapping> resultSets,
        Func<IReadOnlyDictionary<string, object?>, object?>? outputFactory,
        Func<bool, string?, object?, IReadOnlyDictionary<string, object?>, object[], object> aggregateFactory,
        Action<DbCommand, object?>? inputBinder = null)
    {
        // Name is assumed pre-bracketed by code generator (no further normalization here to avoid double quoting / hidden mutations).
        ProcedureName = procedureName;
        Parameters = parameters;
        ResultSets = resultSets;
        OutputFactory = outputFactory;
        AggregateFactory = aggregateFactory;
        InputBinder = inputBinder;
    }
}

/// <summary>
/// Describes a single stored procedure parameter.
/// </summary>
/// <param name="Name">The parameter name, including the <c>@</c> prefix.</param>
/// <param name="DbType">The optional database type for the parameter.</param>
/// <param name="Size">The optional size specification used for variable-length types.</param>
/// <param name="IsOutput">Indicates whether the parameter participates in output binding.</param>
/// <param name="IsNullable">Indicates whether the parameter accepts <c>null</c> values.</param>
public sealed record ProcedureParameter(string Name, DbType? DbType, int? Size, bool IsOutput, bool IsNullable);

/// <summary>
/// Captures the mapping logic required to materialize a stored procedure result set.
/// </summary>
/// <param name="Name">The descriptive name associated with the result set.</param>
/// <param name="Materializer">The asynchronous materializer that projects <see cref="DbDataReader"/> rows into objects.</param>
public sealed record ResultSetMapping(string Name, Func<DbDataReader, CancellationToken, Task<IReadOnlyList<object>>> Materializer);

/// <summary>
/// Executes stored procedure plans using the ambient database connection.
/// </summary>
public static class ProcedureExecutor
{
    private static readonly object InterceptorSync = new();
    private static IXtraqProcedureInterceptor[] _globalInterceptors = Array.Empty<IXtraqProcedureInterceptor>();

    /// <summary>Sets the global interceptor list, replacing previously registered instances.</summary>
    /// <param name="interceptor">The interceptor instance to observe procedure lifecycle events.</param>
    public static void SetInterceptor(IXtraqProcedureInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);

        lock (InterceptorSync)
        {
            _globalInterceptors = new[] { interceptor };
        }
    }

    /// <summary>Adds an additional global interceptor without replacing previously registered instances.</summary>
    /// <param name="interceptor">The interceptor instance to append.</param>
    public static void AddInterceptor(IXtraqProcedureInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);

        lock (InterceptorSync)
        {
            var snapshot = _globalInterceptors;
            var newArray = new IXtraqProcedureInterceptor[snapshot.Length + 1];
            Array.Copy(snapshot, newArray, snapshot.Length);
            newArray[^1] = interceptor;
            _globalInterceptors = newArray;
        }
    }

    /// <summary>Clears all globally registered interceptors.</summary>
    public static void ClearInterceptors()
    {
        lock (InterceptorSync)
        {
            _globalInterceptors = Array.Empty<IXtraqProcedureInterceptor>();
        }
    }

    /// <summary>
    /// Executes the provided procedure plan and returns the generated aggregate value.
    /// </summary>
    /// <typeparam name="TAggregate">The aggregate type produced by the execution plan.</typeparam>
    /// <param name="connection">The database connection used to execute the stored procedure.</param>
    /// <param name="plan">The execution plan describing parameters, result sets, and aggregation.</param>
    /// <param name="state">Optional state object propagated to the interceptor and input binder.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The aggregate value produced by the execution pipeline.</returns>
    public static Task<TAggregate> ExecuteAsync<TAggregate>(DbConnection connection, ProcedureExecutionPlan plan, object? state = null, CancellationToken cancellationToken = default)
    {
        return ExecuteAsync<TAggregate>(null, connection, plan, state, cancellationToken);
    }

    /// <summary>
    /// Executes the provided procedure plan using the supplied context-aware interceptors.
    /// </summary>
    /// <typeparam name="TAggregate">The aggregate type produced by the execution plan.</typeparam>
    /// <param name="interceptorProvider">Optional interceptor provider supplying scoped interceptors.</param>
    /// <param name="connection">The database connection used to execute the stored procedure.</param>
    /// <param name="plan">The execution plan describing parameters, result sets, and aggregation.</param>
    /// <param name="state">Optional state object propagated to the interceptor and input binder.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The aggregate value produced by the execution pipeline.</returns>
    public static async Task<TAggregate> ExecuteAsync<TAggregate>(IXtraqProcedureInterceptorProvider? interceptorProvider, DbConnection connection, ProcedureExecutionPlan plan, object? state = null, CancellationToken cancellationToken = default)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = plan.ProcedureName;
        cmd.CommandType = CommandType.StoredProcedure;
        foreach (var p in plan.Parameters)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = p.Name;
            if (p.DbType.HasValue) param.DbType = p.DbType.Value;
            if (p.Size.HasValue) param.Size = p.Size.Value;
            param.Direction = p.IsOutput ? ParameterDirection.InputOutput : ParameterDirection.Input;
            if (!p.IsOutput)
            {
                // Value binding is deferred to generated wrapper; here default to DBNull (overridden by wrapper before execute);
                param.Value = DBNull.Value;
            }
            cmd.Parameters.Add(param);
        }

        var interceptors = await InvokeBeforeAsync(interceptorProvider, plan.ProcedureName, cmd, state, cancellationToken).ConfigureAwait(false);
        var start = DateTime.UtcNow;
        try
        {
            if (connection.State != ConnectionState.Open) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            // Bind input values (if any) before execution
            plan.InputBinder?.Invoke(cmd, state); // wrapper supplies state (input record) if available

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var resultSetResults = new List<object>(plan.ResultSets.Count);
            for (int i = 0; i < plan.ResultSets.Count; i++)
            {
                var map = plan.ResultSets[i];
                var list = await map.Materializer(reader, cancellationToken).ConfigureAwait(false);
                resultSetResults.Add(list);
                if (i < plan.ResultSets.Count - 1)
                {
                    if (!await reader.NextResultAsync(cancellationToken).ConfigureAwait(false)) break;
                }
            }
            // Collect output parameter values
            var outputValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in plan.Parameters)
            {
                if (p.IsOutput)
                {
                    var val = cmd.Parameters[p.Name].Value;
                    outputValues[p.Name.TrimStart('@')] = val == DBNull.Value ? null : val;
                }
            }
            object? outputObj = plan.OutputFactory?.Invoke(outputValues);
            var rsArray = resultSetResults.ToArray();
            var aggregateObj = plan.AggregateFactory(true, null, outputObj, outputValues, rsArray);
            var duration = DateTime.UtcNow - start;
            await NotifyAfterAsync(interceptors, plan.ProcedureName, cmd, true, null, duration, aggregateObj, cancellationToken).ConfigureAwait(false);
            return (TAggregate)aggregateObj;
        }
        catch (Exception ex)
        {
            var aggregateObj = plan.AggregateFactory(false, ex.Message, null, new Dictionary<string, object?>(), Array.Empty<object>());
            var duration = DateTime.UtcNow - start;
            try
            {
                await NotifyAfterAsync(interceptors, plan.ProcedureName, cmd, false, ex.Message, duration, aggregateObj, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // swallow interceptor errors
            }
            return (TAggregate)aggregateObj;
        }
    }

    /// <summary>
    /// Streams a single result set without materialising the aggregate payload.
    /// The specified <paramref name="streamAction"/> receives the active <see cref="DbDataReader"/> positioned on the desired result set and is responsible for consuming the rows.
    /// </summary>
    /// <param name="connection">The open or closed database connection used to execute the stored procedure.</param>
    /// <param name="plan">The execution plan describing parameters and materialisers for the stored procedure.</param>
    /// <param name="resultSetIndex">Zero-based index of the result set to stream.</param>
    /// <param name="streamAction">Delegate that processes the requested result set by consuming rows from the provided reader.</param>
    /// <param name="state">Optional state propagated to the input binder and interceptor pipeline.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The typed output payload produced by the plan's <see cref="ProcedureExecutionPlan.OutputFactory"/>, or <c>null</c> when the procedure does not emit output parameters.</returns>
    public static Task<object?> StreamResultSetAsync(DbConnection connection, ProcedureExecutionPlan plan, int resultSetIndex, Func<DbDataReader, CancellationToken, Task> streamAction, object? state = null, CancellationToken cancellationToken = default)
    {
        return StreamResultSetAsync(null, connection, plan, resultSetIndex, streamAction, state, cancellationToken);
    }

    /// <summary>
    /// Streams a result set using the specified context-aware interceptors.
    /// </summary>
    /// <param name="interceptorProvider">Optional interceptor provider supplying scoped interceptors.</param>
    /// <param name="connection">The open or closed database connection used to execute the stored procedure.</param>
    /// <param name="plan">The execution plan describing parameters and materialisers for the stored procedure.</param>
    /// <param name="resultSetIndex">Zero-based index of the result set to stream.</param>
    /// <param name="streamAction">Delegate that processes the requested result set by consuming rows from the provided reader.</param>
    /// <param name="state">Optional state propagated to the input binder and interceptor pipeline.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The typed output payload produced by the plan's <see cref="ProcedureExecutionPlan.OutputFactory"/>, or <c>null</c> when the procedure does not emit output parameters.</returns>
    public static async Task<object?> StreamResultSetAsync(IXtraqProcedureInterceptorProvider? interceptorProvider, DbConnection connection, ProcedureExecutionPlan plan, int resultSetIndex, Func<DbDataReader, CancellationToken, Task> streamAction, object? state = null, CancellationToken cancellationToken = default)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (plan == null) throw new ArgumentNullException(nameof(plan));
        if (streamAction == null) throw new ArgumentNullException(nameof(streamAction));
        if (resultSetIndex < 0 || resultSetIndex >= plan.ResultSets.Count) throw new ArgumentOutOfRangeException(nameof(resultSetIndex));

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = plan.ProcedureName;
        cmd.CommandType = CommandType.StoredProcedure;
        foreach (var p in plan.Parameters)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = p.Name;
            if (p.DbType.HasValue) param.DbType = p.DbType.Value;
            if (p.Size.HasValue) param.Size = p.Size.Value;
            param.Direction = p.IsOutput ? ParameterDirection.InputOutput : ParameterDirection.Input;
            if (!p.IsOutput)
            {
                param.Value = DBNull.Value;
            }
            cmd.Parameters.Add(param);
        }

        var interceptors = await InvokeBeforeAsync(interceptorProvider, plan.ProcedureName, cmd, state, cancellationToken).ConfigureAwait(false);
        var start = DateTime.UtcNow;
        try
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            plan.InputBinder?.Invoke(cmd, state);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            for (int i = 0; i < plan.ResultSets.Count; i++)
            {
                if (i == resultSetIndex)
                {
                    await streamAction(reader, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        // Intentionally discard rows for non-target result sets.
                    }
                }

                if (i < plan.ResultSets.Count - 1)
                {
                    if (!await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }
                }
            }

            var outputValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in plan.Parameters)
            {
                if (!p.IsOutput)
                {
                    continue;
                }

                var value = cmd.Parameters[p.Name].Value;
                outputValues[p.Name.TrimStart('@')] = value == DBNull.Value ? null : value;
            }

            var outputObj = plan.OutputFactory?.Invoke(outputValues);
            var duration = DateTime.UtcNow - start;
            await NotifyAfterAsync(interceptors, plan.ProcedureName, cmd, true, null, duration, outputObj, cancellationToken).ConfigureAwait(false);
            return outputObj;
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - start;
            try
            {
                await NotifyAfterAsync(interceptors, plan.ProcedureName, cmd, false, ex.Message, duration, null, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore interceptor failures during exception propagation.
            }

            throw;
        }
    }

    private static async Task<List<(IXtraqProcedureInterceptor Interceptor, object? State)>?> InvokeBeforeAsync(
        IXtraqProcedureInterceptorProvider? interceptorProvider,
        string procedureName,
        DbCommand command,
        object? state,
        CancellationToken cancellationToken)
    {
        var interceptors = CollectInterceptors(interceptorProvider);
        if (interceptors.Count == 0)
        {
            return null;
        }

        var results = new List<(IXtraqProcedureInterceptor, object?)>(interceptors.Count);
        foreach (var interceptor in interceptors)
        {
            if (interceptor is null)
            {
                continue;
            }

            try
            {
                var beforeState = await interceptor.OnBeforeExecuteAsync(procedureName, command, state, cancellationToken).ConfigureAwait(false);
                results.Add((interceptor, beforeState));
            }
            catch
            {
                // Skip this interceptor on failure to avoid affecting the execution pipeline.
            }
        }

        return results.Count == 0 ? null : results;
    }

    private static async Task NotifyAfterAsync(
        List<(IXtraqProcedureInterceptor Interceptor, object? State)>? interceptors,
        string procedureName,
        DbCommand command,
        bool success,
        string? error,
        TimeSpan duration,
        object? aggregate,
        CancellationToken cancellationToken)
    {
        if (interceptors is null || interceptors.Count == 0)
        {
            return;
        }

        foreach (var (interceptor, beforeState) in interceptors)
        {
            try
            {
                await interceptor.OnAfterExecuteAsync(procedureName, command, success, error, duration, beforeState, aggregate, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore interceptor failures to avoid breaking the execution pipeline.
            }
        }
    }

    private static List<IXtraqProcedureInterceptor> CollectInterceptors(IXtraqProcedureInterceptorProvider? interceptorProvider)
    {
        var snapshot = Volatile.Read(ref _globalInterceptors);
        var globalCount = snapshot.Length;

        List<IXtraqProcedureInterceptor>? scoped = null;
        if (interceptorProvider is not null)
        {
            var scopedInterceptors = interceptorProvider.GetInterceptors();
            if (scopedInterceptors is { Count: > 0 })
            {
                scoped = new List<IXtraqProcedureInterceptor>(scopedInterceptors.Count);
                for (var i = 0; i < scopedInterceptors.Count; i++)
                {
                    var interceptor = scopedInterceptors[i];
                    if (interceptor is not null)
                    {
                        scoped.Add(interceptor);
                    }
                }
            }
        }

        if (globalCount == 0 && (scoped is null || scoped.Count == 0))
        {
            return scoped ?? new List<IXtraqProcedureInterceptor>(capacity: 0);
        }

        if (scoped is null || scoped.Count == 0)
        {
            return new List<IXtraqProcedureInterceptor>(snapshot);
        }

        var combined = new List<IXtraqProcedureInterceptor>(globalCount + scoped.Count);
        combined.AddRange(snapshot);
        combined.AddRange(scoped);
        return combined;
    }
}
