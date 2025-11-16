using System.Data;
using System.Data.Common;

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
    private static IXtraqProcedureInterceptor _interceptor = new NoOpProcedureInterceptor();

    /// <summary>Sets a global interceptor. Thread-safe overwrite; expected rarely (e.g. application startup).</summary>
    /// <param name="interceptor">The interceptor instance to observe procedure lifecycle events.</param>
    public static void SetInterceptor(IXtraqProcedureInterceptor interceptor) => _interceptor = interceptor ?? new NoOpProcedureInterceptor();

    /// <summary>
    /// Executes the provided procedure plan and returns the generated aggregate value.
    /// </summary>
    /// <typeparam name="TAggregate">The aggregate type produced by the execution plan.</typeparam>
    /// <param name="connection">The database connection used to execute the stored procedure.</param>
    /// <param name="plan">The execution plan describing parameters, result sets, and aggregation.</param>
    /// <param name="state">Optional state object propagated to the interceptor and input binder.</param>
    /// <param name="cancellationToken">Token used to observe cancellation requests.</param>
    /// <returns>The aggregate value produced by the execution pipeline.</returns>
    public static async Task<TAggregate> ExecuteAsync<TAggregate>(DbConnection connection, ProcedureExecutionPlan plan, object? state = null, CancellationToken cancellationToken = default)
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

        object? beforeState = null;
        var start = DateTime.UtcNow;
        try
        {
            if (connection.State != ConnectionState.Open) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            // Bind input values (if any) before execution
            plan.InputBinder?.Invoke(cmd, state); // wrapper supplies state (input record) if available

            beforeState = await _interceptor.OnBeforeExecuteAsync(plan.ProcedureName, cmd, state, cancellationToken).ConfigureAwait(false);

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
            await _interceptor.OnAfterExecuteAsync(plan.ProcedureName, cmd, true, null, duration, beforeState, aggregateObj, cancellationToken).ConfigureAwait(false);
            return (TAggregate)aggregateObj;
        }
        catch (Exception ex)
        {
            var aggregateObj = plan.AggregateFactory(false, ex.Message, null, new Dictionary<string, object?>(), Array.Empty<object>());
            var duration = DateTime.UtcNow - start;
            try { await _interceptor.OnAfterExecuteAsync(plan.ProcedureName, cmd, false, ex.Message, duration, beforeState, aggregateObj, cancellationToken).ConfigureAwait(false); } catch { /* swallow interceptor errors */ }
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
    public static async Task<object?> StreamResultSetAsync(DbConnection connection, ProcedureExecutionPlan plan, int resultSetIndex, Func<DbDataReader, CancellationToken, Task> streamAction, object? state = null, CancellationToken cancellationToken = default)
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

        object? beforeState = null;
        var start = DateTime.UtcNow;
        try
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            plan.InputBinder?.Invoke(cmd, state);

            beforeState = await _interceptor.OnBeforeExecuteAsync(plan.ProcedureName, cmd, state, cancellationToken).ConfigureAwait(false);

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
            await _interceptor.OnAfterExecuteAsync(plan.ProcedureName, cmd, true, null, duration, beforeState, outputObj, cancellationToken).ConfigureAwait(false);
            return outputObj;
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - start;
            try
            {
                await _interceptor.OnAfterExecuteAsync(plan.ProcedureName, cmd, false, ex.Message, duration, beforeState, null, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore interceptor failures during exception propagation.
            }

            throw;
        }
    }
}
