using System.Data.Common;

namespace Xtraq.Execution;

/// <summary>
/// Interceptor hook points for procedure execution. Implementations can log, trace, or mutate parameters before execution.
/// Lifecycle: OnBeforeExecute (timing start) -> ProcedureExecutor core -> OnAfterExecute (success/failure + duration).
/// </summary>
public interface IXtraqProcedureInterceptor
{
    /// <summary>Called before the command is executed. Return a state object carried into <see cref="OnAfterExecuteAsync(string, DbCommand, bool, string?, TimeSpan, object?, object?, CancellationToken)"/>.</summary>
    Task<object?> OnBeforeExecuteAsync(string procedureName, DbCommand command, object? state, CancellationToken cancellationToken);
    /// <summary>Called after execution completes (success or failure). Receives state from <see cref="OnBeforeExecuteAsync"/>.</summary>
    Task OnAfterExecuteAsync(string procedureName, DbCommand command, bool success, string? error, TimeSpan duration, object? beforeState, object? aggregate, CancellationToken cancellationToken);
}
