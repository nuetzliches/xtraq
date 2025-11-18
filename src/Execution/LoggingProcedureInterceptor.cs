using System.Data.Common;
using Microsoft.Extensions.Logging;

namespace Xtraq.Execution;

/// <summary>
/// Logging interceptor capturing duration, success flag and optional error. Designed for lightweight structured logging without throwing.
/// Register via <c>ProcedureExecutor.AddInterceptor(new LoggingProcedureInterceptor(logger));</c> during application startup.
/// </summary>
public sealed class LoggingProcedureInterceptor : IXtraqProcedureInterceptor
{
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="LoggingProcedureInterceptor"/> class.
    /// </summary>
    /// <param name="logger">The logger that receives structured execution events.</param>
    public LoggingProcedureInterceptor(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task<object?> OnBeforeExecuteAsync(string procedureName, DbCommand command, object? state, CancellationToken cancellationToken)
    {
        // Capture high-resolution timestamp for duration fallback (executor also measures but we keep our own for completeness)
        return Task.FromResult<object?>(Stopwatch.GetTimestamp());
    }

    /// <inheritdoc />
    public Task OnAfterExecuteAsync(string procedureName, DbCommand command, bool success, string? error, TimeSpan duration, object? beforeState, object? aggregate, CancellationToken cancellationToken)
    {
        try
        {
            var paramCount = command.Parameters.Count;
            // Avoid enumerating potentially large result sets; log only metadata
            if (success)
            {
                _logger.LogInformation("xtraq.proc.executed {Procedure} duration_ms={DurationMs} params={ParamCount} success={Success}", procedureName, duration.TotalMilliseconds, paramCount, true);
            }
            else
            {
                _logger.LogWarning("xtraq.proc.failed {Procedure} duration_ms={DurationMs} params={ParamCount} success={Success} error={Error}", procedureName, duration.TotalMilliseconds, paramCount, false, error);
            }
        }
        catch
        {
            // Swallow logging exceptions to not affect execution path
        }
        return Task.CompletedTask;
    }
}
