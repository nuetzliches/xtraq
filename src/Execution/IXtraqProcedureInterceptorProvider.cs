using System.Collections.Generic;

namespace Xtraq.Execution;

/// <summary>
/// Provides additional procedure interceptors that should run alongside globally registered instances.
/// Implement this interface on <see cref="IXtraqDbContext"/> implementations that need scoped interception behaviour.
/// </summary>
public interface IXtraqProcedureInterceptorProvider
{
    /// <summary>
    /// Gets the interceptors that should participate in the next command execution.
    /// </summary>
    IReadOnlyList<IXtraqProcedureInterceptor> GetInterceptors();
}
