
#nullable enable
namespace Xtraq.Samples.RestApi.Xtraq;

using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

public interface IXtraqDbContext
{
    /// <summary>Create and open a new connection (synchronous).</summary>
    DbConnection OpenConnection();
    /// <summary>Create and open a new connection asynchronously.</summary>
    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);
    /// <summary>Simple health probe (true = can open & close a connection).</summary>
    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);
    /// <summary>Configured command timeout in seconds (never &lt; 1).</summary>
    int CommandTimeout { get; }
}
