/// <summary>Generated minimal API endpoints for XtraqDbContext. (net10 variant)</summary>
namespace Xtraq.Samples.RestApi.Xtraq;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using System.Threading.Tasks;

public static class XtraqDbContextEndpointRouteBuilderExtensions
{
    /// <summary>Maps health endpoint for DB connectivity: GET /xtraq/health/db (200 ok / 503 problem).</summary>
    public static IEndpointRouteBuilder MapXtraqDbContextEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/xtraq/health/db", async (IXtraqDbContext db, CancellationToken ct) =>
        {
            var healthy = await db.HealthCheckAsync(ct).ConfigureAwait(false);
            return healthy ? Results.Ok(new { status = "ok" }) : Results.Problem("database unavailable", statusCode: 503);
        });
        return endpoints;
    }
}