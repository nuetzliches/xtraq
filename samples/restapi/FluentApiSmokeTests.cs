using Xtraq.Samples.RestApi.Xtraq;
using Xtraq.Samples.RestApi.Xtraq.Sample;

namespace Xtraq.Samples.RestApi;

/// <summary>Compile-time smoke tests for the Xtraq fluent API to guard type inference/overloads.</summary>
internal static class FluentApiSmokeTests
{
    internal static ValueTask<UserListResult> UserListPipelineAsync(IXtraqDbContext db, CancellationToken ct = default)
    {
        return db.ConfigureProcedure(new UserListInput(false))
            .WithTransaction()
            .WithLabel("user-list-pipeline")
            .WithExecutor((ctx, input, token) => new ValueTask<UserListResult>(ctx.UserListAsync(input, token)))
            .ExecuteAsync(ct);
    }

    internal static ValueTask<UserListResult> UserListPipeline2Async(IXtraqDbContext db, CancellationToken ct = default)
    {
        return db.ConfigureProcedure(new UserListInput(false))
            .ExecuteAsync(async (ctx, input, token) => await ctx.UserListAsync(input, token), ct);
    }

    internal static ValueTask<UserListResult> UserListPipeline3Async(IXtraqDbContext db, CancellationToken ct = default)
    {
        return db.ConfigureProcedure(new UserListInput(false))
            .ExecuteAsync((ctx, input, token) => new ValueTask<UserListResult>(ctx.UserListAsync(input, token)));
    }

    //internal static async Task<UserListResult> UserListPipeline4Async(IXtraqDbContext db, CancellationToken ct = default)
    //{
    //    var result = await db.ConfigureProcedure(new UserListInput(false))
    //        .ExecuteAsync(ctx => ctx.UserListAsync, ct);

    //    return result.Result;
    //}

    internal static async ValueTask<UserOrderInsightsResult> UserOrderInsightsPipelineAsync(IXtraqDbContext db, int userId, CancellationToken ct = default)
    {
        var request = new UserOrderInsightsRequest { UserId = userId };
        var pipeline = await db.ConfigureProcedure(
            request,
            (req, context, token) => UserOrderInsightsRequestMapper.ToInputAsync(req, context, token),
            ct).ConfigureAwait(false);

        return await pipeline
            .WithTransaction()
            .WithLabel("user-order-insights-pipeline")
            .WithExecutor((ctx, input, token) => new ValueTask<UserOrderInsightsResult>(ctx.UserOrderInsightsAsync(input, token)))
            .ExecuteAsync(ct)
            .ConfigureAwait(false);
    }
}
