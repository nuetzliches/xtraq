using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.Data.SqlClient;
using global::Xtraq.Samples.RestApi.Xtraq;
using global::Xtraq.Samples.RestApi.Xtraq.Sample;

var builder = WebApplication.CreateBuilder(args);

var configuredConnection = builder.Configuration.GetConnectionString("SampleDb");
var environmentConnection = Environment.GetEnvironmentVariable("XTRAQ_SAMPLE_SQL_CONNECTION");
var connectionString = string.IsNullOrWhiteSpace(environmentConnection) ? configuredConnection : environmentConnection;

if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException("Connection string 'SampleDb' is not configured. Set it in appsettings.json or provide XTRAQ_SAMPLE_SQL_CONNECTION.");
}

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddXtraqDbContext(options =>
{
    options.ConnectionString = connectionString;
    options.CommandTimeout = 30;
});

var app = builder.Build();

app.UseHttpsRedirection();

app.MapGet("/", () => Results.Ok(new { status = "ok", utc = DateTime.UtcNow }))
   .WithName("RootPing");

app.MapGet("/api/users", async (IXtraqDbContext db, CancellationToken ct) =>
{
    try
    {
        var result = await db.UserListAsync(new UserListInput(null), ct).ConfigureAwait(false);
        var users = result.Result
            .Select(r => SampleApiHelpers.CreateUserSummary(r.UserId, r.Alias, r.DisplayName, r.Email, r.IsActive, r.PreferredLocale, r.CreatedAtUtc))
            .ToList();
        return Results.Ok(new UsersResponse(users.Count, users));
    }
    catch (SqlException ex)
    {
        return Results.Problem("Database unavailable", statusCode: StatusCodes.Status503ServiceUnavailable, extensions: SqlProblemPayload.From(ex));
    }
})
.WithName("UserDirectory");

app.MapGet("/api/users/{userId:int}/orders", async (int userId, IXtraqDbContext db, CancellationToken ct) =>
{
    try
    {
        var result = await db.UserDetailsWithOrdersAsync(new UserDetailsWithOrdersInput(userId), ct).ConfigureAwait(false);

        if (result.Result.Count == 0)
        {
            return Results.NotFound(new { error = $"User {userId} was not found." });
        }

        var summary = SampleApiHelpers.CreateUserSummary(result.Result[0].UserId, result.Result[0].Alias, result.Result[0].DisplayName, result.Result[0].Email, result.Result[0].IsActive, result.Result[0].PreferredLocale, result.Result[0].CreatedAtUtc);
        var orders = result.Result1.Select(SampleApiHelpers.MapOrder).ToList();
        return Results.Ok(new OrdersResponse(summary.UserId, summary.DisplayName, orders.Count, orders));
    }
    catch (SqlException ex)
    {
        return Results.Problem("Failed to retrieve orders", statusCode: StatusCodes.Status503ServiceUnavailable, extensions: SqlProblemPayload.From(ex));
    }
})
.WithName("UserOrders");

app.MapPost("/api/users/{userId:int}/orders", async (int userId, CreateOrderRequest request, IXtraqDbContext db, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.OrderNumber) || string.IsNullOrWhiteSpace(request.Currency) || request.TotalAmount <= 0)
    {
        return Results.BadRequest(new { error = "OrderNumber, Currency and positive TotalAmount are required." });
    }

    try
    {
        var userResult = await db.UserFindAsync(new UserFindInput(userId), ct).ConfigureAwait(false);
        if (userResult.Result.Count == 0)
        {
            return Results.NotFound(new { error = $"User {userId} was not found." });
        }

        await using var connection = await db.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
			INSERT INTO sample.Orders (UserId, OrderNumber, Status, TotalAmount, Currency, PlacedAtUtc, RequiredAtUtc)
			OUTPUT INSERTED.OrderId
			VALUES (@UserId, @OrderNumber, @Status, @TotalAmount, @Currency, SYSUTCDATETIME(), @RequiredAtUtc);
			""";
        command.CommandType = CommandType.Text;
        command.CommandTimeout = db.CommandTimeout;

        SampleApiHelpers.AddParameter(command, "@UserId", userId, DbType.Int32);
        SampleApiHelpers.AddParameter(command, "@OrderNumber", request.OrderNumber, DbType.String);
        SampleApiHelpers.AddParameter(command, "@Status", request.Status ?? "Pending", DbType.String);
        SampleApiHelpers.AddParameter(command, "@TotalAmount", request.TotalAmount, DbType.Decimal);
        SampleApiHelpers.AddParameter(command, "@Currency", request.Currency, DbType.String);
        SampleApiHelpers.AddParameter(command, "@RequiredAtUtc", request.RequiredAtUtc, DbType.DateTime2);

        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        var orderId = Convert.ToInt32(scalar, CultureInfo.InvariantCulture);

        return Results.Created($"/api/users/{userId}/orders/{orderId}", new { orderId, request.OrderNumber, request.Currency, request.TotalAmount });
    }
    catch (SqlException ex) when (ex.Number is 547)
    {
        return Results.NotFound(new { error = $"User {userId} was not found." });
    }
    catch (SqlException ex) when (ex.Number is 2627 or 2601)
    {
        return Results.Conflict(new { error = "OrderNumber must be unique." });
    }
    catch (SqlException ex)
    {
        return Results.Problem("Failed to create order", statusCode: StatusCodes.Status503ServiceUnavailable, extensions: SqlProblemPayload.From(ex));
    }
})
.WithName("CreateOrder");

app.Run();

internal static class SqlProblemPayload
{
    public static IDictionary<string, object?> From(SqlException exception)
        => new Dictionary<string, object?>
        {
            ["sqlNumber"] = exception.Number,
            ["sqlState"] = exception.State,
            ["server"] = exception.Server,
            ["procedure"] = exception.Procedure,
            ["lineNumber"] = exception.LineNumber
        };
}

internal sealed record UsersResponse(int Count, IReadOnlyList<UserSummary> Items);

internal sealed record UserSummary(int UserId, string? Alias, string DisplayName, string? Email, bool IsActive, string PreferredLocale, DateTime CreatedAtUtc);

internal sealed record OrdersResponse(int UserId, string DisplayName, int Count, IReadOnlyList<OrderSummary> Items);

internal sealed record OrderSummary(int OrderId, string OrderNumber, string Status, decimal TotalAmount, string Currency, DateTime PlacedAtUtc, DateTime? RequiredAtUtc, bool HasOutstandingBalance);

internal sealed record CreateOrderRequest(string OrderNumber, decimal TotalAmount, string Currency, string? Status, DateTime? RequiredAtUtc);

internal static class SampleApiHelpers
{
    internal static UserSummary CreateUserSummary(int userId, string? alias, string displayName, string? email, bool isActive, string preferredLocale, DateTime createdAtUtc)
    {
        return new UserSummary(
            userId,
            NormalizeNullable(alias),
            displayName,
            NormalizeNullable(email),
            isActive,
            string.IsNullOrWhiteSpace(preferredLocale) ? "en-US" : preferredLocale,
            createdAtUtc);
    }

    internal static OrderSummary MapOrder(global::Xtraq.Samples.RestApi.Xtraq.Sample.UserDetailsWithOrdersResultSet2Result row)
    {
        return new OrderSummary(
            row.OrderId,
            row.OrderNumber,
            row.Status,
            row.TotalAmount,
            row.Currency,
            row.PlacedAtUtc,
            row.RequiredAtUtc,
            row.HasOutstandingBalance != 0);
    }

    internal static DbParameter AddParameter(DbCommand command, string name, object? value, DbType? dbType = null)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        if (dbType.HasValue)
        {
            parameter.DbType = dbType.Value;
        }
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static string? NormalizeNullable(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value;
}
