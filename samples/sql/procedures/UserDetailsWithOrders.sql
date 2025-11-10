USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserDetailsWithOrders
    @UserId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        u.UserId,
        u.Alias,
        u.DisplayName,
        u.Email,
        u.IsActive,
        u.PreferredLocale,
        u.CreatedAtUtc,
        u.UpdatedAtUtc,
        u.Metadata
    FROM sample.Users AS u
    WHERE u.UserId = @UserId;

    SELECT
        o.OrderId,
        o.OrderNumber,
        o.Status,
        o.TotalAmount,
        o.Currency,
        o.PlacedAtUtc,
        o.RequiredAtUtc,
        o.HasOutstandingBalance
    FROM sample.Orders AS o
    WHERE o.UserId = @UserId
    ORDER BY o.PlacedAtUtc DESC;
END;
GO
