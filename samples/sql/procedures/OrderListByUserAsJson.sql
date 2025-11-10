USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.OrderListByUserAsJson
    @UserId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        o.OrderId,
        o.OrderNumber,
        o.Status,
        o.TotalAmount,
        o.Currency,
        o.PlacedAtUtc,
        o.RequiredAtUtc,
        Payments = (
            SELECT
                p.PaymentId,
                p.Gateway,
                p.Amount,
                p.CapturedAtUtc,
                p.FailureReason
            FROM sample.Payments AS p
            WHERE p.OrderId = o.OrderId
            FOR JSON PATH
        )
    FROM sample.Orders AS o
    WHERE o.UserId = @UserId
    ORDER BY o.PlacedAtUtc DESC
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;
END;
GO
