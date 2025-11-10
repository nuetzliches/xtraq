USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.OrderListAsJson
    @UserId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Result NVARCHAR(MAX);

    SELECT @Result = (
        SELECT
            o.OrderId,
            o.OrderNumber,
            o.Status,
            o.TotalAmount,
            o.Currency,
            o.PlacedAtUtc,
            o.RequiredAtUtc,
            Payments = COALESCE(
                (
                    SELECT
                        p.PaymentId,
                        p.Gateway,
                        p.Amount,
                        p.CapturedAtUtc,
                        p.FailureReason
                    FROM sample.Payments AS p
                    WHERE p.OrderId = o.OrderId
                    ORDER BY p.CapturedAtUtc DESC
                    FOR JSON PATH
                ),
                N'[]'
            )
        FROM sample.Orders AS o
        WHERE @UserId IS NULL OR o.UserId = @UserId
        ORDER BY o.PlacedAtUtc DESC
        FOR JSON PATH, INCLUDE_NULL_VALUES
    );

    SELECT OrdersJson = @Result;
END;
GO
