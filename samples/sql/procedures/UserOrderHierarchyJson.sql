USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserOrderHierarchyJson
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        u.UserId,
        u.Alias,
        u.DisplayName,
        u.Email,
        Orders = (
            SELECT
                o.OrderId,
                o.OrderNumber,
                o.Status,
                o.TotalAmount,
                o.Currency,
                o.PlacedAtUtc,
                Payments = (
                    SELECT
                        p.PaymentId,
                        p.Gateway,
                        p.Amount,
                        p.CapturedAtUtc
                    FROM sample.Payments AS p
                    WHERE p.OrderId = o.OrderId
                    ORDER BY p.CapturedAtUtc DESC
                    FOR JSON PATH
                )
            FROM sample.Orders AS o
            WHERE o.UserId = u.UserId
            ORDER BY o.PlacedAtUtc DESC
            FOR JSON PATH
        )
    FROM sample.Users AS u
    ORDER BY u.UserId
    FOR JSON PATH, ROOT('Users');
END;
GO
