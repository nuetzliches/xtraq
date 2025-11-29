USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserOrderInsights
    @UserId shared.pkInt
AS
BEGIN
    SET NOCOUNT ON;

    WITH PendingOrders AS (
        SELECT o.UserId, COUNT_BIG(*) AS PendingCount
        FROM sample.Orders AS o
        WHERE o.Status IN (N'Pending', N'Processing')
        GROUP BY o.UserId
    ),
    PaymentSummary AS (
        SELECT o.UserId,
               SUM(p.Amount) AS CapturedAmount,
               COUNT_BIG(p.PaymentId) AS CapturedCount
        FROM sample.Orders AS o
        LEFT JOIN sample.Payments AS p
            ON p.OrderId = o.OrderId
           AND p.CapturedAtUtc IS NOT NULL
        GROUP BY o.UserId
    )
    SELECT
        u.UserId,
        u.DisplayName,
        LatestOrderNumber = latest.OrderNumber,
        LatestOrderTotalAmount = latest.TotalAmount,
        LatestOrderCurrency = latest.Currency,
        LatestOrderRequiredAtUtc = latest.RequiredAtUtc,
        LatestOrderMetadata = latest.Metadata,
        PendingOrderCount = pending.PendingCount,
        CapturedPaymentAmount = payment.CapturedAmount,
        CapturedPaymentCount = payment.CapturedCount,
        NextContactReminderUtc = (
            SELECT MIN(c.LastInteractionUtc)
            FROM sample.UserContacts AS c
            WHERE c.UserId = u.UserId
              AND c.LastInteractionUtc IS NOT NULL
        )
    FROM sample.Users AS u
    OUTER APPLY (
        SELECT TOP (1)
            o.OrderNumber,
            o.TotalAmount,
            o.Currency,
            o.RequiredAtUtc,
            o.Metadata
        FROM sample.Orders AS o
        WHERE o.UserId = u.UserId
        ORDER BY o.PlacedAtUtc DESC
    ) AS latest
    LEFT JOIN PendingOrders AS pending
        ON pending.UserId = u.UserId
    LEFT JOIN PaymentSummary AS payment
        ON payment.UserId = u.UserId
    WHERE u.UserId = @UserId;
END;
GO
