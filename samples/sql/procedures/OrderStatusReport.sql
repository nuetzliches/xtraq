USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.OrderStatusReport
    @FromUtc DATETIME2(3) = NULL,
    @ToUtc DATETIME2(3) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        s.Status,
        s.Orders,
        s.TotalAmount,
        s.AverageAmount,
        s.LastPlacedAtUtc
    FROM sample.fnOrderTotalsByStatus(@FromUtc, @ToUtc) AS s
    ORDER BY s.Status;
END;
GO
