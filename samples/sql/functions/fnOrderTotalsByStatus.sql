USE XtraqSample;
GO

CREATE OR ALTER FUNCTION sample.fnOrderTotalsByStatus
(
    @FromUtc DATETIME2(3) = NULL,
    @ToUtc DATETIME2(3) = NULL
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        o.Status,
        Orders = COUNT_BIG(*) ,
        TotalAmount = SUM(CAST(o.TotalAmount AS DECIMAL(18, 4))),
        AverageAmount = AVG(CAST(o.TotalAmount AS DECIMAL(18, 4))),
        LastPlacedAtUtc = MAX(o.PlacedAtUtc)
    FROM sample.Orders AS o
    WHERE (@FromUtc IS NULL OR o.PlacedAtUtc >= @FromUtc)
      AND (@ToUtc IS NULL OR o.PlacedAtUtc < @ToUtc)
    GROUP BY o.Status
);
GO
