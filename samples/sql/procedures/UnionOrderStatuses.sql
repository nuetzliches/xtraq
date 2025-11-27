USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UnionOrderStatuses
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        COUNT(*) AS Quantity,
        N'Pending' AS Day
    FROM sample.Orders AS o
    WHERE o.Status = N'Pending'
    UNION
    SELECT
        COUNT(*) AS Quantity,
        N'Completed' AS Day
    FROM sample.Orders AS o
    WHERE o.Status = N'Completed'
    UNION
    SELECT
        COUNT(*) AS Quantity,
        N'Other' AS Day
    FROM sample.Orders AS o
    WHERE o.Status NOT IN (N'Pending', N'Completed')
    FOR JSON PATH;
END;
GO
