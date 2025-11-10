USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserCompositeJsonSnapshot
    @UserId INT,
    @RecentPaymentCount INT = 3
AS
BEGIN
    SET NOCOUNT ON;

    IF @RecentPaymentCount IS NULL OR @RecentPaymentCount < 1
    BEGIN
        SET @RecentPaymentCount = 1;
    END;

    DECLARE @UserProfileJson NVARCHAR(MAX);

    SET @UserProfileJson = (
        SELECT
            u.UserId,
            u.DisplayName,
            u.Email,
            u.IsActive,
            u.Bio,
            u.PreferredLocale,
            u.Metadata,
            (
                SELECT TOP (5)
                    o.OrderId,
                    o.OrderNumber,
                    o.Status,
                    o.TotalAmount,
                    o.Currency,
                    o.PlacedAtUtc,
                    JSON_QUERY(o.Metadata) AS MetadataJson
                FROM sample.Orders AS o
                WHERE o.UserId = u.UserId
                ORDER BY o.PlacedAtUtc DESC
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS Orders
        FROM sample.Users AS u
        WHERE u.UserId = @UserId
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
    );

    SELECT @UserProfileJson AS ProfileJson;

    SELECT
        c.ContactId,
        c.Email,
        c.DisplayName,
        c.Preferred,
        c.LastInteractionUtc,
        JSON_QUERY((
            SELECT TOP (@RecentPaymentCount)
                p.PaymentId,
                p.Gateway,
                p.Amount,
                p.CapturedAtUtc,
                JSON_QUERY(p.Properties) AS PropertiesJson
            FROM sample.Orders AS o
            INNER JOIN sample.Payments AS p ON p.OrderId = o.OrderId
            WHERE o.UserId = @UserId
            ORDER BY p.CapturedAtUtc DESC
            FOR JSON PATH, INCLUDE_NULL_VALUES
        )) AS RecentPaymentsJson
    FROM sample.UserContacts AS c
    WHERE c.UserId = @UserId
    ORDER BY c.Preferred DESC, c.ContactId;

    SELECT
        o.OrderId,
        o.OrderNumber,
        JSON_QUERY(o.Metadata) AS MetadataJson
    FROM sample.Orders AS o
    WHERE o.UserId = @UserId
    FOR JSON PATH;
END;
GO
