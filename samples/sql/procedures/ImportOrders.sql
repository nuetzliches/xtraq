USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.ImportOrders
    @Orders sample.OrderImportTableType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Resolved TABLE
    (
        UserId INT NOT NULL,
        OrderNumber NVARCHAR(40) NOT NULL,
        TotalAmount DECIMAL(18, 2) NOT NULL,
        Currency CHAR(3) NOT NULL,
        PlacedAtUtc DATETIME2(3) NOT NULL,
        Metadata sample.JsonDocument NULL
    );

    INSERT INTO @Resolved (UserId, OrderNumber, TotalAmount, Currency, PlacedAtUtc, Metadata)
    SELECT
        u.UserId,
        o.OrderNumber,
        o.TotalAmount,
        o.Currency,
        o.PlacedAtUtc,
        o.Metadata
    FROM @Orders AS o
    INNER JOIN sample.Users AS u
        ON u.Alias = o.UserAlias;

    MERGE sample.Orders AS target
    USING @Resolved AS source
        ON target.OrderNumber = source.OrderNumber
    WHEN MATCHED THEN
        UPDATE SET
            UserId = source.UserId,
            TotalAmount = source.TotalAmount,
            Currency = source.Currency,
            PlacedAtUtc = source.PlacedAtUtc,
            Metadata = source.Metadata
    WHEN NOT MATCHED THEN
        INSERT (UserId, OrderNumber, TotalAmount, Currency, PlacedAtUtc, Metadata)
        VALUES (source.UserId, source.OrderNumber, source.TotalAmount, source.Currency, source.PlacedAtUtc, source.Metadata);

    SELECT
        ImportedOrders = COUNT(*)
    FROM @Resolved;
END;
GO
