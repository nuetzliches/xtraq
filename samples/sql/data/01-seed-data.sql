USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

DECLARE @UserSeed TABLE
(
    Alias           NVARCHAR(120)       NOT NULL,
    DisplayName     NVARCHAR(200)       NOT NULL,
    Email           NVARCHAR(320)       NULL,
    IsActive        BIT                 NOT NULL,
    PreferredLocale NVARCHAR(10)        NOT NULL,
    Metadata        sample.JsonDocument NULL
);

INSERT INTO @UserSeed (Alias, DisplayName, Email, IsActive, PreferredLocale, Metadata)
VALUES
    (N'alice', N'Alice Example', N'alice@example.com', 1, N'en-US', N'{"timezone":"UTC","roles":["admin"]}'),
    (N'bob', N'Bob Builder', N'bob.builder@example.com', 1, N'en-GB', N'{"timezone":"Europe/London","roles":["reviewer"]}'),
    (N'carla', N'Carla Curator', NULL, 0, N'de-DE', N'{"timezone":"Europe/Berlin","roles":["archived"]}');

MERGE sample.Users AS target
USING @UserSeed AS source
    ON target.Alias = source.Alias
WHEN MATCHED THEN
    UPDATE SET
        DisplayName = source.DisplayName,
        Email = source.Email,
        IsActive = source.IsActive,
        PreferredLocale = source.PreferredLocale,
        Metadata = source.Metadata,
        UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (Alias, DisplayName, Email, IsActive, PreferredLocale, Metadata)
    VALUES (source.Alias, source.DisplayName, source.Email, source.IsActive, source.PreferredLocale, source.Metadata);

DECLARE @OrderSeed TABLE
(
    OrderNumber NVARCHAR(40)        NOT NULL,
    UserAlias   NVARCHAR(120)       NOT NULL,
    Status      NVARCHAR(50)        NOT NULL,
    TotalAmount DECIMAL(18, 2)      NOT NULL,
    Currency    CHAR(3)             NOT NULL,
    PlacedAtUtc DATETIME2(3)        NOT NULL,
    RequiredAtUtc DATETIME2(3)      NULL,
    Metadata    sample.JsonDocument NULL
);

INSERT INTO @OrderSeed (OrderNumber, UserAlias, Status, TotalAmount, Currency, PlacedAtUtc, RequiredAtUtc, Metadata)
VALUES
    (N'ORD-1000', N'alice', N'Completed', 125.30, N'USD', '2024-06-01T08:30:00', NULL, N'{"channel":"web","priority":"standard"}'),
    (N'ORD-1001', N'alice', N'Processing', 79.95, N'USD', '2024-06-03T10:15:00', '2024-06-10T10:15:00', N'{"channel":"api","priority":"rush"}'),
    (N'ORD-2000', N'bob', N'Pending', 199.00, N'EUR', '2024-07-12T12:45:00', NULL, N'{"channel":"mobile"}');

MERGE sample.Orders AS target
USING (
    SELECT
        s.OrderNumber,
        u.UserId,
        s.Status,
        s.TotalAmount,
        s.Currency,
        s.PlacedAtUtc,
        s.RequiredAtUtc,
        s.Metadata
    FROM @OrderSeed AS s
    INNER JOIN sample.Users AS u
        ON u.Alias = s.UserAlias
) AS source (OrderNumber, UserId, Status, TotalAmount, Currency, PlacedAtUtc, RequiredAtUtc, Metadata)
    ON target.OrderNumber = source.OrderNumber
WHEN MATCHED THEN
    UPDATE SET
        UserId = source.UserId,
        Status = source.Status,
        TotalAmount = source.TotalAmount,
        Currency = source.Currency,
        PlacedAtUtc = source.PlacedAtUtc,
        RequiredAtUtc = source.RequiredAtUtc,
        Metadata = source.Metadata
WHEN NOT MATCHED THEN
    INSERT (UserId, OrderNumber, Status, TotalAmount, Currency, PlacedAtUtc, RequiredAtUtc, Metadata)
    VALUES (source.UserId, source.OrderNumber, source.Status, source.TotalAmount, source.Currency, source.PlacedAtUtc, source.RequiredAtUtc, source.Metadata);

DECLARE @PaymentSeed TABLE
(
    OrderNumber NVARCHAR(40)        NOT NULL,
    Gateway     NVARCHAR(50)        NOT NULL,
    Amount      DECIMAL(18, 4)      NOT NULL,
    CapturedAtUtc DATETIME2(3)      NULL,
    FailureReason NVARCHAR(400)     NULL,
    Properties  sample.JsonDocument NULL
);

INSERT INTO @PaymentSeed (OrderNumber, Gateway, Amount, CapturedAtUtc, FailureReason, Properties)
VALUES
    (N'ORD-1000', N'Stripe', 125.30, '2024-06-01T08:35:00', NULL, N'{"reference":"ch_1000","status":"captured"}'),
    (N'ORD-1001', N'Stripe', 79.95, NULL, N'authorization_pending', N'{"reference":"pi_1001","status":"requires_capture"}'),
    (N'ORD-2000', N'Adyen', 199.00, NULL, NULL, N'{"reference":"pay_2000","attempts":1}');

MERGE sample.Payments AS target
USING (
    SELECT
        o.OrderId,
        s.Gateway,
        s.Amount,
        s.CapturedAtUtc,
        s.FailureReason,
        s.Properties
    FROM @PaymentSeed AS s
    INNER JOIN sample.Orders AS o
        ON o.OrderNumber = s.OrderNumber
) AS source (OrderId, Gateway, Amount, CapturedAtUtc, FailureReason, Properties)
    ON target.OrderId = source.OrderId
   AND target.Gateway = source.Gateway
WHEN MATCHED THEN
    UPDATE SET
        Amount = source.Amount,
        CapturedAtUtc = source.CapturedAtUtc,
        FailureReason = source.FailureReason,
        Properties = source.Properties
WHEN NOT MATCHED THEN
    INSERT (OrderId, Gateway, Amount, CapturedAtUtc, FailureReason, Properties)
    VALUES (source.OrderId, source.Gateway, source.Amount, source.CapturedAtUtc, source.FailureReason, source.Properties);

DECLARE @Contacts sample.UserContactTableType;

INSERT INTO @Contacts (UserId, Email, DisplayName, Source, Preferred, LastInteractionUtc)
SELECT u.UserId, c.Email, c.DisplayName, c.Source, c.Preferred, c.LastInteractionUtc
FROM sample.Users AS u
INNER JOIN (
    VALUES
        (N'alice', N'alice@example.com', N'Alice Example', N'appsync', CAST(1 AS BIT), '2024-08-02T09:00:00'),
        (N'alice', N'a.ops@example.com', N'Alice Ops', N'import', CAST(0 AS BIT), NULL),
        (N'bob', N'bob.builder@example.com', N'Bob Builder', N'appsync', CAST(1 AS BIT), '2024-08-03T14:15:00')
) AS c(Alias, Email, DisplayName, Source, Preferred, LastInteractionUtc)
    ON u.Alias = c.Alias;

MERGE sample.UserContacts AS target
USING @Contacts AS source
    ON target.UserId = source.UserId
   AND target.Email = source.Email
WHEN MATCHED THEN
    UPDATE SET
        DisplayName = source.DisplayName,
        Source = COALESCE(source.Source, target.Source),
        Preferred = COALESCE(source.Preferred, target.Preferred),
        LastInteractionUtc = source.LastInteractionUtc,
        UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (UserId, Email, DisplayName, Source, Preferred, LastInteractionUtc)
    VALUES (source.UserId, source.Email, source.DisplayName, COALESCE(source.Source, N'import'), COALESCE(source.Preferred, 0), source.LastInteractionUtc);
GO
