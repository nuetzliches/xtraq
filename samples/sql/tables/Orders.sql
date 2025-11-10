USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'sample.Orders', N'U') IS NULL
BEGIN
    CREATE TABLE sample.Orders
    (
        OrderId              INT                 IDENTITY(1, 1) NOT NULL CONSTRAINT PK_sample_Orders PRIMARY KEY,
        UserId               INT                 NOT NULL,
        OrderNumber          NVARCHAR(40)        NOT NULL,
        Status               NVARCHAR(50)        NOT NULL,
        TotalAmount          DECIMAL(18, 2)      NOT NULL,
        Currency             CHAR(3)             NOT NULL,
        PlacedAtUtc          DATETIME2(3)        NOT NULL CONSTRAINT DF_sample_Orders_PlacedAtUtc DEFAULT (SYSUTCDATETIME()),
        RequiredAtUtc        DATETIME2(3)        NULL,
        Metadata             sample.JsonDocument NULL,
        HasOutstandingBalance AS CASE WHEN TotalAmount > 0 AND Status IN (N'Pending', N'Processing') THEN 1 ELSE 0 END PERSISTED,
        CONSTRAINT FK_sample_Orders_Users FOREIGN KEY (UserId) REFERENCES sample.Users (UserId),
        CONSTRAINT UX_sample_Orders_OrderNumber UNIQUE (OrderNumber),
        CONSTRAINT CK_sample_Orders_Currency CHECK (Currency LIKE '[A-Z][A-Z][A-Z]')
    );
END;
GO

IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'sample.Orders', N'U')
          AND name = N'IX_sample_Orders_UserStatus'
    )
BEGIN
    CREATE INDEX IX_sample_Orders_UserStatus
        ON sample.Orders (UserId, Status)
        INCLUDE (PlacedAtUtc, TotalAmount);
END;
GO
