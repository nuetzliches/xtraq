USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'sample.Payments', N'U') IS NULL
BEGIN
    CREATE TABLE sample.Payments
    (
        PaymentId        UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_sample_Payments PRIMARY KEY DEFAULT (NEWSEQUENTIALID()),
        OrderId          INT              NOT NULL,
        Gateway          NVARCHAR(50)     NOT NULL,
        Amount           DECIMAL(18, 4)   NOT NULL,
        CapturedAtUtc    DATETIME2(3)     NULL,
        FailureReason    NVARCHAR(400)    NULL,
        Properties       sample.JsonDocument NULL,
        MetadataChecksum VARBINARY(64)    NULL,
        CONSTRAINT FK_sample_Payments_Orders FOREIGN KEY (OrderId) REFERENCES sample.Orders (OrderId)
    );
END;
GO

IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'sample.Payments', N'U')
          AND name = N'IX_sample_Payments_OrderId'
    )
BEGIN
    CREATE INDEX IX_sample_Payments_OrderId
        ON sample.Payments (OrderId)
        INCLUDE (Gateway, CapturedAtUtc);
END;
GO
