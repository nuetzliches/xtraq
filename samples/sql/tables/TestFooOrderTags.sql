USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'[test-foo].OrderTags', N'U') IS NULL
BEGIN
    EXEC(N'
    CREATE TABLE [test-foo].OrderTags
    (
        TagId        INT            IDENTITY(1, 1) NOT NULL CONSTRAINT PK_testfoo_OrderTags PRIMARY KEY,
        OrderId      INT            NOT NULL,
        TagName      NVARCHAR(80)   NOT NULL,
        IsPrimary    BIT            NOT NULL CONSTRAINT DF_testfoo_OrderTags_IsPrimary DEFAULT (0),
        CreatedAtUtc DATETIME2(3)   NOT NULL CONSTRAINT DF_testfoo_OrderTags_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_testfoo_OrderTags_Orders FOREIGN KEY (OrderId) REFERENCES sample.Orders (OrderId)
    );');
END;
GO
