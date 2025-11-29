USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'test.OrderNotes', N'U') IS NULL
BEGIN
    CREATE TABLE test.OrderNotes
    (
        NoteId       INT            IDENTITY(1, 1) NOT NULL CONSTRAINT PK_test_OrderNotes PRIMARY KEY,
        OrderId      INT            NOT NULL,
        NoteText     NVARCHAR(400)  NOT NULL,
        IsPublic     BIT            NOT NULL CONSTRAINT DF_test_OrderNotes_IsPublic DEFAULT (0),
        CreatedAtUtc DATETIME2(3)   NOT NULL CONSTRAINT DF_test_OrderNotes_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_test_OrderNotes_Orders FOREIGN KEY (OrderId) REFERENCES sample.Orders (OrderId)
    );
END;
GO
