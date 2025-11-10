USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'sample.UserContacts', N'U') IS NULL
BEGIN
    CREATE TABLE sample.UserContacts
    (
        ContactId        INT           IDENTITY(1, 1) NOT NULL CONSTRAINT PK_sample_UserContacts PRIMARY KEY,
        UserId           INT           NOT NULL,
        Email            NVARCHAR(320) NOT NULL,
        DisplayName      NVARCHAR(200) NOT NULL,
        Source           NVARCHAR(50)  NOT NULL CONSTRAINT DF_sample_UserContacts_Source DEFAULT (N'import'),
        Preferred        BIT           NOT NULL CONSTRAINT DF_sample_UserContacts_Preferred DEFAULT (0),
        LastInteractionUtc DATETIME2(3) NULL,
        CreatedAtUtc     DATETIME2(3)  NOT NULL CONSTRAINT DF_sample_UserContacts_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        UpdatedAtUtc     DATETIME2(3)  NULL,
        CONSTRAINT FK_sample_UserContacts_Users FOREIGN KEY (UserId) REFERENCES sample.Users (UserId),
        CONSTRAINT UX_sample_UserContacts_UserEmail UNIQUE (UserId, Email)
    );
END;
GO

IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'sample.UserContacts', N'U')
          AND name = N'IX_sample_UserContacts_Email'
    )
BEGIN
    CREATE INDEX IX_sample_UserContacts_Email
        ON sample.UserContacts (Email)
        INCLUDE (UserId, Preferred);
END;
GO
