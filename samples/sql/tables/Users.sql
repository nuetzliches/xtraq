USE XtraqSample;
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'sample.Users', N'U') IS NULL
BEGIN
    CREATE TABLE sample.Users
    (
        UserId            INT                 IDENTITY(1, 1) NOT NULL CONSTRAINT PK_sample_Users PRIMARY KEY,
        Alias             NVARCHAR(120)       NULL,
        DisplayName       NVARCHAR(200)       NOT NULL,
        Email             NVARCHAR(320)       NULL,
        IsActive          BIT                 NOT NULL CONSTRAINT DF_sample_Users_IsActive DEFAULT (1),
        Bio               sample.UserBioType  NULL,
        PreferredLocale   NVARCHAR(10)        NOT NULL CONSTRAINT DF_sample_Users_PreferredLocale DEFAULT (N'en-US'),
        CreatedAtUtc      DATETIME2(3)        NOT NULL CONSTRAINT DF_sample_Users_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        UpdatedAtUtc      DATETIME2(3)        NULL,
        Metadata          sample.JsonDocument NULL,
        SearchName        AS CONCAT(DisplayName, N' ', ISNULL(Alias, N'')) PERSISTED,
        ProfileVersion    ROWVERSION,
        CONSTRAINT UX_sample_Users_Alias UNIQUE (Alias)
    );
END;
GO

IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'sample.Users', N'U')
          AND name = N'UX_sample_Users_Email'
    )
BEGIN
    CREATE UNIQUE INDEX UX_sample_Users_Email
        ON sample.Users (Email)
        WHERE Email IS NOT NULL;
END;
GO
