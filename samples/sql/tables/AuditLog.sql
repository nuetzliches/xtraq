USE XtraqSample;
GO

IF OBJECT_ID(N'sample.AuditLog', N'U') IS NULL
BEGIN
    CREATE TABLE sample.AuditLog
    (
        AuditLogId INT IDENTITY(1, 1) NOT NULL CONSTRAINT PK_AuditLog PRIMARY KEY,
        CreatedAtUtc DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        Source NVARCHAR(120) NOT NULL,
        Message NVARCHAR(4000) NOT NULL,
        Severity TINYINT NOT NULL,
        CorrelationId UNIQUEIDENTIFIER NULL,
        Details NVARCHAR(MAX) NULL
    );
END;
GO
