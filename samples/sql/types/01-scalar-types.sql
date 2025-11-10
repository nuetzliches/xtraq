USE XtraqSample;
GO

IF TYPE_ID(N'sample.UserBioType') IS NULL
BEGIN
    EXEC(N'CREATE TYPE sample.UserBioType FROM NVARCHAR(4000) NULL');
END;
GO

IF TYPE_ID(N'sample.JsonDocument') IS NULL
BEGIN
    EXEC(N'CREATE TYPE sample.JsonDocument FROM NVARCHAR(MAX) NULL');
END;
GO
