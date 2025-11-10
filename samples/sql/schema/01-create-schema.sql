USE XtraqSample;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sample')
BEGIN
    EXEC(N'CREATE SCHEMA sample');
END;
GO
