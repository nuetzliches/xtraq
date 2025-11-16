USE XtraqSample;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sample')
BEGIN
    EXEC(N'CREATE SCHEMA sample');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'shared')
BEGIN
    EXEC(N'CREATE SCHEMA shared');
END;
GO
