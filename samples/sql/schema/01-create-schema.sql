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

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'test')
BEGIN
    EXEC(N'CREATE SCHEMA test');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'test-foo')
BEGIN
    EXEC(N'CREATE SCHEMA [test-foo]');
END;
GO
