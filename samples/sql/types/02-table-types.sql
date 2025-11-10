USE XtraqSample;
GO

IF TYPE_ID(N'sample.UserContactTableType') IS NULL
BEGIN
    EXEC sys.sp_executesql N'
        CREATE TYPE sample.UserContactTableType AS TABLE
        (
            UserId INT NOT NULL,
            Email NVARCHAR(320) NOT NULL,
            DisplayName NVARCHAR(200) NOT NULL,
            Source NVARCHAR(50) NULL,
            Preferred BIT NULL,
            LastInteractionUtc DATETIME2(3) NULL
        );
    ';
END;
GO

IF TYPE_ID(N'sample.OrderImportTableType') IS NULL
BEGIN
    EXEC sys.sp_executesql N'
        CREATE TYPE sample.OrderImportTableType AS TABLE
        (
            UserAlias NVARCHAR(120) NOT NULL,
            OrderNumber NVARCHAR(40) NOT NULL,
            TotalAmount DECIMAL(18, 2) NOT NULL,
            Currency CHAR(3) NOT NULL,
            PlacedAtUtc DATETIME2(3) NOT NULL,
            Metadata sample.JsonDocument NULL
        );
    ';
END;
GO
