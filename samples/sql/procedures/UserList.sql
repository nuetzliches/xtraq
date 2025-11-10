USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserList
    @IncludeInactive BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        u.UserId,
        u.Alias,
        u.DisplayName,
        u.Email,
        u.IsActive,
        u.PreferredLocale,
        u.CreatedAtUtc,
        u.UpdatedAtUtc,
        u.SearchName
    FROM sample.Users AS u
    WHERE @IncludeInactive = 1 OR u.IsActive = 1
    ORDER BY u.DisplayName;
END;
GO
