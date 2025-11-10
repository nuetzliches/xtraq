USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserFind
    @UserId INT
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
        PreferredContactEmail = pc.Email,
        PreferredContactDisplayName = pc.DisplayName
    FROM sample.Users AS u
    OUTER APPLY sample.fnUserPreferredContact(u.UserId) AS pc
    WHERE u.UserId = @UserId;
END;
GO
