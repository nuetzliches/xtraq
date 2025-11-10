USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UpdateUserBio
    @UserId INT,
    @Bio sample.UserBioType
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE sample.Users
    SET
        Bio = @Bio,
        UpdatedAtUtc = SYSUTCDATETIME()
    WHERE UserId = @UserId;

    SELECT
        u.UserId,
        u.DisplayName,
        u.Bio,
        u.UpdatedAtUtc
    FROM sample.Users AS u
    WHERE u.UserId = @UserId;
END;
GO
