USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserPreferredContactApply
    @UserId shared.pkInt,
    @OnlyPreferred BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        u.UserId,
        u.DisplayName,
        u.Email,
        u.IsActive,
        PreferredEmail = pc.Email,
        PreferredDisplayName = pc.DisplayName,
        PreferredSource = pc.Source,
        PreferredLastInteractionUtc = pc.LastInteractionUtc,
        PreferredRank = CASE WHEN pc.Preferred = 1 THEN N'Primary' ELSE N'Secondary' END
    FROM sample.Users AS u
    OUTER APPLY sample.fnUserPreferredContact(u.UserId) AS pc
    WHERE u.UserId = @UserId
      AND (@OnlyPreferred = 0 OR pc.Preferred = 1);

    SELECT
        u.UserId,
        PreferredContactJson = (
            SELECT TOP (1)
                uc.Email,
                uc.DisplayName,
                uc.Source,
                uc.Preferred,
                uc.LastInteractionUtc
            FROM sample.UserContacts AS uc
            WHERE uc.UserId = u.UserId
            ORDER BY uc.Preferred DESC, uc.LastInteractionUtc DESC, uc.ContactId DESC
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM sample.Users AS u
    WHERE u.UserId = @UserId;
END;
GO
