USE XtraqSample;
GO

CREATE OR ALTER FUNCTION sample.fnUserPreferredContact
(
    @UserId INT
)
RETURNS TABLE
AS
RETURN
(
    SELECT TOP (1)
        uc.ContactId,
        uc.Email,
        uc.DisplayName,
        uc.Source,
        uc.Preferred,
        uc.LastInteractionUtc
    FROM sample.UserContacts AS uc
    WHERE uc.UserId = @UserId
    ORDER BY uc.Preferred DESC, uc.LastInteractionUtc DESC, uc.ContactId DESC
);
GO
