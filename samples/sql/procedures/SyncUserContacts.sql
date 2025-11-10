USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.SyncUserContacts
    @Contacts sample.UserContactTableType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE sample.UserContacts AS target
    USING @Contacts AS source
        ON target.UserId = source.UserId
       AND target.Email = source.Email
    WHEN MATCHED THEN
        UPDATE SET
            target.DisplayName = source.DisplayName,
            target.Source = COALESCE(source.Source, target.Source),
            target.Preferred = COALESCE(source.Preferred, target.Preferred),
            target.LastInteractionUtc = COALESCE(source.LastInteractionUtc, target.LastInteractionUtc),
            target.UpdatedAtUtc = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (UserId, Email, DisplayName, Source, Preferred, LastInteractionUtc)
        VALUES
        (
            source.UserId,
            source.Email,
            source.DisplayName,
            COALESCE(source.Source, N'import'),
            COALESCE(source.Preferred, 0),
            source.LastInteractionUtc
        )
    OUTPUT
        $action AS MergeAction,
        inserted.ContactId,
        inserted.UserId,
        inserted.Email,
        inserted.DisplayName,
        inserted.Preferred,
        inserted.LastInteractionUtc,
        inserted.UpdatedAtUtc;
END;
GO
