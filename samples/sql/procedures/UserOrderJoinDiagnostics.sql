USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserOrderJoinDiagnostics
    @UserId shared.pkInt
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        o.OrderId,
        o.OrderNumber,
        o.Status,
        PublicNote = notes.NoteText,
        NoteIsPublic = notes.IsPublic
    FROM sample.Orders AS o
    LEFT JOIN test.OrderNotes AS notes
        ON notes.OrderId = o.OrderId
       AND notes.IsPublic = 1
    WHERE o.UserId = @UserId
    ORDER BY o.OrderId;

    SELECT
        o.OrderId,
        o.OrderNumber,
        PrimaryTag = tags.TagName,
        tags.IsPrimary
    FROM sample.Orders AS o
    LEFT JOIN [test-foo].OrderTags AS tags
        ON tags.OrderId = o.OrderId
    WHERE o.UserId = @UserId
      AND tags.IsPrimary = 1
    ORDER BY o.OrderId;
END;
GO
