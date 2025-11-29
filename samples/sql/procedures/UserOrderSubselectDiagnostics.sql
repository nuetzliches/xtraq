USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.UserOrderSubselectDiagnostics
    @UserId shared.pkInt
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        o.OrderId,
        o.OrderNumber,
        PrimaryTagWithNoteSummary = (
            SELECT TOP (1)
                CONCAT(tags.TagName, N' • ', COALESCE(notes.NoteText, N''))
            FROM [test-foo].OrderTags AS tags
            LEFT JOIN test.OrderNotes AS notes
                ON notes.OrderId = tags.OrderId
               AND notes.IsPublic = 1
            WHERE tags.OrderId = o.OrderId
              AND tags.IsPrimary = 1
            ORDER BY tags.TagId DESC
        ),
        PrimaryTagNoteIsPublic = (
            SELECT TOP (1)
                notes.IsPublic
            FROM [test-foo].OrderTags AS tags
            LEFT JOIN test.OrderNotes AS notes
                ON notes.OrderId = tags.OrderId
            WHERE tags.OrderId = o.OrderId
              AND tags.IsPrimary = 1
            ORDER BY tags.TagId DESC
        ),
        LatestPublicNote = (
            SELECT TOP (1)
                notes.NoteText
            FROM test.OrderNotes AS notes
            WHERE notes.OrderId = o.OrderId
              AND notes.IsPublic = 1
            ORDER BY notes.NoteId DESC
        )
    FROM sample.Orders AS o
    WHERE o.UserId = @UserId
    ORDER BY o.OrderId;
END;
GO
