USE XtraqSample;
GO

CREATE OR ALTER PROCEDURE sample.WriteAuditLogEntries
    @Entries shared.AuditLogEntryTableType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO sample.AuditLog
    (
        Source,
        Message,
        Severity,
        CorrelationId,
        Details
    )
    SELECT
        e.Source,
        e.Message,
        e.Severity,
        e.CorrelationId,
        e.Details
    FROM @Entries AS e;
END;
GO
