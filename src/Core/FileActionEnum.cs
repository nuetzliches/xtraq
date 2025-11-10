namespace Xtraq.Core;

/// <summary>
/// Identifies the status of a file during generation and update operations.
/// </summary>
internal enum FileActionEnum
{
    Undefined,
    Created,
    Modified,
    UpToDate
}
