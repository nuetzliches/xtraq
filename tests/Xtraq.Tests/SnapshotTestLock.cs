namespace Xtraq.Tests;

/// <summary>
/// Shared lock used by tests that modify global snapshot environment variables.
/// Prevents parallel test execution from corrupting shared state.
/// </summary>
internal static class SnapshotTestLock
{
    public static readonly object Gate = new();
}
