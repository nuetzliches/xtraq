namespace Xtraq.TestFramework;

/// <summary>
/// Base class for Xtraq test scenarios providing shared helpers.
/// </summary>
public abstract class XtraqTestBase
{
    /// <summary>
    /// Resolves the test connection string from the environment or falls back to a localdb instance.
    /// </summary>
    protected virtual string GetTestConnectionString()
    {
        return Environment.GetEnvironmentVariable("XTRAQ_TEST_CONNECTION_STRING")
               ?? "Server=(localdb)\\MSSQLLocalDB;Database=XtraqTest;Trusted_Connection=True;";
    }

    /// <summary>
    /// Placeholder for Roslyn-based compilation validation of generated code.
    /// </summary>
    protected static void ValidateGeneratedCodeCompiles(string generatedCode, out bool success, out string[] errors)
    {
        success = true;
        errors = Array.Empty<string>();
    }
}
