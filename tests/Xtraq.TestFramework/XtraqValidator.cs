namespace Xtraq.TestFramework;

/// <summary>
/// Lightweight validator utilities for generated Xtraq artefacts.
/// </summary>
public static class XtraqValidator
{
    /// <summary>
    /// Validates that a configuration file exists before running generator validations.
    /// </summary>
    public static bool ValidateProjectConfiguration(string configPath, out string[] errors)
    {
        var errorList = new List<string>();

        if (!File.Exists(configPath))
        {
            errorList.Add($"Configuration file not found: {configPath}");
        }

        errors = errorList.ToArray();
        return errorList.Count == 0;
    }

    /// <summary>
    /// Performs a basic sanity check on generated C# code output.
    /// </summary>
    public static bool ValidateGeneratedCodeSyntax(string code, out string[] errors)
    {
        var errorList = new List<string>();

        if (string.IsNullOrWhiteSpace(code))
        {
            errorList.Add("Generated code is empty or null");
        }

        errors = errorList.ToArray();
        return errorList.Count == 0;
    }
}
