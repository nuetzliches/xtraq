namespace Xtraq.Utils;

/// <summary>
/// Provides helpers for interpreting environment variable values that represent boolean flags.
/// </summary>
internal static class EnvironmentHelper
{
    private static readonly string[] TrueValues = ["1", "true", "yes", "on"];

    /// <summary>
    /// Determines whether the provided value represents an enabled flag ("1", "true", "yes", or "on").
    /// </summary>
    /// <param name="value">Raw value to evaluate.</param>
    /// <returns><c>true</c> if the value signals an enabled flag; otherwise <c>false</c>.</returns>
    internal static bool EqualsTrue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        foreach (var candidate in TrueValues)
        {
            if (string.Equals(value, candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Reads an environment variable and evaluates whether it represents an enabled flag value.
    /// </summary>
    /// <param name="variableName">Name of the environment variable to check.</param>
    /// <returns><c>true</c> if the variable is present and marks the flag as enabled.</returns>
    internal static bool IsTrue(string variableName)
    {
        if (string.IsNullOrWhiteSpace(variableName))
        {
            return false;
        }

        return EqualsTrue(Environment.GetEnvironmentVariable(variableName));
    }
}
