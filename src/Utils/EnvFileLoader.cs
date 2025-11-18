namespace Xtraq.Utils;

/// <summary>
/// Provides helpers for applying .env key/value pairs to the current process environment.
/// </summary>
internal static class EnvFileLoader
{
    /// <summary>
    /// Loads environment variables from a .env file and applies them to the current process.
    /// </summary>
    /// <param name="envPath">Absolute or relative path to the .env file.</param>
    /// <param name="overwrite">Determines whether existing environment variables should be overwritten.</param>
    internal static void Apply(string envPath, bool overwrite = false)
    {
        if (string.IsNullOrWhiteSpace(envPath) || !File.Exists(envPath))
        {
            return;
        }

        var variables = Xtraq.Configuration.TrackableConfigManager.BuildEnvMap(File.ReadAllLines(envPath));
        Apply(variables, overwrite);
    }

    /// <summary>
    /// Applies the provided environment variable dictionary to the current process.
    /// </summary>
    /// <param name="variables">Dictionary containing environment variable names and values.</param>
    /// <param name="overwrite">Determines whether existing environment variables should be overwritten.</param>
    internal static void Apply(IReadOnlyDictionary<string, string?>? variables, bool overwrite = false)
    {
        if (variables == null)
        {
            return;
        }

        foreach (var pair in variables)
        {
            if (string.IsNullOrWhiteSpace(pair.Key))
            {
                continue;
            }

            if (!overwrite && !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(pair.Key)))
            {
                continue;
            }

            Environment.SetEnvironmentVariable(pair.Key, pair.Value);
        }
    }
}
