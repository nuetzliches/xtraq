namespace Xtraq.Configuration;

/// <summary>
/// Centralises which environment variables are allowed to flow in from project-scoped .env files.
/// </summary>
internal static class EnvironmentVariablePolicy
{
    private static readonly HashSet<string> AllowedEnvFileKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "XTRAQ_GENERATOR_DB",
        "XTRAQ_LOG_LEVEL",
        "XTRAQ_VERBOSE",
        "XTRAQ_ALIAS_DEBUG",
        "XTRAQ_FORWARDING_DIAG",
        "XTRAQ_DEFER_JSON_FUNCTION_EXPANSION",
        "XTRAQ_DUMP_FIRST_ROW",
        "XTRAQ_JSON_AST_DIAG",
        "XTRAQ_JSON_AUDIT",
        "XTRAQ_JSON_TYPE_LOG_LEVEL",
        "XTRAQ_LOG_JSON_TYPE_MAPPING"
    };

    /// <summary>
    /// Determines whether the supplied environment key is allowed to be sourced from .env files.
    /// </summary>
    /// <param name="key">Environment variable name.</param>
    internal static bool IsEnvFileAllowedKey(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        return AllowedEnvFileKeys.Contains(key.Trim());
    }
}
