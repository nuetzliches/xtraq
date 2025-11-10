namespace Xtraq.Utils;

internal enum LogLevelThreshold
{
    Quiet = 0,
    Info = 1,
    Debug = 2,
    Trace = 3
}

internal static class LogLevelConfiguration
{
    private const string VariableName = "XTRAQ_LOG_LEVEL";

    internal static LogLevelThreshold GetLevel()
    {
        return Parse(Environment.GetEnvironmentVariable(VariableName));
    }

    internal static bool IsAtLeast(LogLevelThreshold level)
    {
        return GetLevel() >= level;
    }

    internal static void PromoteTo(LogLevelThreshold level)
    {
        var current = GetLevel();
        if (current >= level)
        {
            return;
        }

        var value = level switch
        {
            LogLevelThreshold.Info => "info",
            LogLevelThreshold.Debug => "debug",
            LogLevelThreshold.Trace => "trace",
            _ => null
        };

        Environment.SetEnvironmentVariable(VariableName, value);
    }

    private static LogLevelThreshold Parse(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return LogLevelThreshold.Quiet;
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "trace" => LogLevelThreshold.Trace,
            "debug" => LogLevelThreshold.Debug,
            "info" => LogLevelThreshold.Info,
            _ => LogLevelThreshold.Quiet
        };
    }
}
