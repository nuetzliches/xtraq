
namespace Xtraq.Utils;

/// <summary>
/// Helper for conditional debug output that respects CLI verbosity settings.
/// Replaces direct Console.WriteLine calls for debug information.
/// </summary>
public static class DebugOutputHelper
{
    private static bool _debugMode = LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug);
    private static bool _verboseMode = EnvironmentHelper.IsTrue("XTRAQ_VERBOSE") || LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Info);

    /// <summary>
    /// Write debug message only if debug mode is explicitly enabled.
    /// </summary>
    public static void WriteDebug(string message)
    {
        if (_debugMode)
        {
            Console.WriteLine(message);
        }
    }

    /// <summary>
    /// Write verbose debug message only if verbose or debug mode is enabled.
    /// </summary>
    public static void WriteVerboseDebug(string message)
    {
        if (_debugMode || _verboseMode)
        {
            Console.WriteLine(message);
        }
    }

    /// <summary>
    /// Update debug mode state from command options.
    /// </summary>
    public static void UpdateFromOptions(bool verbose, bool debug)
    {
        if (debug)
        {
            _debugMode = true;
            LogLevelConfiguration.PromoteTo(LogLevelThreshold.Debug);
        }
        if (verbose)
        {
            _verboseMode = true;
            LogLevelConfiguration.PromoteTo(LogLevelThreshold.Info);
        }
    }
}
