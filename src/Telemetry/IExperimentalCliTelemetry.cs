
namespace Xtraq.Telemetry;

/// <summary>
/// Abstraction for recording experimental CLI usage events.
/// </summary>
internal interface IExperimentalCliTelemetry
{
    void Record(ExperimentalCliUsageEvent evt);
}

/// <summary>
/// Simple event data structure.
/// </summary>
internal sealed record ExperimentalCliUsageEvent(string command, string mode, TimeSpan duration, bool success);

/// <summary>
/// Console-based implementation (placeholder for future structured logging / OTLP export).
/// </summary>
internal sealed class ConsoleExperimentalCliTelemetry : IExperimentalCliTelemetry
{
    public void Record(ExperimentalCliUsageEvent evt)
    {
        // Only emit telemetry line when verbose mode enabled to reduce default console noise.
        if (Xtraq.Utils.EnvironmentHelper.IsTrue("XTRAQ_VERBOSE"))
        {
            Console.WriteLine($"[telemetry experimental-cli] command={evt.command} mode={evt.mode} success={evt.success} durationMs={evt.duration.TotalMilliseconds:F0}");
        }
    }
}
