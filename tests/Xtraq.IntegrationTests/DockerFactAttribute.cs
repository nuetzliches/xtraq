using System;
using Xunit;

namespace Xtraq.IntegrationTests;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class DockerFactAttribute : FactAttribute
{
    private const string SkipEnvVar = "XTRAQ_SKIP_DOCKER_TESTS";

    public DockerFactAttribute()
    {
        if (IsSkipped())
        {
            Skip = $"Docker-based integration tests explicitly skipped. Unset {SkipEnvVar} to run.";
        }
    }

    private static bool IsSkipped()
    {
        var value = Environment.GetEnvironmentVariable(SkipEnvVar);
        return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(value, "true", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase);
    }
}
