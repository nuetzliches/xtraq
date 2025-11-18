using System;
using System.Collections.Generic;
using System.IO;
using Xtraq.Utils;
using Xunit;

namespace Xtraq.Tests;

public sealed class EnvFileLoaderTests
{
    [Fact]
    public void Apply_WhenVariablesMissing_SetsEnvironment()
    {
        var envPath = Path.Combine(Path.GetTempPath(), "xtraq-env-" + Guid.NewGuid().ToString("N") + ".env");
        File.WriteAllText(envPath, "XTRAQ_LOG_LEVEL=Debug\nXTRAQ_ALIAS_DEBUG=1\n");

        var snapshot = Capture(new[] { "XTRAQ_LOG_LEVEL", "XTRAQ_ALIAS_DEBUG" });
        try
        {
            EnvFileLoader.Apply(envPath);

            Assert.Equal("Debug", Environment.GetEnvironmentVariable("XTRAQ_LOG_LEVEL"));
            Assert.True(EnvironmentHelper.IsTrue("XTRAQ_ALIAS_DEBUG"));
        }
        finally
        {
            Restore(snapshot);
            TryDelete(envPath);
        }
    }

    [Fact]
    public void Apply_WhenOverwriteDisabled_PreservesExistingValues()
    {
        var envPath = Path.Combine(Path.GetTempPath(), "xtraq-env-" + Guid.NewGuid().ToString("N") + ".env");
        File.WriteAllText(envPath, "XTRAQ_LOG_LEVEL=Debug\n");

        var snapshot = Capture(new[] { "XTRAQ_LOG_LEVEL" });
        Environment.SetEnvironmentVariable("XTRAQ_LOG_LEVEL", "info");

        try
        {
            EnvFileLoader.Apply(envPath, overwrite: false);

            Assert.Equal("info", Environment.GetEnvironmentVariable("XTRAQ_LOG_LEVEL"));
        }
        finally
        {
            Restore(snapshot);
            TryDelete(envPath);
        }
    }

    private static Dictionary<string, string?> Capture(IEnumerable<string> keys)
    {
        var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in keys)
        {
            map[key] = Environment.GetEnvironmentVariable(key);
        }

        return map;
    }

    private static void Restore(Dictionary<string, string?> snapshot)
    {
        foreach (var pair in snapshot)
        {
            Environment.SetEnvironmentVariable(pair.Key, pair.Value);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }
}
