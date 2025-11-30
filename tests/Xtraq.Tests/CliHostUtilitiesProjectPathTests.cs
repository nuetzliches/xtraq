using System;
using System.IO;
using Xtraq.Cli.Hosting;
using Xtraq.Configuration;

namespace Xtraq.Tests.Cli;

public sealed class CliHostUtilitiesProjectPathTests
{
    [Xunit.Fact]
    public void NormalizeProjectPath_WithExplicitDirectory_DoesNotClimbToParent()
    {
        var sandbox = Directory.CreateTempSubdirectory("xtraq-cli-lock-");
        try
        {
            // Parent config redirects to debug to simulate repo-level bootstrap.
            File.WriteAllText(Path.Combine(sandbox.FullName, ".xtraqconfig"), "{ \"ProjectPath\": \"debug\" }");
            var debugDir = Directory.CreateDirectory(Path.Combine(sandbox.FullName, "debug"));
            File.WriteAllText(Path.Combine(debugDir.FullName, ".xtraqconfig"), "{ \"Namespace\": \"Debug.Root\" }");
            var sampleDir = Directory.CreateDirectory(Path.Combine(sandbox.FullName, "samples", "restapi"));

            var originalCwd = Directory.GetCurrentDirectory();
            try
            {
                Directory.SetCurrentDirectory(sandbox.FullName);
                var resolved = CliHostUtilities.NormalizeProjectPath(sampleDir.FullName);
                var expected = Path.GetFullPath(sampleDir.FullName);
                Xunit.Assert.Equal(expected, resolved);
                Xunit.Assert.Equal(expected, Environment.GetEnvironmentVariable(TrackableConfigManager.ProjectRootLockEnvironmentVariableName));

                var located = TrackableConfigManager.LocateConfigDirectory(sampleDir.FullName);
                Xunit.Assert.Equal(expected, located);
            }
            finally
            {
                Directory.SetCurrentDirectory(originalCwd);
                Environment.SetEnvironmentVariable(TrackableConfigManager.ProjectRootLockEnvironmentVariableName, null);
            }
        }
        finally
        {
            TryDeleteDirectory(sandbox.FullName);
        }
    }

    [Xunit.Fact]
    public void NormalizeProjectPath_WhenNotSpecified_UsesTrackedParent()
    {
        var sandbox = Directory.CreateTempSubdirectory("xtraq-cli-default-");
        try
        {
            File.WriteAllText(Path.Combine(sandbox.FullName, ".xtraqconfig"), "{ \"ProjectPath\": \"debug\" }");
            var debugDir = Directory.CreateDirectory(Path.Combine(sandbox.FullName, "debug"));
            File.WriteAllText(Path.Combine(debugDir.FullName, ".xtraqconfig"), "{ \"Namespace\": \"Debug.Root\" }");
            var sampleDir = Directory.CreateDirectory(Path.Combine(sandbox.FullName, "samples", "restapi"));

            var originalCwd = Directory.GetCurrentDirectory();
            try
            {
                Directory.SetCurrentDirectory(sampleDir.FullName);
                var resolved = CliHostUtilities.NormalizeProjectPath(null);
                Xunit.Assert.Equal(Path.GetFullPath(debugDir.FullName), resolved);
                Xunit.Assert.Null(Environment.GetEnvironmentVariable(TrackableConfigManager.ProjectRootLockEnvironmentVariableName));
            }
            finally
            {
                Directory.SetCurrentDirectory(originalCwd);
                Environment.SetEnvironmentVariable(TrackableConfigManager.ProjectRootLockEnvironmentVariableName, null);
            }
        }
        finally
        {
            TryDeleteDirectory(sandbox.FullName);
        }
    }

    private static void TryDeleteDirectory(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
        }
    }
}
