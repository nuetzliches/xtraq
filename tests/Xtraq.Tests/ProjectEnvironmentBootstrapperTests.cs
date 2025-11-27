using System;
using System.IO;
using Xtraq.Cli;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProjectEnvironmentBootstrapperTests
{
    [Fact]
    public void ResolveExampleTemplateContent_UsesPackagedTemplate_WithAlternateSeparators()
    {
        var repoRoot = FindRepoRoot();
        var templatePath = Path.Combine(repoRoot, "src", "Templates", ".env.example");
        Assert.True(File.Exists(templatePath));

        var altSeparatedRoot = repoRoot.Replace(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var content = ProjectEnvironmentBootstrapper.ResolveExampleTemplateContent(altSeparatedRoot, explicitTemplate: null);

        Assert.Equal(File.ReadAllText(templatePath), content);
    }

    private static string FindRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            var candidateReadme = Path.Combine(current.FullName, "README.md");
            var candidateSrc = Path.Combine(current.FullName, "src");
            if (File.Exists(candidateReadme) && Directory.Exists(candidateSrc))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new InvalidOperationException("Repository root not found from test base directory.");
    }
}
