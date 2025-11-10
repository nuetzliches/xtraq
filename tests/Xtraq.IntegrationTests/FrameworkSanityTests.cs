namespace Xtraq.IntegrationTests;

/// <summary>
/// Basic smoke tests to ensure the shared test infrastructure is wired correctly.
/// </summary>
public sealed class FrameworkSanityTests : Xtraq.TestFramework.XtraqTestBase
{
    [Xunit.Fact]
    public void ConnectionString_ShouldResolve_FromEnvironmentOrFallback()
    {
        var connectionString = GetTestConnectionString();
        Xunit.Assert.False(string.IsNullOrWhiteSpace(connectionString));
    }

    [Xunit.Fact]
    public void Validator_ShouldFail_WhenConfigurationMissing()
    {
        var valid = Xtraq.TestFramework.XtraqValidator.ValidateProjectConfiguration(System.Guid.NewGuid().ToString("N") + ".json", out var errors);
        Xunit.Assert.False(valid);
        Xunit.Assert.NotEmpty(errors);
        Xunit.Assert.Contains("Configuration file not found", errors[0], StringComparison.OrdinalIgnoreCase);
    }
}
