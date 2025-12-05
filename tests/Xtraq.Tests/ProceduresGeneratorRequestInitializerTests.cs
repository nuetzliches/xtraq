using Xtraq.Generators;
using Xunit;

namespace Xtraq.Tests;

public sealed class ProceduresGeneratorRequestInitializerTests
{
    [Theory]
    [InlineData("string", " = default!;")]
    [InlineData("global::System.String", " = default!;")]
    [InlineData("IReadOnlyList<ActorRef>", " = default!;")]
    [InlineData("ActorRef[]", " = default!;")]
    [InlineData("string?", "")]
    [InlineData("int", "")]
    [InlineData("int?", "")]
    public void BuildRequestPropertyInitializer_ReturnsExpectedSuffix(string clrType, string expectedInitializer)
    {
        var initializer = ProceduresGenerator.BuildRequestPropertyInitializer(clrType);
        Assert.Equal(expectedInitializer, initializer);
    }
}
