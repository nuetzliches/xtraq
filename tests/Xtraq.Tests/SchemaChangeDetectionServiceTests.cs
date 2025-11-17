using System;
using Xtraq.Data;
using Xunit;

namespace Xtraq.Tests.Data;

/// <summary>
/// Regression coverage for <see cref="SchemaChangeDetectionService"/> timestamp normalization.
/// </summary>
public sealed class SchemaChangeDetectionServiceTests
{
    [Fact]
    public void NormalizeSqlTimestamp_WhenUnspecified_AssumesUtcWithoutApplyingOffset()
    {
        var original = new DateTime(2025, 11, 17, 12, 0, 0, DateTimeKind.Unspecified);

        var normalized = SchemaChangeDetectionService.NormalizeSqlTimestamp(original);

        Assert.Equal(DateTimeKind.Utc, normalized.Kind);
        Assert.Equal(original.Ticks, normalized.Ticks);

        var offset = TimeZoneInfo.Local.GetUtcOffset(original);
        if (offset != TimeSpan.Zero)
        {
            Assert.NotEqual(original.ToUniversalTime().Ticks, normalized.Ticks);
        }
    }

    [Fact]
    public void NormalizeSqlTimestamp_WhenUtc_ReturnsSameValue()
    {
        var original = new DateTime(2025, 11, 17, 12, 0, 0, DateTimeKind.Utc);

        var normalized = SchemaChangeDetectionService.NormalizeSqlTimestamp(original);

        Assert.Equal(original, normalized);
        Assert.Equal(DateTimeKind.Utc, normalized.Kind);
    }

    [Fact]
    public void NormalizeSqlTimestamp_WhenLocal_ConvertsToUtc()
    {
        var local = new DateTime(2025, 11, 17, 12, 0, 0, DateTimeKind.Local);
        var expected = local.ToUniversalTime();

        var normalized = SchemaChangeDetectionService.NormalizeSqlTimestamp(local);

        Assert.Equal(expected, normalized);
        Assert.Equal(DateTimeKind.Utc, normalized.Kind);
    }
}
