using System.Collections.Generic;
using System.Text.Json;

namespace Xtraq.IntegrationTests;

/// <summary>
/// Ensures the runtime JSON patterns used by generated code behave as expected.
/// </summary>
public sealed class JsonRuntimeDeserializationTests
{
    private sealed record UserListAsJson(string Id, string Name);
    private sealed record UserFindAsJson(string Id, string Name);

    private static string GetArrayJson() => "[{\"Id\":\"1\",\"Name\":\"Alice\"},{\"Id\":\"2\",\"Name\":\"Bob\"}]";
    private static string GetSingleJson() => "{\"Id\":\"42\",\"Name\":\"Zaphod\"}";
    private static string GetSingleJsonWithoutWrapper() => "{\"Id\":\"7\",\"Name\":\"Trillian\"}";

    [Xunit.Fact]
    public void Deserialize_List_Model_ShouldWork()
    {
        var raw = GetArrayJson();
        var typed = JsonSerializer.Deserialize<List<UserListAsJson>>(raw) ?? new List<UserListAsJson>();

        Xunit.Assert.Equal(2, typed.Count);
        Xunit.Assert.Equal("1", typed[0].Id);
        Xunit.Assert.Equal("Bob", typed[1].Name);
    }

    [Xunit.Fact]
    public void Deserialize_Single_Model_ShouldWork()
    {
        var raw = GetSingleJson();
        var typed = JsonSerializer.Deserialize<UserFindAsJson>(raw);

        Xunit.Assert.NotNull(typed);
        Xunit.Assert.Equal("42", typed!.Id);
        Xunit.Assert.Equal("Zaphod", typed.Name);
    }

    [Xunit.Fact]
    public void Deserialize_List_Null_ShouldReturnEmptyList()
    {
        const string raw = "null";
        var typed = JsonSerializer.Deserialize<List<UserListAsJson>>(raw) ?? new List<UserListAsJson>();
        Xunit.Assert.Empty(typed);
    }

    [Xunit.Fact]
    public void Deserialize_List_EmptyArray_ShouldReturnEmptyList()
    {
        const string raw = "[]";
        var typed = JsonSerializer.Deserialize<List<UserListAsJson>>(raw) ?? new List<UserListAsJson>();
        Xunit.Assert.Empty(typed);
    }

    [Xunit.Fact]
    public void Deserialize_List_WithWhitespace_ShouldTrimSuccessfully()
    {
        var raw = "  \n  " + GetArrayJson() + "  \n  ";
        var typed = JsonSerializer.Deserialize<List<UserListAsJson>>(raw.Trim()) ?? new List<UserListAsJson>();
        Xunit.Assert.Equal(2, typed.Count);
    }

    [Xunit.Fact]
    public void Deserialize_Single_NoArrayWrapper_ShouldWork()
    {
        var raw = GetSingleJsonWithoutWrapper();
        var typed = JsonSerializer.Deserialize<UserFindAsJson>(raw);

        Xunit.Assert.NotNull(typed);
        Xunit.Assert.Equal("Trillian", typed!.Name);
    }

    [Xunit.Fact]
    public void Deserialize_MalformedJson_ShouldThrow()
    {
        const string raw = "{\"Id\":\"1\",\"Name\":\"Broken\",}";
#pragma warning disable IL2026, IL3050
        var exception = Xunit.Record.Exception(() => JsonSerializer.Deserialize<UserFindAsJson>(raw));
#pragma warning restore IL2026, IL3050
        Xunit.Assert.IsType<JsonException>(exception);
    }
}
