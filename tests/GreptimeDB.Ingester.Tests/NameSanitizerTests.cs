using FluentAssertions;
using GreptimeDB.Ingester.Internal;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class NameSanitizerTests
{
    [Theory]
    [InlineData("MyTableName", "my_table_name")]
    [InlineData("myTableName", "my_table_name")]
    [InlineData("my_table_name", "my_table_name")]
    [InlineData("MY_TABLE_NAME", "my_table_name")]
    [InlineData("XMLParser", "xml_parser")]
    [InlineData("parseXML", "parse_xml")]
    [InlineData("CPUUsage", "cpu_usage")]
    public void Sanitize_CamelCaseToSnakeCase_Succeeds(string input, string expected)
    {
        var result = NameSanitizer.Sanitize(input);
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("  spaces  ", "spaces")]
    [InlineData("  LeadingSpaces", "leading_spaces")]
    [InlineData("TrailingSpaces  ", "trailing_spaces")]
    public void Sanitize_TrimsWhitespace(string input, string expected)
    {
        var result = NameSanitizer.Sanitize(input);
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("hello-world", "hello_world")]
    [InlineData("hello world", "hello_world")]
    [InlineData("hello--world", "hello_world")]
    [InlineData("hello  world", "hello_world")]
    public void Sanitize_ReplacesDelimitersWithUnderscores(string input, string expected)
    {
        var result = NameSanitizer.Sanitize(input);
        result.Should().Be(expected);
    }

    [Fact]
    public void Sanitize_LongName_Truncates()
    {
        var longName = new string('a', 150);
        var result = NameSanitizer.Sanitize(longName);
        result.Length.Should().BeLessThanOrEqualTo(99);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Sanitize_EmptyOrNull_ThrowsArgumentException(string? input)
    {
        var act = () => NameSanitizer.Sanitize(input!);
        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData("table123", "table123")]
    [InlineData("Table123Name", "table123_name")]
    [InlineData("123table", "123table")]
    public void Sanitize_WithNumbers_PreservesNumbers(string input, string expected)
    {
        var result = NameSanitizer.Sanitize(input);
        result.Should().Be(expected);
    }

    [Fact]
    public void Sanitize_AlreadySnakeCase_NoChange()
    {
        var result = NameSanitizer.Sanitize("already_snake_case");
        result.Should().Be("already_snake_case");
    }

    [Theory]
    [InlineData("a", "a")]
    [InlineData("A", "a")]
    [InlineData("_", "_")]
    public void Sanitize_SingleCharacter_Succeeds(string input, string expected)
    {
        var result = NameSanitizer.Sanitize(input);
        result.Should().Be(expected);
    }
}
