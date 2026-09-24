using FluentAssertions;
using GreptimeDB.Ingester.Internal;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class RequestHintsTests
{
    [Fact]
    public void ToMetadata_EncodesHintsAsSingleHeader()
    {
        var hints = new Dictionary<string, string>
        {
            ["append_mode"] = "true",
            ["ttl"] = "7d",
        };

        var metadata = RequestHints.ToMetadata(hints, "hints");

        metadata.Should().ContainSingle();
        metadata![0].Key.Should().Be("x-greptime-hints");
        metadata[0].Value.Should().Be("append_mode=true,ttl=7d");
    }

    [Theory]
    [InlineData("", "true")]
    [InlineData("a,b", "true")]
    [InlineData("a=b", "true")]
    [InlineData("append_mode", "true,ttl=1d")]
    [InlineData("append_mode", "trué")]
    [InlineData("append_mode", "a\nb")]
    public void ToMetadata_UnencodableHint_Throws(string key, string value)
    {
        var act = () => RequestHints.ToMetadata(new Dictionary<string, string> { [key] = value }, "hints");

        act.Should().Throw<ArgumentException>().WithParameterName("hints");
    }
}
