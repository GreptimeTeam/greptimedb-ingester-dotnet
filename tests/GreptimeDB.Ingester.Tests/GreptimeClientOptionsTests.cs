using FluentAssertions;
using GreptimeDB.Ingester.Client;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class GreptimeClientOptionsTests
{
    [Fact]
    public void Validate_DefaultOptions_Passes()
    {
        var options = new GreptimeClientOptions();

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_SingleEndpoint_Passes()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "http://host-a:4001" }
        };

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_MultipleEndpoints_Passes()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "http://host-a:4001", "http://host-b:4001" }
        };

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Fact]
    public void ResolveEndpoints_PrefersEndpointsOverDeprecatedEndpoint()
    {
#pragma warning disable CS0618 // intentional: verify precedence of Endpoints over deprecated Endpoint
        var options = new GreptimeClientOptions
        {
            Endpoint = "http://legacy:4001",
            Endpoints = new List<string> { "http://host-a:4001", "http://host-b:4001" }
        };
#pragma warning restore CS0618

        options.ResolveEndpoints().Should().Equal("http://host-a:4001", "http://host-b:4001");
    }

    [Fact]
    public void ResolveEndpoints_EmptyEndpoints_FallsBackToDeprecatedEndpoint()
    {
#pragma warning disable CS0618
        var options = new GreptimeClientOptions
        {
            Endpoint = "http://fallback:4001",
            Endpoints = new List<string>()
        };
#pragma warning restore CS0618

        options.ResolveEndpoints().Should().Equal("http://fallback:4001");
    }

    [Fact]
    public void Validate_EmptyEndpointsAndEmptyEndpoint_Throws()
    {
#pragma warning disable CS0618
        var options = new GreptimeClientOptions
        {
            Endpoint = string.Empty,
            Endpoints = new List<string>()
        };
#pragma warning restore CS0618

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*endpoint*");
    }

    [Fact]
    public void Validate_InvalidUri_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "not a uri" }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*Invalid endpoint URI*");
    }

    [Fact]
    public void Validate_MixedSchemes_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "http://host-a:4001", "https://host-b:4001" }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*same scheme*");
    }

    [Fact]
    public void Validate_NonHttpScheme_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "ftp://host:4001" }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*unsupported scheme*");
    }

    [Fact]
    public void Validate_EndpointsAllWhitespace_ThrowsAndDoesNotFallBack()
    {
#pragma warning disable CS0618 // verify that an explicit but all-whitespace Endpoints does not silently fall back to deprecated Endpoint
        var options = new GreptimeClientOptions
        {
            Endpoint = "http://fallback:4001",
            Endpoints = new List<string> { "  ", string.Empty, "\t" }
        };
#pragma warning restore CS0618

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*no non-whitespace*");
    }

    [Fact]
    public void ResolveEndpoints_FiltersWhitespaceAndTrims()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "  http://host-a:4001  ", string.Empty, "http://host-b:4001" }
        };

        options.ResolveEndpoints().Should().Equal("http://host-a:4001", "http://host-b:4001");
    }
}
