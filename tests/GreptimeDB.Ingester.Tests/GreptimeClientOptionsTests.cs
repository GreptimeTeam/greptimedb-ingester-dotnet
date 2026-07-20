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
    public void LoadBalancing_DefaultsToRandom()
    {
        new GreptimeClientOptions().LoadBalancing.Should().Be(LoadBalancingStrategy.Random);
    }

    [Fact]
    public void Failover_DefaultsToEnabled()
    {
        var failover = new GreptimeClientOptions().Failover;

        failover.Enabled.Should().BeTrue();
        failover.MaxAttempts.Should().BeNull();
        failover.ConsecutiveFailuresBeforeEjection.Should().Be(5);
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
    public void Validate_DuplicateEndpoints_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "http://host:4001", "http://host:4001" }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*Duplicate endpoint*");
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

    [Theory]
    [InlineData(LoadBalancingStrategy.Random)]
    [InlineData(LoadBalancingStrategy.RoundRobin)]
    public void Constructor_MultiEndpoint_AcceptsAllStrategies(LoadBalancingStrategy strategy)
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { "http://host-a:4001", "http://host-b:4001" },
            LoadBalancing = strategy,
        };

        var act = () =>
        {
            using var client = new GreptimeClient(options);
        };

        act.Should().NotThrow();
    }

    [Theory]
    [InlineData("http://host:4001/v1")]
    [InlineData("http://host:4001/?foo=1")]
    [InlineData("http://host:4001/#frag")]
    public void Validate_EndpointWithPathQueryOrFragment_Throws(string endpoint)
    {
        var options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { endpoint }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*without a path, query, or fragment*");
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

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_InvalidFailoverMaxAttempts_Throws(int maxAttempts)
    {
        var options = new GreptimeClientOptions
        {
            Failover = new FailoverOptions
            {
                MaxAttempts = maxAttempts
            }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*MaxAttempts*");
    }

    [Fact]
    public void Validate_InvalidFailoverEjectionDelay_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Failover = new FailoverOptions
            {
                BaseEjectionDelay = TimeSpan.FromSeconds(10),
                MaxEjectionDelay = TimeSpan.FromSeconds(1)
            }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*MaxEjectionDelay*");
    }

    [Fact]
    public void Validate_NullFailover_Throws()
    {
        var options = new GreptimeClientOptions
        {
            Failover = null!
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*Failover*");
    }

    [Fact]
    public void KeepAlive_DefaultsToEnabled()
    {
        var keepAlive = new GreptimeClientOptions().KeepAlive;

        keepAlive.Enabled.Should().BeTrue();
        keepAlive.PingDelay.Should().Be(TimeSpan.FromSeconds(30));
        keepAlive.PingTimeout.Should().Be(TimeSpan.FromSeconds(10));
        keepAlive.PingWhileIdle.Should().BeTrue();
    }

    [Fact]
    public void Validate_KeepAlivePingDelayBelowMinimum_Throws()
    {
        var options = new GreptimeClientOptions
        {
            KeepAlive = new KeepAliveOptions { PingDelay = TimeSpan.FromMilliseconds(500) }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*PingDelay*");
    }

    [Fact]
    public void Validate_KeepAlivePingTimeoutBelowMinimum_Throws()
    {
        var options = new GreptimeClientOptions
        {
            KeepAlive = new KeepAliveOptions { PingTimeout = TimeSpan.Zero }
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*PingTimeout*");
    }

    [Fact]
    public void Validate_DisabledKeepAlive_SkipsIntervalChecks()
    {
        var options = new GreptimeClientOptions
        {
            KeepAlive = new KeepAliveOptions
            {
                Enabled = false,
                PingDelay = TimeSpan.Zero,
                PingTimeout = TimeSpan.Zero
            }
        };

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_NullKeepAlive_Throws()
    {
        var options = new GreptimeClientOptions
        {
            KeepAlive = null!
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>().WithMessage("*KeepAlive*");
    }
}
