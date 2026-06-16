using FluentAssertions;
using GreptimeDB.Ingester.Client;
using Grpc.Core;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class EndpointSelectorTests
{
    private static readonly string[] Endpoints = ["http://a:4001", "http://b:4001", "http://c:4001"];

    [Fact]
    public void Select_RoundRobin_RotatesFromFirstEndpoint()
    {
        var selector = CreateSelector(LoadBalancingStrategy.RoundRobin);

        var selected = new[]
        {
            selector.Select(),
            selector.Select(),
            selector.Select(),
            selector.Select(),
        };

        selected.Should().Equal(
            "http://a:4001",
            "http://b:4001",
            "http://c:4001",
            "http://a:4001");
    }

    [Fact]
    public void Select_ExcludesAlreadyFailedEndpoints()
    {
        var selector = CreateSelector(LoadBalancingStrategy.RoundRobin);
        var excluded = new HashSet<string>(StringComparer.Ordinal)
        {
            "http://a:4001",
            "http://b:4001",
        };

        selector.Select(excluded).Should().Be("http://c:4001");
    }

    [Fact]
    public void Select_FallsOpen_WhenEveryEndpointIsExcluded()
    {
        var selector = CreateSelector(LoadBalancingStrategy.RoundRobin);
        var excluded = Endpoints.ToHashSet(StringComparer.Ordinal);

        selector.Select(excluded).Should().Be("http://a:4001");
    }

    [Fact]
    public void ReportFailure_EjectsEndpointAfterConsecutiveFailures()
    {
        var selector = CreateSelector(
            LoadBalancingStrategy.RoundRobin,
            new FailoverOptions
            {
                ConsecutiveFailuresBeforeEjection = 2,
                BaseEjectionDelay = TimeSpan.FromMinutes(1),
                MaxEjectionDelay = TimeSpan.FromMinutes(1),
            });

        selector.ReportFailure("http://a:4001");
        selector.ReportFailure("http://a:4001");

        var selected = new[]
        {
            selector.Select(),
            selector.Select(),
            selector.Select(),
            selector.Select(),
        };

        selected.Should().NotContain("http://a:4001");
    }

    [Fact]
    public void ReportSuccess_ReadmitsEjectedEndpoint()
    {
        var selector = CreateSelector(
            LoadBalancingStrategy.RoundRobin,
            new FailoverOptions
            {
                ConsecutiveFailuresBeforeEjection = 1,
                BaseEjectionDelay = TimeSpan.FromMinutes(1),
                MaxEjectionDelay = TimeSpan.FromMinutes(1),
            });

        selector.ReportFailure("http://a:4001");
        selector.Select().Should().Be("http://b:4001");

        selector.ReportSuccess("http://a:4001");

        new[] { selector.Select(), selector.Select(), selector.Select() }
            .Should().Contain("http://a:4001");
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, true)]
    [InlineData(StatusCode.DeadlineExceeded, true)]
    [InlineData(StatusCode.ResourceExhausted, true)]
    [InlineData(StatusCode.InvalidArgument, false)]
    [InlineData(StatusCode.Internal, false)]
    public void IsEndpointFailure_ClassifiesRpcStatus(StatusCode statusCode, bool expected)
    {
        var exception = new RpcException(new Status(statusCode, "test"));

        EndpointSelector.IsEndpointFailure(exception).Should().Be(expected);
    }

    [Fact]
    public void IsEndpointFailure_TreatsTimeoutAsEndpointFailure()
    {
        EndpointSelector.IsEndpointFailure(new TimeoutException()).Should().BeTrue();
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, true)]
    [InlineData(StatusCode.ResourceExhausted, true)]
    [InlineData(StatusCode.DeadlineExceeded, false)]
    [InlineData(StatusCode.InvalidArgument, false)]
    public void IsRetryableUnaryWriteFailure_DoesNotRetryAmbiguousWriteTimeouts(
        StatusCode statusCode,
        bool expected)
    {
        var exception = new RpcException(new Status(statusCode, "test"));

        GreptimeClient.IsRetryableUnaryWriteFailure(exception).Should().Be(expected);
    }

    [Fact]
    public void CreateCallOptions_UsesCallerProvidedDeadline()
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        using var cancellationTokenSource = new CancellationTokenSource();

        var callOptions = GreptimeClient.CreateCallOptions(deadline, cancellationTokenSource.Token);

        callOptions.Deadline.Should().Be(deadline);
        callOptions.CancellationToken.Should().Be(cancellationTokenSource.Token);
    }

    private static EndpointSelector CreateSelector(
        LoadBalancingStrategy strategy,
        FailoverOptions? options = null)
    {
        return new EndpointSelector(Endpoints, strategy, options ?? new FailoverOptions());
    }
}
