using Apache.Arrow.Flight;
using FluentAssertions;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Exceptions;
using Grpc.Core;
using Moq;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class BulkWriterTests
{
    [Fact]
    public async Task DrainResponsesAsync_AccumulatesAffectedRows()
    {
        var responses = new[]
        {
            new FlightPutResult("""{"affected_rows": 10}"""),
            new FlightPutResult("""{"affected_rows": 25}"""),
            new FlightPutResult("""{"affected_rows": 5}"""),
        };

        var mockStream = CreateMockStream(responses);

        var (affectedRows, error) = await BulkWriter.DrainResponsesAsync(mockStream.Object);

        affectedRows.Should().Be(40);
        error.Should().BeNull();
    }

    [Fact]
    public async Task DrainResponsesAsync_CapturesRpcException()
    {
        var mockStream = new Mock<IAsyncStreamReader<FlightPutResult>>();
        mockStream.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new RpcException(new Status(StatusCode.Internal, "server error")));

        var (affectedRows, error) = await BulkWriter.DrainResponsesAsync(mockStream.Object);

        affectedRows.Should().Be(0);
        error.Should().BeOfType<RpcException>();
    }

    private static Mock<IAsyncStreamReader<FlightPutResult>> CreateMockStream(FlightPutResult[] responses)
    {
        var mock = new Mock<IAsyncStreamReader<FlightPutResult>>();
        var index = -1;

        mock.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                index++;
                return index < responses.Length;
            });

        mock.Setup(s => s.Current)
            .Returns(() => responses[index]);

        return mock;
    }
}
