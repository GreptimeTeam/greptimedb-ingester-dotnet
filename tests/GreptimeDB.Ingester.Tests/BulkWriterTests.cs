using Apache.Arrow.Flight;
using FluentAssertions;
using Google.Protobuf;
using GreptimeDB.Ingester.Client;
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
        var writer = CreateWriter();

        await writer.DrainResponsesAsync(mockStream.Object);

        var rows = GetServerAffectedRows(writer);
        rows.Should().Be(40);
    }

    [Fact]
    public async Task DrainResponsesAsync_CapturesRpcException()
    {
        var mockStream = new Mock<IAsyncStreamReader<FlightPutResult>>();
        mockStream.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new RpcException(new Status(StatusCode.Internal, "server error")));

        var writer = CreateWriter();

        await writer.DrainResponsesAsync(mockStream.Object);

        var error = GetRecvError(writer);
        error.Should().BeOfType<RpcException>();
    }

    private static BulkWriter CreateWriter()
    {
        return new BulkWriter(null!, "test-db", null);
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

    private static uint GetServerAffectedRows(BulkWriter writer)
    {
        var field = typeof(BulkWriter).GetField("_serverAffectedRows",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (uint)field!.GetValue(writer)!;
    }

    private static Exception? GetRecvError(BulkWriter writer)
    {
        var field = typeof(BulkWriter).GetField("_recvError",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (Exception?)field!.GetValue(writer);
    }
}
