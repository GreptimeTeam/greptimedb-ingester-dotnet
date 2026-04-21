using FluentAssertions;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

/// <summary>
/// Integration tests for StreamIngestWriter.
/// Requires a running GreptimeDB instance.
/// </summary>
[Collection("GreptimeDB Integration")]
public sealed class StreamIngestWriterIntegrationTests : IAsyncLifetime
{
    private readonly GreptimeDbFixture _fixture;
    private readonly GreptimeClientOptions _options;
    private GreptimeClient? _client;

    private GreptimeClient Client => _client ?? throw new InvalidOperationException("Client not initialized");

    public StreamIngestWriterIntegrationTests(GreptimeDbFixture fixture)
    {
        _fixture = fixture;
        _options = new GreptimeClientOptions
        {
            Endpoints = new List<string> { _fixture.GetEndpoint() },
            Database = Environment.GetEnvironmentVariable("GREPTIMEDB_DATABASE") ?? "public"
        };
    }

    public ValueTask InitializeAsync()
    {
        _client = new GreptimeClient(_options);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_client != null)
        {
            await _client.DisposeAsync();
        }
    }

    [Fact]
    public async Task StreamWrite_SingleTable_DataPersisted()
    {
        var tableName = $"test_stream_single_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("cpu_usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .AddRow("server2", 0.45, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task StreamWrite_MultipleTables_AllPersisted()
    {
        var tableName1 = $"test_stream_multi1_{DateTime.UtcNow.Ticks}";
        var tableName2 = $"test_stream_multi2_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table1 = new TableBuilder(tableName1)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        var table2 = new TableBuilder(tableName2)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server2", 0.45, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table1);
        await writer.WriteAsync(table2);

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task StreamWrite_LargeBatch_Succeeds()
    {
        var tableName = $"test_stream_large_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var baseTime = DateTime.UtcNow;

        // Write 10 tables with 100 rows each = 1000 rows total
        for (var batch = 0; batch < 10; batch++)
        {
            var builder = new TableBuilder(tableName)
                .AddTag("host", ColumnDataType.String)
                .AddField("value", ColumnDataType.Float64)
                .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

            for (var i = 0; i < 100; i++)
            {
                var rowNum = batch * 100 + i;
                builder.AddRow($"server{rowNum % 10}", rowNum * 0.1, baseTime.AddMilliseconds(rowNum));
            }

            await writer.WriteAsync(builder.Build());
        }

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1000);
    }

    [Fact]
    public async Task StreamWrite_AllDataTypes_Succeeds()
    {
        var tableName = $"test_stream_all_types_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("tag_string", ColumnDataType.String)
            .AddField("field_bool", ColumnDataType.Boolean)
            .AddField("field_int8", ColumnDataType.Int8)
            .AddField("field_int16", ColumnDataType.Int16)
            .AddField("field_int32", ColumnDataType.Int32)
            .AddField("field_int64", ColumnDataType.Int64)
            .AddField("field_uint8", ColumnDataType.UInt8)
            .AddField("field_uint16", ColumnDataType.UInt16)
            .AddField("field_uint32", ColumnDataType.UInt32)
            .AddField("field_uint64", ColumnDataType.UInt64)
            .AddField("field_float32", ColumnDataType.Float32)
            .AddField("field_float64", ColumnDataType.Float64)
            .AddField("field_string", ColumnDataType.String)
            .AddField("field_binary", ColumnDataType.Binary)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(
                "tag1",
                true,
                (sbyte)1,
                (short)2,
                3,
                4L,
                (byte)5,
                (ushort)6,
                7u,
                8ul,
                1.5f,
                2.5,
                "hello",
                new byte[] { 1, 2, 3 },
                DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_WithNullValues_Succeeds()
    {
        var tableName = $"test_stream_null_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .AddRow("server2", null, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task StreamWrite_ConcurrentWrites_AllSucceed()
    {
        var tableName = $"test_stream_concurrent_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var baseTime = DateTime.UtcNow;

        // Write from multiple tasks concurrently
        var tasks = Enumerable.Range(0, 10).Select(async batch =>
        {
            var builder = new TableBuilder(tableName)
                .AddTag("host", ColumnDataType.String)
                .AddField("value", ColumnDataType.Float64)
                .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

            for (var i = 0; i < 10; i++)
            {
                var rowNum = batch * 10 + i;
                builder.AddRow($"server{rowNum % 10}", rowNum * 0.1, baseTime.AddMilliseconds(rowNum));
            }

            await writer.WriteAsync(builder.Build());
        });

        await Task.WhenAll(tasks);

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(100);
    }

    [Fact]
    public async Task StreamWrite_TryWrite_ReturnsTrue_WhenBufferNotFull()
    {
        var tableName = $"test_stream_trywrite_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        var result = writer.TryWrite(table);

        result.Should().BeTrue();

        var affectedRows = await writer.CompleteAsync();
        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_DisposeWithoutComplete_DoesNotThrow()
    {
        var tableName = $"test_stream_dispose_{DateTime.UtcNow.Ticks}";

        var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);

        // Dispose without calling Complete - should not throw
        var act = async () => await writer.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StreamWrite_WriteAfterComplete_ThrowsInvalidOperation()
    {
        var tableName = $"test_stream_write_after_complete_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        await writer.CompleteAsync();

        // Write after Complete should throw
        var act = async () => await writer.WriteAsync(table);
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task StreamWrite_CompleteCalledTwice_ThrowsInvalidOperation()
    {
        var tableName = $"test_stream_complete_twice_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        await writer.CompleteAsync();

        // Second Complete should throw
        var act = async () => await writer.CompleteAsync();
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task StreamWrite_TimestampSecond_Succeeds()
    {
        var tableName = $"test_stream_ts_second_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampSecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_TimestampMicrosecond_Succeeds()
    {
        var tableName = $"test_stream_ts_micro_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMicrosecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_TimestampNanosecond_Succeeds()
    {
        var tableName = $"test_stream_ts_nano_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampNanosecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_DateField_Succeeds()
    {
        var tableName = $"test_stream_date_{DateTime.UtcNow.Ticks}";

        await using var writer = Client.CreateStreamIngestWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("event_date", ColumnDataType.Date)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", DateTime.UtcNow.Date, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task StreamWrite_CompleteWithoutWrite_ReturnsZero()
    {
        await using var writer = Client.CreateStreamIngestWriter();

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(0);
    }

    [Fact]
    public async Task StreamWrite_WithCustomOptions_Succeeds()
    {
        var tableName = $"test_stream_options_{DateTime.UtcNow.Ticks}";

        var options = new StreamIngestWriterOptions
        {
            BufferCapacity = 100,
            WriteTimeout = TimeSpan.FromSeconds(60)
        };

        await using var writer = Client.CreateStreamIngestWriter(options);

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await writer.WriteAsync(table);
        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(1);
    }
}
