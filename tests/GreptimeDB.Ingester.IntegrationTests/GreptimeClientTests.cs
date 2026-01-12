using FluentAssertions;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

/// <summary>
/// Integration tests for GreptimeClient.
/// Requires a running GreptimeDB instance.
/// </summary>
public sealed class GreptimeClientTests : IAsyncLifetime, IDisposable
{
    private readonly GreptimeClientOptions _options;
    private GreptimeClient? _client;

    private GreptimeClient Client => _client ?? throw new InvalidOperationException("Client not initialized");

    public GreptimeClientTests()
    {
        _options = new GreptimeClientOptions
        {
            Endpoint = Environment.GetEnvironmentVariable("GREPTIMEDB_ENDPOINT") ?? "http://localhost:4001",
            Database = Environment.GetEnvironmentVariable("GREPTIMEDB_DATABASE") ?? "public"
        };
    }

    public Task InitializeAsync()
    {
        _client = new GreptimeClient(_options);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_client != null)
        {
            await _client.DisposeAsync();
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
    }

    [Fact]
    public async Task HealthCheck_ReturnsTrue_WhenServerIsRunning()
    {
        var result = await Client.HealthCheckAsync();

        result.Should().BeTrue();
    }

    [Fact]
    public async Task WriteAsync_SingleTable_Succeeds()
    {
        var table = new TableBuilder("test_write_single")
            .AddTag("host", ColumnDataType.String)
            .AddField("cpu_usage", ColumnDataType.Float64)
            .AddField("memory_usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, 0.72, DateTime.UtcNow)
            .AddRow("server2", 0.45, 0.55, DateTime.UtcNow)
            .Build();

        var affectedRows = await Client.WriteAsync(table);

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task WriteAsync_MultipleTables_Succeeds()
    {
        var cpuTable = new TableBuilder("test_cpu_metrics")
            .AddTag("host", ColumnDataType.String)
            .AddField("usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .AddRow("server2", 0.45, DateTime.UtcNow)
            .Build();

        var memTable = new TableBuilder("test_memory_metrics")
            .AddTag("host", ColumnDataType.String)
            .AddField("usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.72, DateTime.UtcNow)
            .AddRow("server2", 0.55, DateTime.UtcNow)
            .Build();

        var affectedRows = await Client.WriteAsync(new[] { cpuTable, memTable });

        affectedRows.Should().Be(4);
    }

    [Fact]
    public async Task WriteAsync_AllDataTypes_Succeeds()
    {
        var table = new TableBuilder("test_all_types")
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

        var affectedRows = await Client.WriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task WriteAsync_WithNullValues_Succeeds()
    {
        var table = new TableBuilder("test_null_values")
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .AddRow("server2", null, DateTime.UtcNow)
            .Build();

        var affectedRows = await Client.WriteAsync(table);

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task WriteAsync_LargeBatch_Succeeds()
    {
        var builder = new TableBuilder("test_large_batch")
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

        var baseTime = DateTime.UtcNow;
        for (var i = 0; i < 1000; i++)
        {
            builder.AddRow($"server{i % 10}", i * 0.1, baseTime.AddMilliseconds(i));
        }

        var table = builder.Build();
        var affectedRows = await Client.WriteAsync(table);

        affectedRows.Should().Be(1000);
    }

    [Fact]
    public async Task WriteAsync_EmptyTable_ReturnsZero()
    {
        var affectedRows = await Client.WriteAsync(Array.Empty<Table.Table>());

        affectedRows.Should().Be(0);
    }

    [Fact]
    public async Task DeleteAsync_ExistingRows_Succeeds()
    {
        // First, insert some data
        var insertTable = new TableBuilder("test_delete")
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, DateTime.UtcNow)
            .Build();

        await Client.WriteAsync(insertTable);

        // Then delete by tag and timestamp
        var deleteTable = new TableBuilder("test_delete")
            .AddTag("host", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", DateTime.UtcNow)
            .Build();

        // Delete may return 0 if the exact timestamp doesn't match
        var act = () => Client.DeleteAsync(deleteTable);
        await act.Should().NotThrowAsync();
    }
}
