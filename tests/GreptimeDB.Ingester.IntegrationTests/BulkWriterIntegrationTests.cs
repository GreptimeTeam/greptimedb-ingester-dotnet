using FluentAssertions;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

/// <summary>
/// Integration tests for BulkWriter (Arrow Flight).
/// Requires a running GreptimeDB instance with Arrow Flight support.
/// NOTE: Arrow Flight DoPut requires tables to exist first. These tests
/// create tables using regular gRPC (which auto-creates) before testing BulkWriter.
/// </summary>
public sealed class BulkWriterIntegrationTests : IAsyncLifetime
{
    private readonly GreptimeClientOptions _options;
    private GreptimeClient? _client;

    private GreptimeClient Client => _client ?? throw new InvalidOperationException("Client not initialized");

    public BulkWriterIntegrationTests()
    {
        _options = new GreptimeClientOptions
        {
            Endpoint = Environment.GetEnvironmentVariable("GREPTIMEDB_ENDPOINT") ?? "http://localhost:4001",
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

    /// <summary>
    /// Creates a table by inserting a row using regular gRPC (which auto-creates tables).
    /// Arrow Flight DoPut requires tables to exist first.
    /// </summary>
    private async Task EnsureTableExistsAsync(TableBuilder builder, DateTime baseTime)
    {
        var initTable = builder.AddRow(GetDefaultRowValues(builder, baseTime)).Build();
        await using var writer = Client.CreateStreamIngestWriter();
        await writer.WriteAsync(initTable);
        await writer.CompleteAsync();
    }

    /// <summary>
    /// Gets default row values based on the column schema.
    /// </summary>
    private static object?[] GetDefaultRowValues(TableBuilder builder, DateTime baseTime)
    {
        var columns = builder.GetColumnDefinitions();
        var values = new object?[columns.Count];

        for (var i = 0; i < columns.Count; i++)
        {
            var col = columns[i];
            values[i] = col.DataType switch
            {
                ColumnDataType.String => "init",
                ColumnDataType.Boolean => false,
                ColumnDataType.Int8 => (sbyte)0,
                ColumnDataType.Int16 => (short)0,
                ColumnDataType.Int32 => 0,
                ColumnDataType.Int64 => 0L,
                ColumnDataType.UInt8 => (byte)0,
                ColumnDataType.UInt16 => (ushort)0,
                ColumnDataType.UInt32 => 0u,
                ColumnDataType.UInt64 => 0ul,
                ColumnDataType.Float32 => 0f,
                ColumnDataType.Float64 => 0.0,
                ColumnDataType.Binary => Array.Empty<byte>(),
                ColumnDataType.Date => baseTime.Date,
                ColumnDataType.TimestampSecond or
                ColumnDataType.TimestampMillisecond or
                ColumnDataType.TimestampMicrosecond or
                ColumnDataType.TimestampNanosecond => baseTime,
                _ => null
            };
        }

        return values;
    }

    [Fact]
    public async Task BulkWrite_SingleTable_DataPersisted()
    {
        var tableName = $"test_bulk_single_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("cpu_usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        // Now use BulkWriter
        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("cpu_usage", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, baseTime)
            .AddRow("server2", 0.45, baseTime.AddMilliseconds(1))
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task BulkWrite_WithBulkWriter_MultipleTables()
    {
        var tableName = $"test_bulk_multi_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        // Now use BulkWriter
        await using var writer = Client.CreateBulkWriter();

        // Write multiple batches to the same table
        for (var batch = 0; batch < 5; batch++)
        {
            var builder = new TableBuilder(tableName)
                .AddTag("host", ColumnDataType.String)
                .AddField("value", ColumnDataType.Float64)
                .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

            for (var i = 0; i < 10; i++)
            {
                var rowNum = batch * 10 + i;
                builder.AddRow($"server{rowNum % 5}", rowNum * 0.1, baseTime.AddMilliseconds(rowNum));
            }

            await writer.WriteAsync(builder.Build());
        }

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(50);
    }

    [Fact]
    public async Task BulkWrite_AllDataTypes_Succeeds()
    {
        var tableName = $"test_bulk_all_types_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
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
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        // Now use BulkWriter
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
                baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task BulkWrite_WithNullValues_Succeeds()
    {
        var tableName = $"test_bulk_null_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        // Now use BulkWriter
        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, baseTime)
            .AddRow("server2", null, baseTime.AddMilliseconds(1))
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task BulkWrite_LargeBatch_Succeeds()
    {
        var tableName = $"test_bulk_large_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-100));

        // Now use BulkWriter
        var builder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

        for (var i = 0; i < 10000; i++)
        {
            builder.AddRow($"server{i % 100}", i * 0.01, baseTime.AddMilliseconds(i));
        }

        var table = builder.Build();
        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(10000);
    }

    [Fact]
    public async Task BulkWrite_WriteAfterComplete_ThrowsInvalidOperation()
    {
        var tableName = $"test_bulk_write_after_complete_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        await using var writer = Client.CreateBulkWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        await writer.WriteAsync(table);
        await writer.CompleteAsync();

        // Write after Complete should throw
        var act = async () => await writer.WriteAsync(table);
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task BulkWrite_CompleteCalledTwice_ThrowsInvalidOperation()
    {
        var tableName = $"test_bulk_complete_twice_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        await using var writer = Client.CreateBulkWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        await writer.WriteAsync(table);
        await writer.CompleteAsync();

        // Second Complete should throw
        var act = async () => await writer.CompleteAsync();
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task BulkWrite_DisposeWithoutComplete_DoesNotThrow()
    {
        var tableName = $"test_bulk_dispose_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var writer = Client.CreateBulkWriter();

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        await writer.WriteAsync(table);

        // Dispose without calling Complete - should not throw
        var act = async () => await writer.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task BulkWrite_TimestampSecond_Succeeds()
    {
        var tableName = $"test_bulk_ts_second_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampSecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampSecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task BulkWrite_TimestampMicrosecond_Succeeds()
    {
        var tableName = $"test_bulk_ts_micro_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMicrosecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMicrosecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task BulkWrite_TimestampNanosecond_Succeeds()
    {
        var tableName = $"test_bulk_ts_nano_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampNanosecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("value", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampNanosecond)
            .AddRow("server1", 0.85, baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task BulkWrite_DateField_Succeeds()
    {
        var tableName = $"test_bulk_date_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("event_date", ColumnDataType.Date)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("event_date", ColumnDataType.Date)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", baseTime.Date, baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }

    [Fact]
    public async Task BulkWrite_CompleteWithoutWrite_ReturnsZero()
    {
        await using var writer = Client.CreateBulkWriter();

        var affectedRows = await writer.CompleteAsync();

        affectedRows.Should().Be(0);
    }

    [Fact]
    public async Task BulkWrite_MultipleNullFields_Succeeds()
    {
        var tableName = $"test_bulk_multi_null_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("int_field", ColumnDataType.Int64)
            .AddField("float_field", ColumnDataType.Float64)
            .AddField("string_field", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("int_field", ColumnDataType.Int64)
            .AddField("float_field", ColumnDataType.Float64)
            .AddField("string_field", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", 100L, 1.5, "hello", baseTime)
            .AddRow("server2", null, null, null, baseTime.AddMilliseconds(1))
            .AddRow("server3", 300L, 3.5, "world", baseTime.AddMilliseconds(2))
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(3);
    }

    [Fact]
    public async Task BulkWrite_EmptyStringAndBinary_Succeeds()
    {
        var tableName = $"test_bulk_empty_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("text_field", ColumnDataType.String)
            .AddField("binary_field", ColumnDataType.Binary)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("text_field", ColumnDataType.String)
            .AddField("binary_field", ColumnDataType.Binary)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", "", Array.Empty<byte>(), baseTime)
            .AddRow("server2", "non-empty", new byte[] { 1, 2, 3 }, baseTime.AddMilliseconds(1))
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(2);
    }

    [Fact]
    public async Task BulkWrite_ExtremeIntegerValues_Succeeds()
    {
        var tableName = $"test_bulk_extreme_int_{DateTime.UtcNow.Ticks}";
        var baseTime = DateTime.UtcNow;

        // Create the table first using regular gRPC
        var initBuilder = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("min_int64", ColumnDataType.Int64)
            .AddField("max_int64", ColumnDataType.Int64)
            .AddField("max_uint64", ColumnDataType.UInt64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);
        await EnsureTableExistsAsync(initBuilder, baseTime.AddSeconds(-10));

        var table = new TableBuilder(tableName)
            .AddTag("host", ColumnDataType.String)
            .AddField("min_int64", ColumnDataType.Int64)
            .AddField("max_int64", ColumnDataType.Int64)
            .AddField("max_uint64", ColumnDataType.UInt64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("server1", long.MinValue, long.MaxValue, ulong.MaxValue, baseTime)
            .Build();

        var affectedRows = await Client.BulkWriteAsync(table);

        affectedRows.Should().Be(1);
    }
}
