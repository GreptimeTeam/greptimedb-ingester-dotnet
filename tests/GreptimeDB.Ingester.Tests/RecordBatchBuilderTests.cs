using Apache.Arrow;
using Apache.Arrow.Types;
using FluentAssertions;
using GreptimeDB.Ingester.Arrow;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class RecordBatchBuilderTests : IDisposable
{
    private readonly RecordBatchBuilder _builder;

    public RecordBatchBuilderTests()
    {
        _builder = new RecordBatchBuilder();
    }

    public void Dispose()
    {
        _builder.Dispose();
        GC.SuppressFinalize(this);
    }

    #region Schema Tests

    [Fact]
    public void Build_CreatesCorrectSchema()
    {
        var table = new TableBuilder("test_table")
            .AddTag("tag1", ColumnDataType.String)
            .AddField("field1", ColumnDataType.Int64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value1", 100L, DateTime.UtcNow)
            .Build();

        using var batch = _builder.Build(table);

        batch.Schema.FieldsList.Should().HaveCount(3);
        batch.Schema.FieldsList[0].Name.Should().Be("tag1");
        batch.Schema.FieldsList[0].DataType.Should().Be(StringType.Default);
        batch.Schema.FieldsList[1].Name.Should().Be("field1");
        batch.Schema.FieldsList[1].DataType.Should().Be(Int64Type.Default);
        batch.Schema.FieldsList[2].Name.Should().Be("ts");
        batch.Schema.FieldsList[2].DataType.Should().BeOfType<TimestampType>();
    }

    [Fact]
    public void Build_AllFieldsAreNullable()
    {
        var table = new TableBuilder("test_table")
            .AddTag("tag1", ColumnDataType.String)
            .AddField("field1", ColumnDataType.Int64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value1", 100L, DateTime.UtcNow)
            .Build();

        using var batch = _builder.Build(table);

        foreach (var field in batch.Schema.FieldsList)
        {
            field.IsNullable.Should().BeTrue($"Field {field.Name} should be nullable");
        }
    }

    #endregion

    #region Row Count Tests

    [Fact]
    public void Build_SingleRow_CorrectRowCount()
    {
        var table = new TableBuilder("test_table")
            .AddTag("tag1", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value1", DateTime.UtcNow)
            .Build();

        using var batch = _builder.Build(table);

        batch.Length.Should().Be(1);
    }

    [Fact]
    public void Build_MultipleRows_CorrectRowCount()
    {
        var builder = new TableBuilder("test_table")
            .AddTag("tag1", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

        for (int i = 0; i < 100; i++)
        {
            builder.AddRow($"value{i}", DateTime.UtcNow.AddMilliseconds(i));
        }

        var table = builder.Build();
        using var batch = _builder.Build(table);

        batch.Length.Should().Be(100);
    }

    #endregion

    #region Boolean Type Tests

    [Fact]
    public void Build_BooleanColumn_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("bool_field", ColumnDataType.Boolean)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(true, DateTime.UtcNow)
            .AddRow(false, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var boolArray = (BooleanArray)batch.Column(0);
        boolArray.GetValue(0).Should().BeTrue();
        boolArray.GetValue(1).Should().BeFalse();
    }

    #endregion

    #region Integer Type Tests

    [Fact]
    public void Build_Int8Column_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("int8_field", ColumnDataType.Int8)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow((sbyte)-128, DateTime.UtcNow)
            .AddRow((sbyte)127, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var int8Array = (Int8Array)batch.Column(0);
        int8Array.GetValue(0).Should().Be(-128);
        int8Array.GetValue(1).Should().Be(127);
    }

    [Fact]
    public void Build_Int64Column_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("int64_field", ColumnDataType.Int64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(long.MinValue, DateTime.UtcNow)
            .AddRow(long.MaxValue, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var int64Array = (Int64Array)batch.Column(0);
        int64Array.GetValue(0).Should().Be(long.MinValue);
        int64Array.GetValue(1).Should().Be(long.MaxValue);
    }

    [Fact]
    public void Build_UInt64Column_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("uint64_field", ColumnDataType.UInt64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(0UL, DateTime.UtcNow)
            .AddRow(ulong.MaxValue, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var uint64Array = (UInt64Array)batch.Column(0);
        uint64Array.GetValue(0).Should().Be(0UL);
        uint64Array.GetValue(1).Should().Be(ulong.MaxValue);
    }

    #endregion

    #region Float Type Tests

    [Fact]
    public void Build_Float32Column_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("float32_field", ColumnDataType.Float32)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(3.14f, DateTime.UtcNow)
            .AddRow(-2.71f, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var floatArray = (FloatArray)batch.Column(0);
        floatArray.GetValue(0).Should().BeApproximately(3.14f, 0.001f);
        floatArray.GetValue(1).Should().BeApproximately(-2.71f, 0.001f);
    }

    [Fact]
    public void Build_Float64Column_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddField("float64_field", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(3.14159265359, DateTime.UtcNow)
            .AddRow(-2.71828182846, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var doubleArray = (DoubleArray)batch.Column(0);
        doubleArray.GetValue(0).Should().BeApproximately(3.14159265359, 0.0000001);
        doubleArray.GetValue(1).Should().BeApproximately(-2.71828182846, 0.0000001);
    }

    #endregion

    #region String and Binary Type Tests

    [Fact]
    public void Build_StringColumn_CorrectValues()
    {
        var table = new TableBuilder("test_table")
            .AddTag("string_field", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("hello", DateTime.UtcNow)
            .AddRow("world", DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var stringArray = (StringArray)batch.Column(0);
        stringArray.GetString(0).Should().Be("hello");
        stringArray.GetString(1).Should().Be("world");
    }

    [Fact]
    public void Build_BinaryColumn_CorrectValues()
    {
        var bytes1 = new byte[] { 1, 2, 3 };
        var bytes2 = new byte[] { 4, 5, 6, 7, 8 };

        var table = new TableBuilder("test_table")
            .AddField("binary_field", ColumnDataType.Binary)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(bytes1, DateTime.UtcNow)
            .AddRow(bytes2, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var binaryArray = (BinaryArray)batch.Column(0);
        binaryArray.GetBytes(0).ToArray().Should().BeEquivalentTo(bytes1);
        binaryArray.GetBytes(1).ToArray().Should().BeEquivalentTo(bytes2);
    }

    #endregion

    #region Timestamp Type Tests

    [Fact]
    public void Build_TimestampMillisecond_CorrectConversion()
    {
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value", timestamp)
            .Build();

        using var batch = _builder.Build(table);

        var tsArray = (TimestampArray)batch.Column(1);
        var result = tsArray.GetTimestamp(0);
        result.Should().NotBeNull();
        result!.Value.UtcDateTime.Should().BeCloseTo(timestamp, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public void Build_TimestampSecond_CorrectConversion()
    {
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampSecond)
            .AddRow("value", timestamp)
            .Build();

        using var batch = _builder.Build(table);

        var tsArray = (TimestampArray)batch.Column(1);
        var result = tsArray.GetTimestamp(0);
        result.Should().NotBeNull();
        result!.Value.UtcDateTime.Should().BeCloseTo(timestamp, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Build_TimestampMicrosecond_CorrectConversion()
    {
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMicrosecond)
            .AddRow("value", timestamp)
            .Build();

        using var batch = _builder.Build(table);

        var tsArray = (TimestampArray)batch.Column(1);
        var result = tsArray.GetTimestamp(0);
        result.Should().NotBeNull();
        result!.Value.UtcDateTime.Should().BeCloseTo(timestamp, TimeSpan.FromMicroseconds(1));
    }

    [Fact]
    public void Build_DateTimeColumn_UsesMicrosecondUnit()
    {
        var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc).AddTicks(450); // +45us

        var table = new TableBuilder("test_table")
            .AddField("dt", ColumnDataType.DateTime)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(value, DateTime.UtcNow)
            .Build();

        using var batch = _builder.Build(table);

        var dtArray = (TimestampArray)batch.Column(0);
        dtArray.Data.DataType.Should().BeOfType<TimestampType>();
        ((TimestampType)dtArray.Data.DataType).Unit.Should().Be(TimeUnit.Microsecond);

        var result = dtArray.GetTimestamp(0);
        result.Should().NotBeNull();
        result!.Value.UtcDateTime.Should().BeCloseTo(value, TimeSpan.FromMicroseconds(1));
    }

    [Fact]
    public void Build_TimestampNanosecond_CorrectConversion()
    {
        var timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);

        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampNanosecond)
            .AddRow("value", timestamp)
            .Build();

        using var batch = _builder.Build(table);

        var tsArray = (TimestampArray)batch.Column(1);
        var result = tsArray.GetTimestamp(0);
        result.Should().NotBeNull();
        // Nanosecond precision is limited by DateTime ticks (100ns)
        result!.Value.UtcDateTime.Should().BeCloseTo(timestamp, TimeSpan.FromTicks(1));
    }

    #endregion

    #region Date Type Tests

    [Fact]
    public void Build_DateColumn_CorrectConversion()
    {
        var date = new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc);

        var table = new TableBuilder("test_table")
            .AddField("date_field", ColumnDataType.Date)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(date, DateTime.UtcNow)
            .Build();

        using var batch = _builder.Build(table);

        var dateArray = (Date32Array)batch.Column(0);
        var result = dateArray.GetDateTime(0);
        result.Should().NotBeNull();
        result!.Value.Date.Should().Be(date.Date);
    }

    #endregion

    #region Null Value Tests

    [Fact]
    public void Build_NullValues_CorrectlyHandled()
    {
        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddField("int_field", ColumnDataType.Int64)
            .AddField("float_field", ColumnDataType.Float64)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value1", 100L, 1.5, DateTime.UtcNow)
            .AddRow(null, null, null, DateTime.UtcNow.AddMilliseconds(1))
            .AddRow("value3", 300L, 3.5, DateTime.UtcNow.AddMilliseconds(2))
            .Build();

        using var batch = _builder.Build(table);

        var stringArray = (StringArray)batch.Column(0);
        var intArray = (Int64Array)batch.Column(1);
        var floatArray = (DoubleArray)batch.Column(2);

        // Row 0 - all values present
        stringArray.IsNull(0).Should().BeFalse();
        intArray.IsNull(0).Should().BeFalse();
        floatArray.IsNull(0).Should().BeFalse();

        // Row 1 - all nulls
        stringArray.IsNull(1).Should().BeTrue();
        intArray.IsNull(1).Should().BeTrue();
        floatArray.IsNull(1).Should().BeTrue();

        // Row 2 - all values present
        stringArray.IsNull(2).Should().BeFalse();
        intArray.IsNull(2).Should().BeFalse();
        floatArray.IsNull(2).Should().BeFalse();
    }

    [Fact]
    public void Build_NullBinaryValue_CorrectlyHandled()
    {
        var table = new TableBuilder("test_table")
            .AddField("binary_field", ColumnDataType.Binary)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(new byte[] { 1, 2, 3 }, DateTime.UtcNow)
            .AddRow(null, DateTime.UtcNow.AddMilliseconds(1))
            .Build();

        using var batch = _builder.Build(table);

        var binaryArray = (BinaryArray)batch.Column(0);
        binaryArray.IsNull(0).Should().BeFalse();
        binaryArray.IsNull(1).Should().BeTrue();
    }

    #endregion

    #region All Data Types Test

    [Fact]
    public void Build_AllDataTypes_CorrectlyBuilt()
    {
        var timestamp = DateTime.UtcNow;

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
                timestamp)
            .Build();

        using var batch = _builder.Build(table);

        batch.Length.Should().Be(1);
        batch.ColumnCount.Should().Be(15);

        // Verify types
        batch.Column(0).Should().BeOfType<StringArray>();
        batch.Column(1).Should().BeOfType<BooleanArray>();
        batch.Column(2).Should().BeOfType<Int8Array>();
        batch.Column(3).Should().BeOfType<Int16Array>();
        batch.Column(4).Should().BeOfType<Int32Array>();
        batch.Column(5).Should().BeOfType<Int64Array>();
        batch.Column(6).Should().BeOfType<UInt8Array>();
        batch.Column(7).Should().BeOfType<UInt16Array>();
        batch.Column(8).Should().BeOfType<UInt32Array>();
        batch.Column(9).Should().BeOfType<UInt64Array>();
        batch.Column(10).Should().BeOfType<FloatArray>();
        batch.Column(11).Should().BeOfType<DoubleArray>();
        batch.Column(12).Should().BeOfType<StringArray>();
        batch.Column(13).Should().BeOfType<BinaryArray>();
        batch.Column(14).Should().BeOfType<TimestampArray>();
    }

    #endregion

    #region Disposed Tests

    [Fact]
    public void Build_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = new RecordBatchBuilder();
        builder.Dispose();

        var table = new TableBuilder("test_table")
            .AddTag("tag", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("value", DateTime.UtcNow)
            .Build();

        var act = () => builder.Build(table);
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        var builder = new RecordBatchBuilder();
        var act = () =>
        {
            builder.Dispose();
            builder.Dispose();
        };
        act.Should().NotThrow();
    }

    #endregion
}
