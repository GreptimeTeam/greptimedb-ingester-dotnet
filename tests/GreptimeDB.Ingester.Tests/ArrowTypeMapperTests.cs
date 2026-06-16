using Apache.Arrow.Types;
using FluentAssertions;
using GreptimeDB.Ingester.Arrow;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class ArrowTypeMapperTests
{
    #region ToArrowType Tests

    [Fact]
    public void ToArrowType_Boolean_ReturnsBooleanType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Boolean);
        Assert.Same(BooleanType.Default, result);
    }

    [Theory]
    [InlineData(ColumnDataType.Int8, typeof(Int8Type))]
    [InlineData(ColumnDataType.Int16, typeof(Int16Type))]
    [InlineData(ColumnDataType.Int32, typeof(Int32Type))]
    [InlineData(ColumnDataType.Int64, typeof(Int64Type))]
    public void ToArrowType_SignedIntegers_ReturnsCorrectType(ColumnDataType dataType, Type expectedType)
    {
        var result = ArrowTypeMapper.ToArrowType(dataType);
        Assert.IsType(expectedType, result);
    }

    [Theory]
    [InlineData(ColumnDataType.UInt8, typeof(UInt8Type))]
    [InlineData(ColumnDataType.UInt16, typeof(UInt16Type))]
    [InlineData(ColumnDataType.UInt32, typeof(UInt32Type))]
    [InlineData(ColumnDataType.UInt64, typeof(UInt64Type))]
    public void ToArrowType_UnsignedIntegers_ReturnsCorrectType(ColumnDataType dataType, Type expectedType)
    {
        var result = ArrowTypeMapper.ToArrowType(dataType);
        Assert.IsType(expectedType, result);
    }

    [Fact]
    public void ToArrowType_Float32_ReturnsFloatType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Float32);
        Assert.Same(FloatType.Default, result);
    }

    [Fact]
    public void ToArrowType_Float64_ReturnsDoubleType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Float64);
        Assert.Same(DoubleType.Default, result);
    }

    [Fact]
    public void ToArrowType_String_ReturnsStringType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.String);
        Assert.Same(StringType.Default, result);
    }

    [Fact]
    public void ToArrowType_Json_ReturnsStringType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Json);
        Assert.Same(StringType.Default, result);
    }

    [Fact]
    public void ToArrowType_Binary_ReturnsBinaryType()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Binary);
        Assert.Same(BinaryType.Default, result);
    }

    [Fact]
    public void ToArrowType_Date_ReturnsDate32Type()
    {
        var result = ArrowTypeMapper.ToArrowType(ColumnDataType.Date);
        Assert.Same(Date32Type.Default, result);
    }

    [Theory]
    [InlineData(ColumnDataType.TimestampSecond, TimeUnit.Second)]
    [InlineData(ColumnDataType.TimestampMillisecond, TimeUnit.Millisecond)]
    [InlineData(ColumnDataType.TimestampMicrosecond, TimeUnit.Microsecond)]
    [InlineData(ColumnDataType.TimestampNanosecond, TimeUnit.Nanosecond)]
    public void ToArrowType_Timestamps_ReturnsCorrectTimestampType(ColumnDataType dataType, TimeUnit expectedUnit)
    {
        var result = ArrowTypeMapper.ToArrowType(dataType);
        var timestampType = Assert.IsType<TimestampType>(result);
        timestampType.Unit.Should().Be(expectedUnit);
    }

    [Theory]
    [InlineData(ColumnDataType.TimeSecond, TimeUnit.Second)]
    [InlineData(ColumnDataType.TimeMillisecond, TimeUnit.Millisecond)]
    public void ToArrowType_Time32Types_ReturnsCorrectTime32Type(ColumnDataType dataType, TimeUnit expectedUnit)
    {
        var result = ArrowTypeMapper.ToArrowType(dataType);
        var timeType = Assert.IsType<Time32Type>(result);
        timeType.Unit.Should().Be(expectedUnit);
    }

    [Theory]
    [InlineData(ColumnDataType.TimeMicrosecond, TimeUnit.Microsecond)]
    [InlineData(ColumnDataType.TimeNanosecond, TimeUnit.Nanosecond)]
    public void ToArrowType_Time64Types_ReturnsCorrectTime64Type(ColumnDataType dataType, TimeUnit expectedUnit)
    {
        var result = ArrowTypeMapper.ToArrowType(dataType);
        var timeType = Assert.IsType<Time64Type>(result);
        timeType.Unit.Should().Be(expectedUnit);
    }

    [Fact]
    public void ToArrowType_UnsupportedType_ThrowsNotSupportedException()
    {
        var unsupportedType = (ColumnDataType)999;
        var act = () => ArrowTypeMapper.ToArrowType(unsupportedType);
        act.Should().Throw<NotSupportedException>()
            .WithMessage("*999*");
    }

    #endregion

    #region GetTimestampUnit Tests

    [Theory]
    [InlineData(ColumnDataType.TimestampSecond, TimeUnit.Second)]
    [InlineData(ColumnDataType.TimestampMillisecond, TimeUnit.Millisecond)]
    [InlineData(ColumnDataType.TimestampMicrosecond, TimeUnit.Microsecond)]
    [InlineData(ColumnDataType.TimestampNanosecond, TimeUnit.Nanosecond)]
    public void GetTimestampUnit_ValidTimestampTypes_ReturnsCorrectUnit(ColumnDataType dataType, TimeUnit expectedUnit)
    {
        var result = ArrowTypeMapper.GetTimestampUnit(dataType);
        result.Should().Be(expectedUnit);
    }

    [Theory]
    [InlineData(ColumnDataType.Int64)]
    [InlineData(ColumnDataType.String)]
    [InlineData(ColumnDataType.Date)]
    public void GetTimestampUnit_NonTimestampType_ThrowsArgumentException(ColumnDataType dataType)
    {
        var act = () => ArrowTypeMapper.GetTimestampUnit(dataType);
        act.Should().Throw<ArgumentException>()
            .WithMessage($"*{dataType}*is not a timestamp type*");
    }

    #endregion

    #region IsTimestampType Tests

    [Theory]
    [InlineData(ColumnDataType.TimestampSecond, true)]
    [InlineData(ColumnDataType.TimestampMillisecond, true)]
    [InlineData(ColumnDataType.TimestampMicrosecond, true)]
    [InlineData(ColumnDataType.TimestampNanosecond, true)]
    [InlineData(ColumnDataType.Int64, false)]
    [InlineData(ColumnDataType.Date, false)]
    [InlineData(ColumnDataType.TimeSecond, false)]
    public void IsTimestampType_ReturnsCorrectResult(ColumnDataType dataType, bool expected)
    {
        var result = ArrowTypeMapper.IsTimestampType(dataType);
        result.Should().Be(expected);
    }

    #endregion

    #region IsTimeType Tests

    [Theory]
    [InlineData(ColumnDataType.TimeSecond, true)]
    [InlineData(ColumnDataType.TimeMillisecond, true)]
    [InlineData(ColumnDataType.TimeMicrosecond, true)]
    [InlineData(ColumnDataType.TimeNanosecond, true)]
    [InlineData(ColumnDataType.TimestampSecond, false)]
    [InlineData(ColumnDataType.Date, false)]
    [InlineData(ColumnDataType.Int64, false)]
    public void IsTimeType_ReturnsCorrectResult(ColumnDataType dataType, bool expected)
    {
        var result = ArrowTypeMapper.IsTimeType(dataType);
        result.Should().Be(expected);
    }

    #endregion

    #region All Types Coverage

    [Fact]
    public void ToArrowType_AllSupportedTypes_DoNotThrow()
    {
        var supportedTypes = new[]
        {
            ColumnDataType.Boolean,
            ColumnDataType.Int8, ColumnDataType.Int16, ColumnDataType.Int32, ColumnDataType.Int64,
            ColumnDataType.UInt8, ColumnDataType.UInt16, ColumnDataType.UInt32, ColumnDataType.UInt64,
            ColumnDataType.Float32, ColumnDataType.Float64,
            ColumnDataType.String, ColumnDataType.Binary, ColumnDataType.Json,
            ColumnDataType.Date,
            ColumnDataType.TimestampSecond, ColumnDataType.TimestampMillisecond,
            ColumnDataType.TimestampMicrosecond, ColumnDataType.TimestampNanosecond,
            ColumnDataType.TimeSecond, ColumnDataType.TimeMillisecond,
            ColumnDataType.TimeMicrosecond, ColumnDataType.TimeNanosecond
        };

        foreach (var dataType in supportedTypes)
        {
            var act = () => ArrowTypeMapper.ToArrowType(dataType);
            act.Should().NotThrow($"Type {dataType} should be supported");
        }
    }

    #endregion
}
