using FluentAssertions;
using GreptimeDB.Ingester.Exceptions;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class TypeCoercionTests
{
    private const string TestColumn = "test_column";

    #region Boolean Tests

    [Fact]
    public void Coerce_Boolean_FromBool_Succeeds()
    {
        var result = TypeCoercion.Coerce(true, ColumnDataType.Boolean, TestColumn);
        result.Should().Be(true);
    }

    [Fact]
    public void Coerce_Boolean_FromInt_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(1, ColumnDataType.Boolean, TestColumn);
        act.Should().Throw<TypeMismatchException>();
    }

    #endregion

    #region Integer Cross-Conversion Tests

    [Theory]
    [InlineData((sbyte)1)]
    [InlineData((byte)1)]
    [InlineData((short)1)]
    [InlineData((int)1)]
    [InlineData((long)1)]
    public void Coerce_Int8_FromVariousIntegers_Succeeds(object input)
    {
        var result = TypeCoercion.Coerce(input, ColumnDataType.Int8, TestColumn);
        result.Should().Be((sbyte)1);
    }

    [Fact]
    public void Coerce_Int8_FromOutOfRangeValue_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(1000, ColumnDataType.Int8, TestColumn);
        act.Should().Throw<TypeMismatchException>()
            .Which.Message.Should().Contain("out of range");
    }

    [Theory]
    [InlineData((sbyte)42)]
    [InlineData((byte)42)]
    [InlineData((short)42)]
    [InlineData((ushort)42)]
    [InlineData((int)42)]
    [InlineData((uint)42)]
    [InlineData((long)42)]
    [InlineData((ulong)42)]
    public void Coerce_Int64_FromAllIntegerTypes_Succeeds(object input)
    {
        var result = TypeCoercion.Coerce(input, ColumnDataType.Int64, TestColumn);
        result.Should().Be(42L);
    }

    [Theory]
    [InlineData((byte)200)]
    [InlineData((ushort)50000)]
    [InlineData((uint)3000000000)]
    public void Coerce_UInt64_FromUnsignedIntegers_Succeeds(object input)
    {
        var result = TypeCoercion.Coerce(input, ColumnDataType.UInt64, TestColumn);
        result.Should().BeOfType<ulong>();
    }

    [Fact]
    public void Coerce_UInt64_FromNegativeValue_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(-1, ColumnDataType.UInt64, TestColumn);
        act.Should().Throw<TypeMismatchException>();
    }

    #endregion

    #region Float Tests (NO integer cross-conversion)

    [Fact]
    public void Coerce_Float64_FromFloat_Succeeds()
    {
        var result = TypeCoercion.Coerce(3.14f, ColumnDataType.Float64, TestColumn);
        ((double)result!).Should().BeApproximately(3.14, 0.01);
    }

    [Fact]
    public void Coerce_Float64_FromDouble_Succeeds()
    {
        var result = TypeCoercion.Coerce(3.14159, ColumnDataType.Float64, TestColumn);
        result.Should().Be(3.14159);
    }

    [Fact]
    public void Coerce_Float64_FromInteger_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(42, ColumnDataType.Float64, TestColumn);
        act.Should().Throw<TypeMismatchException>()
            .Which.Message.Should().Contain("integer");
    }

    [Fact]
    public void Coerce_Float32_FromInteger_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(42L, ColumnDataType.Float32, TestColumn);
        act.Should().Throw<TypeMismatchException>();
    }

    #endregion

    #region String and Binary Tests

    [Fact]
    public void Coerce_String_FromString_Succeeds()
    {
        var result = TypeCoercion.Coerce("hello", ColumnDataType.String, TestColumn);
        result.Should().Be("hello");
    }

    [Fact]
    public void Coerce_String_FromInt_ThrowsTypeMismatch()
    {
        var act = () => TypeCoercion.Coerce(123, ColumnDataType.String, TestColumn);
        act.Should().Throw<TypeMismatchException>();
    }

    [Fact]
    public void Coerce_Binary_FromByteArray_Succeeds()
    {
        var input = new byte[] { 1, 2, 3 };
        var result = TypeCoercion.Coerce(input, ColumnDataType.Binary, TestColumn);
        result.Should().BeEquivalentTo(input);
    }

    #endregion

    #region Timestamp Tests

    [Fact]
    public void Coerce_TimestampMillisecond_FromDateTime_Succeeds()
    {
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var result = TypeCoercion.Coerce(dt, ColumnDataType.TimestampMillisecond, TestColumn);

        var expected = new DateTimeOffset(dt).ToUnixTimeMilliseconds();
        result.Should().Be(expected);
    }

    [Fact]
    public void Coerce_TimestampSecond_FromDateTimeOffset_Succeeds()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 12, 0, 0, TimeSpan.Zero);
        var result = TypeCoercion.Coerce(dto, ColumnDataType.TimestampSecond, TestColumn);

        result.Should().Be(dto.ToUnixTimeSeconds());
    }

    [Fact]
    public void Coerce_DateTime_FromDateTime_UsesMicrosecondPrecision()
    {
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc).AddTicks(1230); // +123us
        var result = TypeCoercion.Coerce(dt, ColumnDataType.DateTime, TestColumn);

        var expected = (dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).Ticks / 10;
        result.Should().Be(expected);
    }

    [Fact]
    public void Coerce_TimestampMillisecond_FromLong_PassesThrough()
    {
        var timestamp = 1705320000000L;
        var result = TypeCoercion.Coerce(timestamp, ColumnDataType.TimestampMillisecond, TestColumn);
        result.Should().Be(timestamp);
    }

    #endregion

    #region Null Handling

    [Fact]
    public void Coerce_Null_ReturnsNull()
    {
        var result = TypeCoercion.Coerce(null, ColumnDataType.String, TestColumn);
        result.Should().BeNull();
    }

    [Fact]
    public void Coerce_Null_ForAnyType_ReturnsNull()
    {
        TypeCoercion.Coerce(null, ColumnDataType.Int64, TestColumn).Should().BeNull();
        TypeCoercion.Coerce(null, ColumnDataType.Float64, TestColumn).Should().BeNull();
        TypeCoercion.Coerce(null, ColumnDataType.Boolean, TestColumn).Should().BeNull();
        TypeCoercion.Coerce(null, ColumnDataType.TimestampMillisecond, TestColumn).Should().BeNull();
    }

    #endregion

    #region Date Tests

    [Fact]
    public void Coerce_Date_FromDateTime_ReturnsCorrectDays()
    {
        var dt = new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        var result = TypeCoercion.Coerce(dt, ColumnDataType.Date, TestColumn);

        // Days since Unix epoch (1970-01-01)
        var expected = (int)(dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalDays;
        result.Should().Be(expected);
    }

    #endregion
}
