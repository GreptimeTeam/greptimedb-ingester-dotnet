using Apache.Arrow;
using Apache.Arrow.Types;
using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Arrow;

/// <summary>
/// Maps GreptimeDB column data types to Apache Arrow types.
/// </summary>
internal static class ArrowTypeMapper
{
    /// <summary>
    /// Converts a GreptimeDB column data type to an Apache Arrow data type.
    /// </summary>
    /// <param name="dataType">The GreptimeDB column data type.</param>
    /// <returns>The corresponding Apache Arrow data type.</returns>
    /// <exception cref="NotSupportedException">Thrown when the data type is not supported.</exception>
    public static IArrowType ToArrowType(ColumnDataType dataType)
    {
        return dataType switch
        {
            // Boolean
            ColumnDataType.Boolean => BooleanType.Default,

            // Signed integers
            ColumnDataType.Int8 => Int8Type.Default,
            ColumnDataType.Int16 => Int16Type.Default,
            ColumnDataType.Int32 => Int32Type.Default,
            ColumnDataType.Int64 => Int64Type.Default,

            // Unsigned integers
            ColumnDataType.UInt8 => UInt8Type.Default,
            ColumnDataType.UInt16 => UInt16Type.Default,
            ColumnDataType.UInt32 => UInt32Type.Default,
            ColumnDataType.UInt64 => UInt64Type.Default,

            // Floating point
            ColumnDataType.Float32 => FloatType.Default,
            ColumnDataType.Float64 => DoubleType.Default,

            // String and Binary
            ColumnDataType.String => StringType.Default,
            ColumnDataType.Binary => BinaryType.Default,
            ColumnDataType.Json => StringType.Default, // JSON stored as string

            // Date and DateTime
            ColumnDataType.Date => Date32Type.Default,
            ColumnDataType.DateTime => new TimestampType(TimeUnit.Millisecond, (string?)null),

            // Timestamps with different precisions
            ColumnDataType.TimestampSecond => new TimestampType(TimeUnit.Second, (string?)null),
            ColumnDataType.TimestampMillisecond => new TimestampType(TimeUnit.Millisecond, (string?)null),
            ColumnDataType.TimestampMicrosecond => new TimestampType(TimeUnit.Microsecond, (string?)null),
            ColumnDataType.TimestampNanosecond => new TimestampType(TimeUnit.Nanosecond, (string?)null),

            // Time with different precisions
            ColumnDataType.TimeSecond => new Time32Type(TimeUnit.Second),
            ColumnDataType.TimeMillisecond => new Time32Type(TimeUnit.Millisecond),
            ColumnDataType.TimeMicrosecond => new Time64Type(TimeUnit.Microsecond),
            ColumnDataType.TimeNanosecond => new Time64Type(TimeUnit.Nanosecond),

            _ => throw new NotSupportedException($"Unsupported data type: {dataType}")
        };
    }

    /// <summary>
    /// Gets the TimeUnit for a timestamp column data type.
    /// </summary>
    /// <param name="dataType">The GreptimeDB column data type.</param>
    /// <returns>The corresponding TimeUnit.</returns>
    /// <exception cref="ArgumentException">Thrown when the data type is not a timestamp type.</exception>
    public static TimeUnit GetTimestampUnit(ColumnDataType dataType)
    {
        return dataType switch
        {
            ColumnDataType.TimestampSecond => TimeUnit.Second,
            ColumnDataType.TimestampMillisecond => TimeUnit.Millisecond,
            ColumnDataType.TimestampMicrosecond => TimeUnit.Microsecond,
            ColumnDataType.TimestampNanosecond => TimeUnit.Nanosecond,
            ColumnDataType.DateTime => TimeUnit.Millisecond,
            _ => throw new ArgumentException($"Data type {dataType} is not a timestamp type", nameof(dataType))
        };
    }

    /// <summary>
    /// Checks if the data type is a timestamp type.
    /// </summary>
    public static bool IsTimestampType(ColumnDataType dataType)
    {
        return dataType is ColumnDataType.TimestampSecond
            or ColumnDataType.TimestampMillisecond
            or ColumnDataType.TimestampMicrosecond
            or ColumnDataType.TimestampNanosecond
            or ColumnDataType.DateTime;
    }

    /// <summary>
    /// Checks if the data type is a time type.
    /// </summary>
    public static bool IsTimeType(ColumnDataType dataType)
    {
        return dataType is ColumnDataType.TimeSecond
            or ColumnDataType.TimeMillisecond
            or ColumnDataType.TimeMicrosecond
            or ColumnDataType.TimeNanosecond;
    }
}
