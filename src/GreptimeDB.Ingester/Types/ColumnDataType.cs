namespace GreptimeDB.Ingester.Types;

/// <summary>
/// GreptimeDB column data types.
/// Values are aligned with greptime-proto ColumnDataType enum.
/// </summary>
public enum ColumnDataType
{
    /// <summary>Boolean type.</summary>
    Boolean = 0,

    /// <summary>Signed 8-bit integer.</summary>
    Int8 = 1,

    /// <summary>Signed 16-bit integer.</summary>
    Int16 = 2,

    /// <summary>Signed 32-bit integer.</summary>
    Int32 = 3,

    /// <summary>Signed 64-bit integer.</summary>
    Int64 = 4,

    /// <summary>Unsigned 8-bit integer.</summary>
    UInt8 = 5,

    /// <summary>Unsigned 16-bit integer.</summary>
    UInt16 = 6,

    /// <summary>Unsigned 32-bit integer.</summary>
    UInt32 = 7,

    /// <summary>Unsigned 64-bit integer.</summary>
    UInt64 = 8,

    /// <summary>32-bit floating point.</summary>
    Float32 = 9,

    /// <summary>64-bit floating point.</summary>
    Float64 = 10,

    /// <summary>Binary data.</summary>
    Binary = 11,

    /// <summary>UTF-8 string.</summary>
    String = 12,

    /// <summary>Date (days since Unix epoch).</summary>
    Date = 13,

    /// <summary>DateTime (alias for TimestampMicrosecond).</summary>
    DateTime = 14,

    /// <summary>Timestamp with second precision.</summary>
    TimestampSecond = 15,

    /// <summary>Timestamp with millisecond precision.</summary>
    TimestampMillisecond = 16,

    /// <summary>Timestamp with microsecond precision.</summary>
    TimestampMicrosecond = 17,

    /// <summary>Timestamp with nanosecond precision.</summary>
    TimestampNanosecond = 18,

    /// <summary>Time with second precision.</summary>
    TimeSecond = 19,

    /// <summary>Time with millisecond precision.</summary>
    TimeMillisecond = 20,

    /// <summary>Time with microsecond precision.</summary>
    TimeMicrosecond = 21,

    /// <summary>Time with nanosecond precision.</summary>
    TimeNanosecond = 22,

    /// <summary>JSON data stored as string.</summary>
    Json = 27,
}

/// <summary>
/// Extension methods for <see cref="ColumnDataType"/>.
/// </summary>
public static class ColumnDataTypeExtensions
{
    /// <summary>
    /// Determines whether the column type is a timestamp type.
    /// </summary>
    public static bool IsTimestamp(this ColumnDataType type) => type switch
    {
        ColumnDataType.TimestampSecond => true,
        ColumnDataType.TimestampMillisecond => true,
        ColumnDataType.TimestampMicrosecond => true,
        ColumnDataType.TimestampNanosecond => true,
        ColumnDataType.DateTime => true,
        _ => false,
    };

    /// <summary>
    /// Determines whether the column type is an integer type.
    /// </summary>
    public static bool IsInteger(this ColumnDataType type) => type switch
    {
        ColumnDataType.Int8 => true,
        ColumnDataType.Int16 => true,
        ColumnDataType.Int32 => true,
        ColumnDataType.Int64 => true,
        ColumnDataType.UInt8 => true,
        ColumnDataType.UInt16 => true,
        ColumnDataType.UInt32 => true,
        ColumnDataType.UInt64 => true,
        _ => false,
    };

    /// <summary>
    /// Determines whether the column type is a floating point type.
    /// </summary>
    public static bool IsFloat(this ColumnDataType type) => type switch
    {
        ColumnDataType.Float32 => true,
        ColumnDataType.Float64 => true,
        _ => false,
    };
}
