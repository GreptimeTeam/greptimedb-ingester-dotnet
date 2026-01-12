using GreptimeDB.Ingester.Exceptions;

namespace GreptimeDB.Ingester.Types;

/// <summary>
/// Handles type coercion between .NET types and GreptimeDB column types.
/// </summary>
internal static class TypeCoercion
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Coerce a .NET value to the target column type.
    /// </summary>
    /// <param name="value">The value to coerce.</param>
    /// <param name="targetType">The target column data type.</param>
    /// <param name="columnName">The column name (for error messages).</param>
    /// <returns>The coerced value.</returns>
    /// <exception cref="TypeMismatchException">Thrown when coercion fails.</exception>
    public static object? Coerce(object? value, ColumnDataType targetType, string columnName)
    {
        if (value is null)
        {
            return null;
        }

        return targetType switch
        {
            ColumnDataType.Boolean => CoerceToBoolean(value, columnName),

            ColumnDataType.Int8 => CoerceToInt8(value, columnName),
            ColumnDataType.Int16 => CoerceToInt16(value, columnName),
            ColumnDataType.Int32 => CoerceToInt32(value, columnName),
            ColumnDataType.Int64 => CoerceToInt64(value, columnName),

            ColumnDataType.UInt8 => CoerceToUInt8(value, columnName),
            ColumnDataType.UInt16 => CoerceToUInt16(value, columnName),
            ColumnDataType.UInt32 => CoerceToUInt32(value, columnName),
            ColumnDataType.UInt64 => CoerceToUInt64(value, columnName),

            ColumnDataType.Float32 => CoerceToFloat32(value, columnName),
            ColumnDataType.Float64 => CoerceToFloat64(value, columnName),

            ColumnDataType.String or ColumnDataType.Json => CoerceToString(value, columnName),
            ColumnDataType.Binary => CoerceToBinary(value, columnName),

            ColumnDataType.Date => CoerceToDate(value, columnName),
            ColumnDataType.DateTime => CoerceToTimestamp(value, ColumnDataType.TimestampMicrosecond, columnName),

            ColumnDataType.TimestampSecond or
            ColumnDataType.TimestampMillisecond or
            ColumnDataType.TimestampMicrosecond or
            ColumnDataType.TimestampNanosecond => CoerceToTimestamp(value, targetType, columnName),

            ColumnDataType.TimeSecond or
            ColumnDataType.TimeMillisecond or
            ColumnDataType.TimeMicrosecond or
            ColumnDataType.TimeNanosecond => CoerceToTime(value, targetType, columnName),

            _ => throw new TypeMismatchException(columnName, value.GetType(), targetType, "Unsupported target type."),
        };
    }

    #region Boolean

    private static bool CoerceToBoolean(object value, string columnName)
    {
        return value switch
        {
            bool b => b,
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Boolean),
        };
    }

    #endregion

    #region Signed Integers

    private static sbyte CoerceToInt8(object value, string columnName)
    {
        try
        {
            return value switch
            {
                sbyte v => v,
                byte v when v <= sbyte.MaxValue => (sbyte)v,
                short v when v is >= sbyte.MinValue and <= sbyte.MaxValue => (sbyte)v,
                ushort v when v <= (ushort)sbyte.MaxValue => (sbyte)v,
                int v when v is >= sbyte.MinValue and <= sbyte.MaxValue => (sbyte)v,
                uint v when v <= (uint)sbyte.MaxValue => (sbyte)v,
                long v when v is >= sbyte.MinValue and <= sbyte.MaxValue => (sbyte)v,
                ulong v when v <= (ulong)sbyte.MaxValue => (sbyte)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int8,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int8, "Value out of range.");
        }
    }

    private static short CoerceToInt16(object value, string columnName)
    {
        try
        {
            return value switch
            {
                sbyte v => v,
                byte v => v,
                short v => v,
                ushort v when v <= (ushort)short.MaxValue => (short)v,
                int v when v is >= short.MinValue and <= short.MaxValue => (short)v,
                uint v when v <= (uint)short.MaxValue => (short)v,
                long v when v is >= short.MinValue and <= short.MaxValue => (short)v,
                ulong v when v <= (ulong)short.MaxValue => (short)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int16,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int16, "Value out of range.");
        }
    }

    private static int CoerceToInt32(object value, string columnName)
    {
        try
        {
            return value switch
            {
                sbyte v => v,
                byte v => v,
                short v => v,
                ushort v => v,
                int v => v,
                uint v when v <= int.MaxValue => (int)v,
                long v when v is >= int.MinValue and <= int.MaxValue => (int)v,
                ulong v when v <= int.MaxValue => (int)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int32,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int32, "Value out of range.");
        }
    }

    private static long CoerceToInt64(object value, string columnName)
    {
        try
        {
            return value switch
            {
                sbyte v => v,
                byte v => v,
                short v => v,
                ushort v => v,
                int v => v,
                uint v => v,
                long v => v,
                ulong v when v <= long.MaxValue => (long)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int64,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Int64, "Value out of range.");
        }
    }

    #endregion

    #region Unsigned Integers

    private static byte CoerceToUInt8(object value, string columnName)
    {
        try
        {
            return value switch
            {
                byte v => v,
                sbyte v when v >= 0 => (byte)v,
                short v when v is >= 0 and <= byte.MaxValue => (byte)v,
                ushort v when v <= byte.MaxValue => (byte)v,
                int v when v is >= 0 and <= byte.MaxValue => (byte)v,
                uint v when v <= byte.MaxValue => (byte)v,
                long v when v is >= 0 and <= byte.MaxValue => (byte)v,
                ulong v when v <= byte.MaxValue => (byte)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt8,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt8, "Value out of range.");
        }
    }

    private static ushort CoerceToUInt16(object value, string columnName)
    {
        try
        {
            return value switch
            {
                byte v => v,
                ushort v => v,
                sbyte v when v >= 0 => (ushort)v,
                short v when v >= 0 => (ushort)v,
                int v when v is >= 0 and <= ushort.MaxValue => (ushort)v,
                uint v when v <= ushort.MaxValue => (ushort)v,
                long v when v is >= 0 and <= ushort.MaxValue => (ushort)v,
                ulong v when v <= ushort.MaxValue => (ushort)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt16,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt16, "Value out of range.");
        }
    }

    private static uint CoerceToUInt32(object value, string columnName)
    {
        try
        {
            return value switch
            {
                byte v => v,
                ushort v => v,
                uint v => v,
                sbyte v when v >= 0 => (uint)v,
                short v when v >= 0 => (uint)v,
                int v when v >= 0 => (uint)v,
                long v when v is >= 0 and <= uint.MaxValue => (uint)v,
                ulong v when v <= uint.MaxValue => (uint)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt32,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt32, "Value out of range.");
        }
    }

    private static ulong CoerceToUInt64(object value, string columnName)
    {
        try
        {
            return value switch
            {
                byte v => v,
                ushort v => v,
                uint v => v,
                ulong v => v,
                sbyte v when v >= 0 => (ulong)v,
                short v when v >= 0 => (ulong)v,
                int v when v >= 0 => (ulong)v,
                long v when v >= 0 => (ulong)v,
                _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt64,
                    "Value out of range or incompatible type."),
            };
        }
        catch (OverflowException)
        {
            throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.UInt64, "Value out of range.");
        }
    }

    #endregion

    #region Floating Point (NO integer cross-conversion per Go SDK behavior)

    private static float CoerceToFloat32(object value, string columnName)
    {
        return value switch
        {
            float v => v,
            double v when v is >= float.MinValue and <= float.MaxValue => (float)v,
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Float32,
                "Floats cannot be coerced from integer types."),
        };
    }

    private static double CoerceToFloat64(object value, string columnName)
    {
        return value switch
        {
            float v => v,
            double v => v,
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Float64,
                "Floats cannot be coerced from integer types."),
        };
    }

    #endregion

    #region String and Binary

    private static string CoerceToString(object value, string columnName)
    {
        return value switch
        {
            string s => s,
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.String),
        };
    }

    private static byte[] CoerceToBinary(object value, string columnName)
    {
        return value switch
        {
            byte[] b => b,
            ReadOnlyMemory<byte> m => m.ToArray(),
            Memory<byte> m => m.ToArray(),
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Binary),
        };
    }

    #endregion

    #region Date and Timestamp

    private static int CoerceToDate(object value, string columnName)
    {
        return value switch
        {
            DateTime dt => (int)((dt.ToUniversalTime() - UnixEpoch).TotalDays),
            DateTimeOffset dto => (int)((dto.UtcDateTime - UnixEpoch).TotalDays),
            DateOnly d => d.DayNumber - (int)(UnixEpoch.Ticks / TimeSpan.TicksPerDay),
            int days => days,
            long days when days is >= int.MinValue and <= int.MaxValue => (int)days,
            _ => throw new TypeMismatchException(columnName, value.GetType(), ColumnDataType.Date),
        };
    }

    private static long CoerceToTimestamp(object value, ColumnDataType targetType, string columnName)
    {
        if (value is DateTime dt)
        {
            var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
            var ticks = utc.Ticks - UnixEpoch.Ticks;
            return ConvertTicksToTimestamp(ticks, targetType);
        }

        if (value is DateTimeOffset dto)
        {
            var ticks = dto.UtcTicks - UnixEpoch.Ticks;
            return ConvertTicksToTimestamp(ticks, targetType);
        }

        // Accept raw long as-is (assumed to be in target precision)
        if (value is long l)
        {
            return l;
        }

        if (value is int i)
        {
            return i;
        }

        throw new TypeMismatchException(columnName, value.GetType(), targetType);
    }

    private static long ConvertTicksToTimestamp(long ticks, ColumnDataType targetType)
    {
        return targetType switch
        {
            ColumnDataType.TimestampSecond => ticks / TimeSpan.TicksPerSecond,
            ColumnDataType.TimestampMillisecond or ColumnDataType.DateTime => ticks / TimeSpan.TicksPerMillisecond,
            ColumnDataType.TimestampMicrosecond => ticks / (TimeSpan.TicksPerMillisecond / 1000),
            ColumnDataType.TimestampNanosecond => ticks * 100, // 1 tick = 100 nanoseconds
            _ => throw new ArgumentOutOfRangeException(nameof(targetType), targetType, "Invalid timestamp type."),
        };
    }

    private static long CoerceToTime(object value, ColumnDataType targetType, string columnName)
    {
        if (value is TimeSpan ts)
        {
            return targetType switch
            {
                ColumnDataType.TimeSecond => (long)ts.TotalSeconds,
                ColumnDataType.TimeMillisecond => (long)ts.TotalMilliseconds,
                ColumnDataType.TimeMicrosecond => ts.Ticks / (TimeSpan.TicksPerMillisecond / 1000),
                ColumnDataType.TimeNanosecond => ts.Ticks * 100,
                _ => throw new ArgumentOutOfRangeException(nameof(targetType), targetType, "Invalid time type."),
            };
        }

        if (value is TimeOnly to)
        {
            var ticks = to.Ticks;
            return targetType switch
            {
                ColumnDataType.TimeSecond => ticks / TimeSpan.TicksPerSecond,
                ColumnDataType.TimeMillisecond => ticks / TimeSpan.TicksPerMillisecond,
                ColumnDataType.TimeMicrosecond => ticks / (TimeSpan.TicksPerMillisecond / 1000),
                ColumnDataType.TimeNanosecond => ticks * 100,
                _ => throw new ArgumentOutOfRangeException(nameof(targetType), targetType, "Invalid time type."),
            };
        }

        // Accept raw long as-is
        if (value is long l)
        {
            return l;
        }

        if (value is int i)
        {
            return i;
        }

        throw new TypeMismatchException(columnName, value.GetType(), targetType);
    }

    #endregion
}
