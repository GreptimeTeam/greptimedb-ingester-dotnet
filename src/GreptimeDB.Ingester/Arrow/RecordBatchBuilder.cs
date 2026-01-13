using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Arrow;

/// <summary>
/// Builds Apache Arrow RecordBatch from GreptimeDB Table.
/// </summary>
internal sealed class RecordBatchBuilder : IDisposable
{
    private static readonly DateTimeOffset UnixEpoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTime UnixEpochDateTime = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly MemoryAllocator _allocator;
    private bool _disposed;

    /// <summary>
    /// Creates a new RecordBatchBuilder with the default native memory allocator.
    /// </summary>
    public RecordBatchBuilder()
    {
        _allocator = new NativeMemoryAllocator();
    }

    /// <summary>
    /// Builds an Apache Arrow RecordBatch from a GreptimeDB Table.
    /// </summary>
    /// <param name="table">The table to convert.</param>
    /// <returns>A RecordBatch containing the table data. Caller is responsible for disposing.</returns>
    public RecordBatch Build(Table.Table table)
    {
        ThrowIfDisposed();

        // Build schema
        var fields = new List<Field>(table.ColumnCount);
        foreach (var column in table.Columns)
        {
            var arrowType = ArrowTypeMapper.ToArrowType(column.DataType);
            fields.Add(new Field(column.Name, arrowType, nullable: true));
        }
        var schema = new Schema(fields, null);

        // Build arrays for each column
        var arrays = new IArrowArray[table.ColumnCount];
        for (var colIndex = 0; colIndex < table.ColumnCount; colIndex++)
        {
            var column = table.Columns[colIndex];
            arrays[colIndex] = BuildArray(table, colIndex, column.DataType);
        }

        return new RecordBatch(schema, arrays, table.RowCount);
    }

    private static IArrowArray BuildArray(Table.Table table, int columnIndex, ColumnDataType dataType)
    {
        return dataType switch
        {
            // Boolean
            ColumnDataType.Boolean => BuildBooleanArray(table, columnIndex),

            // Signed integers
            ColumnDataType.Int8 => BuildInt8Array(table, columnIndex),
            ColumnDataType.Int16 => BuildInt16Array(table, columnIndex),
            ColumnDataType.Int32 => BuildInt32Array(table, columnIndex),
            ColumnDataType.Int64 => BuildInt64Array(table, columnIndex),

            // Unsigned integers
            ColumnDataType.UInt8 => BuildUInt8Array(table, columnIndex),
            ColumnDataType.UInt16 => BuildUInt16Array(table, columnIndex),
            ColumnDataType.UInt32 => BuildUInt32Array(table, columnIndex),
            ColumnDataType.UInt64 => BuildUInt64Array(table, columnIndex),

            // Floating point
            ColumnDataType.Float32 => BuildFloatArray(table, columnIndex),
            ColumnDataType.Float64 => BuildDoubleArray(table, columnIndex),

            // String and Binary
            ColumnDataType.String => BuildStringArray(table, columnIndex),
            ColumnDataType.Json => BuildStringArray(table, columnIndex),
            ColumnDataType.Binary => BuildBinaryArray(table, columnIndex),

            // Date
            ColumnDataType.Date => BuildDate32Array(table, columnIndex),

            // Timestamps
            ColumnDataType.DateTime => BuildTimestampArray(table, columnIndex, TimeUnit.Millisecond),
            ColumnDataType.TimestampSecond => BuildTimestampArray(table, columnIndex, TimeUnit.Second),
            ColumnDataType.TimestampMillisecond => BuildTimestampArray(table, columnIndex, TimeUnit.Millisecond),
            ColumnDataType.TimestampMicrosecond => BuildTimestampArray(table, columnIndex, TimeUnit.Microsecond),
            ColumnDataType.TimestampNanosecond => BuildTimestampArray(table, columnIndex, TimeUnit.Nanosecond),

            // Time
            ColumnDataType.TimeSecond => BuildTime32Array(table, columnIndex, TimeUnit.Second),
            ColumnDataType.TimeMillisecond => BuildTime32Array(table, columnIndex, TimeUnit.Millisecond),
            ColumnDataType.TimeMicrosecond => BuildTime64Array(table, columnIndex, TimeUnit.Microsecond),
            ColumnDataType.TimeNanosecond => BuildTime64Array(table, columnIndex, TimeUnit.Nanosecond),

            _ => throw new NotSupportedException($"Unsupported data type: {dataType}")
        };
    }

    #region Array Builders

    private static BooleanArray BuildBooleanArray(Table.Table table, int columnIndex)
    {
        var builder = new BooleanArray.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((bool)value);
            }
        }
        return builder.Build();
    }

    private static Int8Array BuildInt8Array(Table.Table table, int columnIndex)
    {
        var builder = new Int8Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((sbyte)value);
            }
        }
        return builder.Build();
    }

    private static Int16Array BuildInt16Array(Table.Table table, int columnIndex)
    {
        var builder = new Int16Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((short)value);
            }
        }
        return builder.Build();
    }

    private static Int32Array BuildInt32Array(Table.Table table, int columnIndex)
    {
        var builder = new Int32Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((int)value);
            }
        }
        return builder.Build();
    }

    private static Int64Array BuildInt64Array(Table.Table table, int columnIndex)
    {
        var builder = new Int64Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((long)value);
            }
        }
        return builder.Build();
    }

    private static UInt8Array BuildUInt8Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt8Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((byte)value);
            }
        }
        return builder.Build();
    }

    private static UInt16Array BuildUInt16Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt16Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((ushort)value);
            }
        }
        return builder.Build();
    }

    private static UInt32Array BuildUInt32Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt32Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((uint)value);
            }
        }
        return builder.Build();
    }

    private static UInt64Array BuildUInt64Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt64Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((ulong)value);
            }
        }
        return builder.Build();
    }

    private static FloatArray BuildFloatArray(Table.Table table, int columnIndex)
    {
        var builder = new FloatArray.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((float)value);
            }
        }
        return builder.Build();
    }

    private static DoubleArray BuildDoubleArray(Table.Table table, int columnIndex)
    {
        var builder = new DoubleArray.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((double)value);
            }
        }
        return builder.Build();
    }

    private static StringArray BuildStringArray(Table.Table table, int columnIndex)
    {
        var builder = new StringArray.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((string)value);
            }
        }
        return builder.Build();
    }

    private static BinaryArray BuildBinaryArray(Table.Table table, int columnIndex)
    {
        var builder = new BinaryArray.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                var bytes = (byte[])value;
                builder.Append(bytes.AsSpan());
            }
        }
        return builder.Build();
    }

    private static Date32Array BuildDate32Array(Table.Table table, int columnIndex)
    {
        var builder = new Date32Array.Builder();
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                // Value is stored as int (days since epoch), convert back to DateTime
                var days = (int)value;
                var dateTime = UnixEpochDateTime.AddDays(days);
                builder.Append(dateTime);
            }
        }
        return builder.Build();
    }

    private static TimestampArray BuildTimestampArray(Table.Table table, int columnIndex, TimeUnit unit)
    {
        var type = new TimestampType(unit, (string?)null);
        var builder = new TimestampArray.Builder(type);
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                // Value is stored as long (time since epoch in the appropriate unit)
                // Convert back to DateTimeOffset
                var timestamp = (long)value;
                var dateTimeOffset = ConvertToDateTimeOffset(timestamp, unit);
                builder.Append(dateTimeOffset);
            }
        }
        return builder.Build();
    }

    private static DateTimeOffset ConvertToDateTimeOffset(long timestamp, TimeUnit unit)
    {
        var ticks = unit switch
        {
            TimeUnit.Second => timestamp * TimeSpan.TicksPerSecond,
            TimeUnit.Millisecond => timestamp * TimeSpan.TicksPerMillisecond,
            TimeUnit.Microsecond => timestamp * (TimeSpan.TicksPerMillisecond / 1000),
            TimeUnit.Nanosecond => timestamp / 100,
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, "Unsupported time unit")
        };
        return new DateTimeOffset(UnixEpoch.Ticks + ticks, TimeSpan.Zero);
    }

    private static Time32Array BuildTime32Array(Table.Table table, int columnIndex, TimeUnit unit)
    {
        var type = new Time32Type(unit);
        var builder = new Time32Array.Builder(type);
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((int)(long)value);
            }
        }
        return builder.Build();
    }

    private static Time64Array BuildTime64Array(Table.Table table, int columnIndex, TimeUnit unit)
    {
        var type = new Time64Type(unit);
        var builder = new Time64Array.Builder(type);
        foreach (var row in table.Rows)
        {
            var value = row[columnIndex];
            if (value == null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((long)value);
            }
        }
        return builder.Build();
    }

    #endregion

    private void ThrowIfDisposed()
    {
#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RecordBatchBuilder));
        }
#endif
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        // NativeMemoryAllocator doesn't implement IDisposable, so nothing to dispose here
    }
}
