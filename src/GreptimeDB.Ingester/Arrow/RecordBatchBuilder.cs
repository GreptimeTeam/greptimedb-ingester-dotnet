using Apache.Arrow;
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

    private bool _disposed;

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

    // Note: Arrow's builder API lacks a unified generic interface (Build() requires allocator,
    // StringArray.Builder uses different interface), so we keep explicit methods for each type.

    private static BooleanArray BuildBooleanArray(Table.Table table, int columnIndex)
    {
        var builder = new BooleanArray.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is bool value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static Int8Array BuildInt8Array(Table.Table table, int columnIndex)
    {
        var builder = new Int8Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is sbyte value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static Int16Array BuildInt16Array(Table.Table table, int columnIndex)
    {
        var builder = new Int16Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is short value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static Int32Array BuildInt32Array(Table.Table table, int columnIndex)
    {
        var builder = new Int32Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is int value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static Int64Array BuildInt64Array(Table.Table table, int columnIndex)
    {
        var builder = new Int64Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is long value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static UInt8Array BuildUInt8Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt8Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is byte value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static UInt16Array BuildUInt16Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt16Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is ushort value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static UInt32Array BuildUInt32Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt32Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is uint value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static UInt64Array BuildUInt64Array(Table.Table table, int columnIndex)
    {
        var builder = new UInt64Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is ulong value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static FloatArray BuildFloatArray(Table.Table table, int columnIndex)
    {
        var builder = new FloatArray.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is float value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static DoubleArray BuildDoubleArray(Table.Table table, int columnIndex)
    {
        var builder = new DoubleArray.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is double value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static StringArray BuildStringArray(Table.Table table, int columnIndex)
    {
        var builder = new StringArray.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is string value)
                builder.Append(value);
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static BinaryArray BuildBinaryArray(Table.Table table, int columnIndex)
    {
        var builder = new BinaryArray.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is byte[] value)
                builder.Append(value.AsSpan());
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static Date32Array BuildDate32Array(Table.Table table, int columnIndex)
    {
        var builder = new Date32Array.Builder();
        foreach (var row in table.Rows)
        {
            if (row[columnIndex] is int days)
                builder.Append(UnixEpochDateTime.AddDays(days));
            else
                builder.AppendNull();
        }
        return builder.Build();
    }

    private static TimestampArray BuildTimestampArray(Table.Table table, int columnIndex, TimeUnit unit)
    {
        var builder = new TimestampArray.Builder(new TimestampType(unit, (string?)null));
        foreach (var row in table.Rows)
        {
            switch (row[columnIndex])
            {
                case long timestamp:
                    builder.Append(ConvertToDateTimeOffset(timestamp, unit));
                    break;
                case int timestamp:
                    builder.Append(ConvertToDateTimeOffset(timestamp, unit));
                    break;
                default:
                    builder.AppendNull();
                    break;
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
        var builder = new Time32Array.Builder(new Time32Type(unit));
        foreach (var row in table.Rows)
        {
            switch (row[columnIndex])
            {
                case long value:
                    builder.Append((int)value);
                    break;
                case int value:
                    builder.Append(value);
                    break;
                default:
                    builder.AppendNull();
                    break;
            }
        }
        return builder.Build();
    }

    private static Time64Array BuildTime64Array(Table.Table table, int columnIndex, TimeUnit unit)
    {
        var builder = new Time64Array.Builder(new Time64Type(unit));
        foreach (var row in table.Rows)
        {
            switch (row[columnIndex])
            {
                case long value:
                    builder.Append(value);
                    break;
                case int value:
                    builder.Append(value);
                    break;
                default:
                    builder.AppendNull();
                    break;
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
    }
}
