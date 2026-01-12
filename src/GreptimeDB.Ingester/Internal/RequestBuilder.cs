using Google.Protobuf;
using Greptime.V1;
using GreptimeDB.Ingester.Table;
using SdkColumnDataType = GreptimeDB.Ingester.Types.ColumnDataType;
using SdkSemanticType = GreptimeDB.Ingester.Types.SemanticType;

namespace GreptimeDB.Ingester.Internal;

/// <summary>
/// Converts Table instances to gRPC request messages.
/// </summary>
internal static class RequestBuilder
{
    /// <summary>
    /// Builds a RowInsertRequests message from a collection of tables.
    /// </summary>
    public static RowInsertRequests BuildRowInsertRequests(IEnumerable<Table.Table> tables)
    {
        var requests = new RowInsertRequests();
        foreach (var table in tables)
        {
            requests.Inserts.Add(BuildRowInsertRequest(table));
        }
        return requests;
    }

    /// <summary>
    /// Builds a RowInsertRequest message from a single table.
    /// </summary>
    public static RowInsertRequest BuildRowInsertRequest(Table.Table table)
    {
        var request = new RowInsertRequest
        {
            TableName = table.Name,
            Rows = BuildRows(table)
        };
        return request;
    }

    /// <summary>
    /// Builds a Rows message containing schema and row data.
    /// </summary>
    public static Rows BuildRows(Table.Table table)
    {
        var rows = new Rows();

        // Build schema
        foreach (var column in table.Columns)
        {
            rows.Schema.Add(new ColumnSchema
            {
                ColumnName = column.Name,
                Datatype = ToProtoDataType(column.DataType),
                SemanticType = ToProtoSemanticType(column.SemanticType)
            });
        }

        // Build row data
        foreach (var rowValues in table.Rows)
        {
            var row = new Row();
            for (var i = 0; i < rowValues.Length; i++)
            {
                row.Values.Add(BuildValue(rowValues[i], table.Columns[i].DataType));
            }
            rows.Rows_.Add(row);
        }

        return rows;
    }

    /// <summary>
    /// Builds a RowDeleteRequests message from a collection of tables.
    /// Only Tag and Timestamp columns are used for delete operations.
    /// </summary>
    public static RowDeleteRequests BuildRowDeleteRequests(IEnumerable<Table.Table> tables)
    {
        var requests = new RowDeleteRequests();
        foreach (var table in tables)
        {
            requests.Deletes.Add(BuildRowDeleteRequest(table));
        }
        return requests;
    }

    /// <summary>
    /// Builds a RowDeleteRequest message from a single table.
    /// </summary>
    public static RowDeleteRequest BuildRowDeleteRequest(Table.Table table)
    {
        var request = new RowDeleteRequest
        {
            TableName = table.Name,
            Rows = BuildRows(table)
        };
        return request;
    }

    private static Value BuildValue(object? value, SdkColumnDataType dataType)
    {
        var protoValue = new Value();

        if (value == null)
        {
            // Null value - leave all fields unset
            return protoValue;
        }

        switch (dataType)
        {
            case SdkColumnDataType.Boolean:
                protoValue.BoolValue = (bool)value;
                break;

            case SdkColumnDataType.Int8:
                protoValue.I8Value = (sbyte)value;
                break;

            case SdkColumnDataType.Int16:
                protoValue.I16Value = (short)value;
                break;

            case SdkColumnDataType.Int32:
                protoValue.I32Value = (int)value;
                break;

            case SdkColumnDataType.Int64:
                protoValue.I64Value = (long)value;
                break;

            case SdkColumnDataType.UInt8:
                protoValue.U8Value = (byte)value;
                break;

            case SdkColumnDataType.UInt16:
                protoValue.U16Value = (ushort)value;
                break;

            case SdkColumnDataType.UInt32:
                protoValue.U32Value = (uint)value;
                break;

            case SdkColumnDataType.UInt64:
                protoValue.U64Value = (ulong)value;
                break;

            case SdkColumnDataType.Float32:
                protoValue.F32Value = (float)value;
                break;

            case SdkColumnDataType.Float64:
                protoValue.F64Value = (double)value;
                break;

            case SdkColumnDataType.String:
            case SdkColumnDataType.Json:
                protoValue.StringValue = (string)value;
                break;

            case SdkColumnDataType.Binary:
                protoValue.BinaryValue = ByteString.CopyFrom((byte[])value);
                break;

            case SdkColumnDataType.Date:
                protoValue.DateValue = (int)value;
                break;

            case SdkColumnDataType.DateTime:
                protoValue.DatetimeValue = (long)value;
                break;

            case SdkColumnDataType.TimestampSecond:
                protoValue.TimestampSecondValue = (long)value;
                break;

            case SdkColumnDataType.TimestampMillisecond:
                protoValue.TimestampMillisecondValue = (long)value;
                break;

            case SdkColumnDataType.TimestampMicrosecond:
                protoValue.TimestampMicrosecondValue = (long)value;
                break;

            case SdkColumnDataType.TimestampNanosecond:
                protoValue.TimestampNanosecondValue = (long)value;
                break;

            case SdkColumnDataType.TimeSecond:
                protoValue.TimeSecondValue = (long)value;
                break;

            case SdkColumnDataType.TimeMillisecond:
                protoValue.TimeMillisecondValue = (long)value;
                break;

            case SdkColumnDataType.TimeMicrosecond:
                protoValue.TimeMicrosecondValue = (long)value;
                break;

            case SdkColumnDataType.TimeNanosecond:
                protoValue.TimeNanosecondValue = (long)value;
                break;

            default:
                throw new NotSupportedException($"Unsupported data type: {dataType}");
        }

        return protoValue;
    }

    private static Greptime.V1.ColumnDataType ToProtoDataType(SdkColumnDataType dataType)
    {
        return dataType switch
        {
            SdkColumnDataType.Boolean => Greptime.V1.ColumnDataType.Boolean,
            SdkColumnDataType.Int8 => Greptime.V1.ColumnDataType.Int8,
            SdkColumnDataType.Int16 => Greptime.V1.ColumnDataType.Int16,
            SdkColumnDataType.Int32 => Greptime.V1.ColumnDataType.Int32,
            SdkColumnDataType.Int64 => Greptime.V1.ColumnDataType.Int64,
            SdkColumnDataType.UInt8 => Greptime.V1.ColumnDataType.Uint8,
            SdkColumnDataType.UInt16 => Greptime.V1.ColumnDataType.Uint16,
            SdkColumnDataType.UInt32 => Greptime.V1.ColumnDataType.Uint32,
            SdkColumnDataType.UInt64 => Greptime.V1.ColumnDataType.Uint64,
            SdkColumnDataType.Float32 => Greptime.V1.ColumnDataType.Float32,
            SdkColumnDataType.Float64 => Greptime.V1.ColumnDataType.Float64,
            SdkColumnDataType.Binary => Greptime.V1.ColumnDataType.Binary,
            SdkColumnDataType.String => Greptime.V1.ColumnDataType.String,
            SdkColumnDataType.Date => Greptime.V1.ColumnDataType.Date,
            SdkColumnDataType.DateTime => Greptime.V1.ColumnDataType.Datetime,
            SdkColumnDataType.TimestampSecond => Greptime.V1.ColumnDataType.TimestampSecond,
            SdkColumnDataType.TimestampMillisecond => Greptime.V1.ColumnDataType.TimestampMillisecond,
            SdkColumnDataType.TimestampMicrosecond => Greptime.V1.ColumnDataType.TimestampMicrosecond,
            SdkColumnDataType.TimestampNanosecond => Greptime.V1.ColumnDataType.TimestampNanosecond,
            SdkColumnDataType.TimeSecond => Greptime.V1.ColumnDataType.TimeSecond,
            SdkColumnDataType.TimeMillisecond => Greptime.V1.ColumnDataType.TimeMillisecond,
            SdkColumnDataType.TimeMicrosecond => Greptime.V1.ColumnDataType.TimeMicrosecond,
            SdkColumnDataType.TimeNanosecond => Greptime.V1.ColumnDataType.TimeNanosecond,
            SdkColumnDataType.Json => Greptime.V1.ColumnDataType.Json,
            _ => throw new NotSupportedException($"Unsupported data type: {dataType}")
        };
    }

    private static Greptime.V1.SemanticType ToProtoSemanticType(SdkSemanticType semanticType)
    {
        return semanticType switch
        {
            SdkSemanticType.Tag => Greptime.V1.SemanticType.Tag,
            SdkSemanticType.Field => Greptime.V1.SemanticType.Field,
            SdkSemanticType.Timestamp => Greptime.V1.SemanticType.Timestamp,
            _ => throw new NotSupportedException($"Unsupported semantic type: {semanticType}")
        };
    }
}
