using FluentAssertions;
using GreptimeDB.Ingester.Internal;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class RequestBuilderTests
{
    [Fact]
    public void BuildRowInsertRequest_DateTime_UsesTimestampMicrosecond()
    {
        var dateTimeValue = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc).AddTicks(370); // +37us
        var expectedMicros = (dateTimeValue - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).Ticks / 10;

        var table = new TableBuilder("metrics")
            .AddField("dt", ColumnDataType.DateTime)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(dateTimeValue, DateTime.UtcNow)
            .Build();

        var request = RequestBuilder.BuildRowInsertRequest(table);

        request.Rows.Schema[0].Datatype.Should().Be(Greptime.V1.ColumnDataType.TimestampMicrosecond);
        request.Rows.Rows_[0].Values[0].TimestampMicrosecondValue.Should().Be(expectedMicros);
        request.Rows.Rows_[0].Values[0].ValueDataCase.Should().Be(
            Greptime.V1.Value.ValueDataOneofCase.TimestampMicrosecondValue);
    }

    [Fact]
    public void ColumnDataType_Json_Value_IsAlignedWithProto()
    {
        ((int)ColumnDataType.Json).Should().Be((int)Greptime.V1.ColumnDataType.Json);
    }
}
