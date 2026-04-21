using FluentAssertions;
using GreptimeDB.Ingester.Internal;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;

namespace GreptimeDB.Ingester.Tests;

public class TableBuilderTests
{
    [Fact]
    public void AddField_DateTime_NormalizesToTimestampMicrosecond()
    {
#pragma warning disable CS0618 // intentional: verify the deprecated DATETIME alias is normalized to TimestampMicrosecond
        var table = new TableBuilder("t")
            .AddField("dt", ColumnDataType.DateTime)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow(DateTime.UtcNow, DateTime.UtcNow)
            .Build();
#pragma warning restore CS0618

        table.Columns[0].DataType.Should().Be(ColumnDataType.TimestampMicrosecond);
    }

    [Fact]
    public void AddTimestamp_DateTime_NormalizesToTimestampMicrosecondAndSerializesAsSuch()
    {
#pragma warning disable CS0618
        var table = new TableBuilder("t")
            .AddTag("host", ColumnDataType.String)
            .AddTimestamp("ts", ColumnDataType.DateTime)
            .AddRow("h1", DateTime.UtcNow)
            .Build();
#pragma warning restore CS0618

        table.Columns[1].DataType.Should().Be(ColumnDataType.TimestampMicrosecond);

        var request = RequestBuilder.BuildRowInsertRequest(table);
        request.Rows.Schema[1].Datatype.Should().Be(Greptime.V1.ColumnDataType.TimestampMicrosecond);
    }
}
