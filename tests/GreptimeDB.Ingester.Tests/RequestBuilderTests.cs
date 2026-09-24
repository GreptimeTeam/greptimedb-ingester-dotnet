using FluentAssertions;
using Greptime.V1;
using GreptimeDB.Ingester.Internal;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using Xunit;
using ColumnDataType = GreptimeDB.Ingester.Types.ColumnDataType;

namespace GreptimeDB.Ingester.Tests;

public class RequestBuilderTests
{
    [Fact]
    public void BuildRowInsertRequest_DateTimeValue_UsesTimestampMicrosecond()
    {
        var dateTimeValue = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc).AddTicks(370); // +37us
        var expectedMicros = (dateTimeValue - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).Ticks / 10;

        var table = new TableBuilder("metrics")
            .AddField("dt", ColumnDataType.TimestampMicrosecond)
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

    [Fact]
    public void BuildRowInsertRequest_Json2_EncodesNativeJsonSchemaAndValues()
    {
        var table = new TableBuilder("logs")
            .AddField("payload", ColumnDataType.Json2)
            .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
            .AddRow("""{"items":[true,-9223372036854775808,18446744073709551615,18446744073709551616,1.5,"你好",null,{},[]],"k":null}""", 1L)
            .AddRow("null", 2L)
            .AddRow(null, 3L)
            .Build();

        var rows = RequestBuilder.BuildRowInsertRequest(table).Rows;

        var schema = rows.Schema[0];
        schema.Datatype.Should().Be(Greptime.V1.ColumnDataType.Json);
        schema.SemanticType.Should().Be(Greptime.V1.SemanticType.Field);
        schema.DatatypeExtension.JsonNativeType.Datatype.Should().Be(Greptime.V1.ColumnDataType.Json);
        schema.Options.Options.Should().Equal(new Dictionary<string, string>
        {
            ["ARROW:extension:name"] = "greptime.json2",
            ["ARROW:extension:metadata"] =
                """{"json_settings":{"type_hints":[],"max_auto_expanded_paths":100},"layout_version":2}""",
        });

        var items = new JsonList
        {
            Items =
            {
                new JsonValue { Boolean = true },
                new JsonValue { Int = long.MinValue },
                new JsonValue { Uint = ulong.MaxValue },
                new JsonValue { Float = 18446744073709551616d },
                new JsonValue { Float = 1.5 },
                new JsonValue { Str = "你好" },
                new JsonValue(),
                new JsonValue { Object = new JsonObject() },
                new JsonValue { Array = new JsonList() },
            }
        };
        var expected = new JsonValue
        {
            Object = new JsonObject
            {
                Entries =
                {
                    new JsonObject.Types.Entry { Key = "items", Value = new JsonValue { Array = items } },
                    new JsonObject.Types.Entry { Key = "k", Value = new JsonValue() },
                }
            }
        };
        rows.Rows_[0].Values[0].JsonValue.Should().Be(expected);
        rows.Rows_[1].Values[0].ValueDataCase.Should().Be(Greptime.V1.Value.ValueDataOneofCase.None);
        rows.Rows_[2].Values[0].ValueDataCase.Should().Be(Greptime.V1.Value.ValueDataOneofCase.None);
    }
}
