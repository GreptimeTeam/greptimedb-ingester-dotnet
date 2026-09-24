using System.Text.Json;
using Greptime.V1;

namespace GreptimeDB.Ingester.Internal;

/// <summary>
/// Parses JSON text into the native protobuf representation used by JSON2 columns.
/// </summary>
internal static class Json2Encoder
{
    /// <summary>
    /// Parses a JSON2 value. Returns null for JSON <c>null</c>, which is written as SQL NULL.
    /// </summary>
    /// <exception cref="FormatException">Thrown when the text is not valid JSON2 input.</exception>
    public static JsonValue? Parse(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            return root.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.Object => Encode(root),
                _ => throw new FormatException("expected a JSON object or null."),
            };
        }
        catch (JsonException ex)
        {
            throw new FormatException(ex.Message, ex);
        }
        // Thrown by GetString() for strings containing unpaired surrogates.
        catch (InvalidOperationException ex)
        {
            throw new FormatException(ex.Message, ex);
        }
    }

    private static JsonValue Encode(JsonElement element)
    {
        var value = new JsonValue();
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var obj = new JsonObject();
                foreach (var property in element.EnumerateObject())
                {
                    obj.Entries.Add(new JsonObject.Types.Entry
                    {
                        Key = property.Name,
                        Value = Encode(property.Value),
                    });
                }
                value.Object = obj;
                break;

            case JsonValueKind.Array:
                var list = new JsonList();
                foreach (var item in element.EnumerateArray())
                {
                    list.Items.Add(Encode(item));
                }
                value.Array = list;
                break;

            case JsonValueKind.String:
                value.Str = element.GetString();
                break;

            case JsonValueKind.Number:
                // Integers are sent as uint64 when non-negative and int64 when negative;
                // fractions, exponents and integers beyond 64 bits are sent as double.
                if (element.TryGetUInt64(out var u))
                {
                    value.Uint = u;
                }
                else if (element.TryGetInt64(out var i))
                {
                    value.Int = i;
                }
                else if (element.TryGetDouble(out var d) && double.IsFinite(d))
                {
                    value.Float = d;
                }
                else
                {
                    throw new FormatException($"number {element.GetRawText()} is out of range.");
                }
                break;

            case JsonValueKind.True:
                value.Boolean = true;
                break;

            case JsonValueKind.False:
                value.Boolean = false;
                break;

            // JSON null inside an object or array is an empty JsonValue.
            case JsonValueKind.Null:
                break;
        }
        return value;
    }
}
