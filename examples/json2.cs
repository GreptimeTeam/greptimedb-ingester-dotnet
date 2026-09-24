#:package GreptimeDB.Ingester@0.4.0
#:package MySqlConnector@2.4.0

using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using MySqlConnector;

// Requires GreptimeDB 1.2.1 or later.
const string TableName = "dotnet_json2_logs";

var connStr = "Server=127.0.0.1;Port=4002;Database=public;";
await using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();

// Start from a clean table so repeated runs print the same result.
await using (var cmd = new MySqlCommand($"DROP TABLE IF EXISTS {TableName}", conn))
{
    await cmd.ExecuteNonQueryAsync();
}

// --- Write via gRPC ---
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://localhost:4001" },
    Database = "public"
});

var now = DateTime.UtcNow;
var table = new TableBuilder(TableName)
    .AddTag("service", ColumnDataType.String)
    .AddField("payload", ColumnDataType.Json2)
    .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
    .AddRow("api", """{"level":"info","latency_ms":12,"user":{"id":42,"roles":["admin","dev"]}}""", now)
    .AddRow("api", """{"level":"error","latency_ms":350,"error":{"code":503,"message":"upstream timeout"}}""", now.AddSeconds(1))
    .AddRow("worker", """{"level":"info","latency_ms":87,"job":"reindex"}""", now.AddSeconds(2))
    .AddRow("worker", null, now.AddSeconds(3))
    .Build();

// The table is auto-created on first write; JSON2 columns require append_mode=true.
var affected = await client.WriteAsync(table, new Dictionary<string, string> { ["append_mode"] = "true" });
Console.WriteLine($"[gRPC] Written {affected} rows");

await client.DisposeAsync();

// --- Query via MySQL ---
Console.WriteLine("\n--- All rows ---");
await using (var cmd = new MySqlCommand(
    $"SELECT service, json_get(payload, '') FROM {TableName} ORDER BY ts", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
    {
        var payload = reader.IsDBNull(1) ? "NULL" : reader.GetString(1);
        Console.WriteLine($"{reader.GetString(0),-8} {payload}");
    }
}

Console.WriteLine("\n--- Path access: errors ---");
await using (var cmd = new MySqlCommand(
    $"SELECT service, payload.latency_ms::BIGINT, payload.error.message FROM {TableName} " +
    "WHERE payload.level = 'error'", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"{reader.GetString(0),-8} latency={reader.GetInt64(1)}ms message={reader.GetString(2)}");
    }
}

Console.WriteLine("\n--- Avg latency by service ---");
await using (var cmd = new MySqlCommand(
    $"SELECT service, avg(payload.latency_ms::BIGINT) FROM {TableName} " +
    "WHERE payload IS NOT NULL GROUP BY service ORDER BY service", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"{reader.GetString(0),-8} {reader.GetDouble(1):F1}ms");
    }
}

Console.WriteLine("\nDone!");
