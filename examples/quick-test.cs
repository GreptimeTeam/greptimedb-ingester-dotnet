#:package GreptimeDB.Ingester@0.1.0
#:package MySqlConnector@2.4.0

using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using MySqlConnector;

// --- Write via gRPC ---
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://localhost:4001" },
    Database = "public"
});

var table = new TableBuilder("dotnet_test_metrics")
    .AddTag("host", ColumnDataType.String)
    .AddTag("region", ColumnDataType.String)
    .AddField("cpu_usage", ColumnDataType.Float64)
    .AddField("memory_usage", ColumnDataType.Float64)
    .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

var now = DateTime.UtcNow;
var random = new Random(42);
string[] hosts = ["web-01", "web-02", "db-01", "db-02", "cache-01"];
string[] regions = ["us-east", "us-west", "eu-west"];

for (int i = 0; i < 100; i++)
{
    var host = hosts[i % hosts.Length];
    var region = regions[i % regions.Length];
    var cpu = Math.Round(random.NextDouble() * 100, 2);
    var mem = Math.Round(30 + random.NextDouble() * 60, 2);
    var ts = now.AddSeconds(-i * 10);
    table.AddRow(host, region, cpu, mem, ts);
}

var built = table.Build();
var affected = await client.WriteAsync(built);
Console.WriteLine($"[gRPC] Written {affected} rows");

await client.DisposeAsync();

// --- Query via MySQL ---
var connStr = "Server=127.0.0.1;Port=4002;Database=public;";
await using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();

Console.WriteLine("\n--- Row count ---");
await using (var cmd = new MySqlCommand("SELECT count(*) FROM dotnet_test_metrics", conn))
{
    var count = await cmd.ExecuteScalarAsync();
    Console.WriteLine($"Total rows: {count}");
}

Console.WriteLine("\n--- Avg CPU/Memory by host ---");
await using (var cmd = new MySqlCommand(
    "SELECT host, round(avg(cpu_usage),2) as avg_cpu, round(avg(memory_usage),2) as avg_mem " +
    "FROM dotnet_test_metrics GROUP BY host ORDER BY host", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    Console.WriteLine($"{"Host",-12} {"Avg CPU",10} {"Avg Mem",10}");
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"{reader.GetString(0),-12} {reader.GetDouble(1),10:F2} {reader.GetDouble(2),10:F2}");
    }
}

Console.WriteLine("\n--- Latest 5 rows ---");
await using (var cmd = new MySqlCommand(
    "SELECT host, region, cpu_usage, memory_usage, ts " +
    "FROM dotnet_test_metrics ORDER BY ts DESC LIMIT 5", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    Console.WriteLine($"{"Host",-12} {"Region",-10} {"CPU",8} {"Memory",8} {"Timestamp",-24}");
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"{reader.GetString(0),-12} {reader.GetString(1),-10} {reader.GetDouble(2),8:F2} {reader.GetDouble(3),8:F2} {reader.GetDateTime(4),-24:yyyy-MM-dd HH:mm:ss}");
    }
}

Console.WriteLine("\nDone!");
