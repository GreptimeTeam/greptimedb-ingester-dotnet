#:package GreptimeDB.Ingester@0.1.0
#:package MySqlConnector@2.4.0

using System.Diagnostics;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;
using MySqlConnector;

// --- Configuration ---
int[] batchSizes = [100, 500, 1000, 5000];
int[] concurrencyLevels = [1, 2, 4, 8];
int totalRowsPerTask = 100_000;
string endpoint = "http://localhost:4001";
string database = "public";

Console.WriteLine("GreptimeDB .NET Ingester Benchmark");
Console.WriteLine("==================================");
Console.WriteLine($"Endpoint:           {endpoint}");
Console.WriteLine($"Rows per task:      {totalRowsPerTask:N0}");
Console.WriteLine($"Batch sizes:        [{string.Join(", ", batchSizes)}]");
Console.WriteLine($"Concurrency levels: [{string.Join(", ", concurrencyLevels)}]");
Console.WriteLine();

// --- Health check ---
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoint = endpoint,
    Database = database,
    WriteTimeout = TimeSpan.FromSeconds(120)
});

if (!await client.HealthCheckAsync())
{
    Console.Error.WriteLine("ERROR: Cannot connect to GreptimeDB at " + endpoint);
    return;
}
Console.WriteLine("Connected to GreptimeDB.");

// MySQL connection for truncating tables between runs
var mysqlConn = new MySqlConnection("Server=127.0.0.1;Port=4002;Database=public;");
await mysqlConn.OpenAsync();
Console.WriteLine("MySQL connection ready.\n");

// --- Warmup ---
Console.Write("Warming up...");
var warmup = BuildTable("bench_warmup", 100, 0);
await client.WriteAsync(warmup);
Console.WriteLine(" done.\n");

// --- gRPC Unary Write ---
Console.WriteLine("=== gRPC Unary Write (WriteAsync) ===");
Console.WriteLine();
PrintHeader();

foreach (var concurrency in concurrencyLevels)
{
    foreach (var batchSize in batchSizes)
    {
        var result = await RunBenchmark(
            mysqlConn, concurrency, batchSize, totalRowsPerTask,
            async (tbl) => { await client.WriteAsync(tbl); },
            "bench_grpc");
        PrintRow(concurrency, batchSize, result);
    }
}

Console.WriteLine();

// --- gRPC Streaming Write ---
Console.WriteLine("=== gRPC Streaming Write (StreamIngestWriter) ===");
Console.WriteLine();
PrintHeader();

foreach (var concurrency in concurrencyLevels)
{
    foreach (var batchSize in batchSizes)
    {
        var result = await RunBenchmark(
            mysqlConn, concurrency, batchSize, totalRowsPerTask,
            async (tbl) =>
            {
                await using var writer = client.CreateStreamIngestWriter();
                await writer.WriteAsync(tbl);
                await writer.CompleteAsync();
            },
            "bench_stream");
        PrintRow(concurrency, batchSize, result);
    }
}

Console.WriteLine();

// --- Arrow Flight Bulk Write ---
Console.WriteLine("=== Arrow Flight Bulk Write (BulkWriteAsync) ===");
Console.WriteLine();

// Arrow Flight DoPut does not auto-create tables, so pre-create them via gRPC.
Console.Write("Pre-creating tables for Arrow Flight...");
foreach (var concurrency in concurrencyLevels)
{
    foreach (var batchSize in batchSizes)
    {
        var tableName = $"bench_arrow_c{concurrency}_b{batchSize}";
        var seed = BuildTable(tableName, 1, 0);
        await client.WriteAsync(seed);
        await TruncateTable(mysqlConn, tableName);
    }
}
Console.WriteLine(" done.\n");

PrintHeader();

foreach (var concurrency in concurrencyLevels)
{
    foreach (var batchSize in batchSizes)
    {
        var result = await RunBenchmark(
            mysqlConn, concurrency, batchSize, totalRowsPerTask,
            async (tbl) => { await client.BulkWriteAsync(tbl); },
            "bench_arrow");
        PrintRow(concurrency, batchSize, result);
    }
}

await mysqlConn.DisposeAsync();
await client.DisposeAsync();

Console.WriteLine("\nBenchmark complete.");

// --- Helpers ---

async Task<BenchResult> RunBenchmark(
    MySqlConnection mysqlConn, int concurrency, int batchSize, int rowsPerTask,
    Func<Table, Task> writeFunc, string tablePrefix)
{
    int batchesPerTask = rowsPerTask / batchSize;
    if (batchesPerTask == 0) batchesPerTask = 1;
    int actualRowsPerTask = batchesPerTask * batchSize;
    long totalRows = (long)actualRowsPerTask * concurrency;
    string tableName = $"{tablePrefix}_c{concurrency}_b{batchSize}";

    // Truncate table to get clean row counts across runs
    await TruncateTable(mysqlConn, tableName);

    // Pre-build all tables to exclude build time from measurement.
    // Each task gets a unique rowOffset so timestamps never collide across tasks.
    var tables = new Table[concurrency][];
    for (int t = 0; t < concurrency; t++)
    {
        tables[t] = new Table[batchesPerTask];
        for (int b = 0; b < batchesPerTask; b++)
        {
            long rowOffset = (long)t * actualRowsPerTask + (long)b * batchSize;
            tables[t][b] = BuildTable(tableName, batchSize, rowOffset);
        }
    }

    // Force GC before measurement
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();

    var sw = Stopwatch.StartNew();

    var tasks = new Task[concurrency];
    for (int t = 0; t < concurrency; t++)
    {
        var taskTables = tables[t];
        tasks[t] = Task.Run(async () =>
        {
            foreach (var tbl in taskTables)
            {
                await writeFunc(tbl);
            }
        });
    }

    try
    {
        await Task.WhenAll(tasks);
    }
    catch (Exception ex)
    {
        return new BenchResult(totalRows, 0, 0, ex.GetType().Name + ": " + ex.Message);
    }

    sw.Stop();
    double elapsedSec = sw.Elapsed.TotalSeconds;
    double rowsPerSec = totalRows / elapsedSec;

    return new BenchResult(totalRows, elapsedSec, rowsPerSec, null);
}

Table BuildTable(string name, int rows, long rowOffset)
{
    var builder = new TableBuilder(name)
        .AddTag("host", ColumnDataType.String)
        .AddTag("region", ColumnDataType.String)
        .AddField("cpu", ColumnDataType.Float64)
        .AddField("memory", ColumnDataType.Float64)
        .AddField("disk_io", ColumnDataType.Float64)
        .AddField("network_in", ColumnDataType.Float64)
        .AddField("network_out", ColumnDataType.Float64)
        .AddField("active_conns", ColumnDataType.Int32)
        .AddTimestamp("ts", ColumnDataType.TimestampMillisecond);

    var random = new Random();
    string[] hosts = ["web-01", "web-02", "web-03", "db-01", "db-02", "cache-01", "cache-02", "lb-01"];
    string[] regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];

    // Use a fixed base time with sequential millisecond offsets to guarantee unique timestamps.
    var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    for (int i = 0; i < rows; i++)
    {
        builder.AddRow(
            hosts[random.Next(hosts.Length)],
            regions[random.Next(regions.Length)],
            Math.Round(random.NextDouble() * 100, 2),
            Math.Round(20 + random.NextDouble() * 70, 2),
            Math.Round(random.NextDouble() * 500, 2),
            Math.Round(random.NextDouble() * 1000, 2),
            Math.Round(random.NextDouble() * 800, 2),
            random.Next(0, 5000),
            baseTime.AddMilliseconds(rowOffset + i));
    }

    return builder.Build();
}

async Task TruncateTable(MySqlConnection conn, string tableName)
{
    try
    {
        await using var cmd = new MySqlCommand($"TRUNCATE TABLE `{tableName}`", conn);
        await cmd.ExecuteNonQueryAsync();
    }
    catch
    {
        // Table may not exist yet, ignore.
    }
}

void PrintHeader()
{
    Console.WriteLine($"{"Concurrency",12} {"Batch",8} {"Total Rows",12} {"Time (s)",10} {"Rows/s",14} {"Status",10}");
    Console.WriteLine(new string('-', 70));
}

void PrintRow(int concurrency, int batchSize, BenchResult r)
{
    if (r.Error != null)
    {
        Console.WriteLine($"{concurrency,12} {batchSize,8} {r.TotalRows,12:N0} {"—",10} {"—",14} {"FAILED",10}");
        Console.WriteLine($"  Error: {r.Error}");
    }
    else
    {
        Console.WriteLine($"{concurrency,12} {batchSize,8} {r.TotalRows,12:N0} {r.ElapsedSec,10:F2} {r.RowsPerSec,14:N0} {"OK",10}");
    }
}

record BenchResult(long TotalRows, double ElapsedSec, double RowsPerSec, string? Error);
