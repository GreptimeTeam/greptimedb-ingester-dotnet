#:package GreptimeDB.Ingester@0.1.0

using System.Diagnostics;
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;

// --- Configuration ---
int[] batchSizes = [100, 500, 1000, 5000];
int[] concurrencyLevels = [1, 2, 4, 8];
int totalRowsPerTask = 10_000;
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
Console.WriteLine("Connected to GreptimeDB.\n");

// --- Warmup ---
Console.Write("Warming up...");
var warmup = BuildTable("bench_warmup", 100);
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
            concurrency, batchSize, totalRowsPerTask,
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
            concurrency, batchSize, totalRowsPerTask,
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
        var seed = BuildTable($"bench_arrow_c{concurrency}_b{batchSize}", 1);
        await client.WriteAsync(seed);
    }
}
Console.WriteLine(" done.\n");

PrintHeader();

foreach (var concurrency in concurrencyLevels)
{
    foreach (var batchSize in batchSizes)
    {
        var result = await RunBenchmark(
            concurrency, batchSize, totalRowsPerTask,
            async (tbl) => { await client.BulkWriteAsync(tbl); },
            "bench_arrow");
        PrintRow(concurrency, batchSize, result);
    }
}

await client.DisposeAsync();

Console.WriteLine("\nBenchmark complete.");

// --- Helpers ---

async Task<BenchResult> RunBenchmark(
    int concurrency, int batchSize, int rowsPerTask,
    Func<Table, Task> writeFunc, string tablePrefix)
{
    int batchesPerTask = rowsPerTask / batchSize;
    if (batchesPerTask == 0) batchesPerTask = 1;
    int actualRowsPerTask = batchesPerTask * batchSize;
    long totalRows = (long)actualRowsPerTask * concurrency;

    // Pre-build all tables to exclude build time from measurement
    var tables = new Table[concurrency][];
    for (int t = 0; t < concurrency; t++)
    {
        tables[t] = new Table[batchesPerTask];
        for (int b = 0; b < batchesPerTask; b++)
        {
            tables[t][b] = BuildTable($"{tablePrefix}_c{concurrency}_b{batchSize}", batchSize);
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

Table BuildTable(string name, int rows)
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
            DateTime.UtcNow.AddMilliseconds(-random.Next(0, 3_600_000)));
    }

    return builder.Build();
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
