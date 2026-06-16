# GreptimeDB .NET Ingester

[![CI](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/GreptimeDB.Ingester.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester)
[![NuGet Downloads](https://img.shields.io/nuget/dt/GreptimeDB.Ingester.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester)
[![NuGet Grpc](https://img.shields.io/nuget/v/GreptimeDB.Ingester.Grpc.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester.Grpc)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple)

.NET SDK for writing data to [GreptimeDB](https://github.com/GreptimeTeam/greptimedb).

> **Warning**
> This project is under heavy development. APIs may change without notice.
> Use at your own risk in production environments.

## Installation

```bash
dotnet add package GreptimeDB.Ingester
```

> **.NET 6.0 / 7.0 users:** the latest version requires .NET 8.0 or newer.
> Stay on the `0.1.x` line for net6.0 / net7.0 support:
>
> ```bash
> dotnet add package GreptimeDB.Ingester --version 0.1.*
> ```

## Quick Start

```csharp
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;

// Create client
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://localhost:4001" },
    Database = "public"
});

// Build table
var table = new TableBuilder("cpu_metrics")
    .AddTag("host", ColumnDataType.String)
    .AddField("usage", ColumnDataType.Float64)
    .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
    .AddRow("server1", 0.85, DateTime.UtcNow)
    .AddRow("server2", 0.72, DateTime.UtcNow)
    .Build();

// Write
var affectedRows = await client.WriteAsync(table);

// Cleanup
await client.DisposeAsync();
```

## Client Options

```csharp
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://localhost:4001" },
    Database = "public",
    ConnectTimeout = TimeSpan.FromSeconds(5),
    WriteTimeout = TimeSpan.FromSeconds(30)
});
```

With basic auth:

```csharp
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://localhost:4001" },
    Database = "public",
    Authentication = new AuthenticationOptions
    {
        Username = "greptime_user",
        Password = "greptime_password"
    }
});
```

## Multiple Endpoints

Pass more than one endpoint to enable client-side endpoint selection and
request-level failover across a GreptimeDB cluster:

```csharp
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoints = new List<string>
    {
        "http://node-a:4001",
        "http://node-b:4001",
        "http://node-c:4001",
    },
    Database = "public",
});
```

A single-element list is the single-node case; two or more endpoints are picked
per request with the configured strategy. All endpoints must share the same
scheme (all `http` or all `https`) and must be plain `host:port` URIs without a
path/query/fragment.

### Load-balancing strategy

`LoadBalancing` selects the policy used in the multi-endpoint case:

```csharp
new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://node-a:4001", "http://node-b:4001" },
    LoadBalancing = LoadBalancingStrategy.Random,     // default
    // LoadBalancing = LoadBalancingStrategy.RoundRobin,
};
```

- `Random` (default) — pick a ready endpoint uniformly at random per call.
  Avoids the herding pattern that round-robin can produce when many
  short-lived clients start simultaneously.
- `RoundRobin` — cycle through ready endpoints in order.

### Failover

Unary `WriteAsync` and `DeleteAsync` retry transient endpoint-level failures
against another configured endpoint when it is safe to do so. Server-side
business errors are not retried and do not mark an endpoint unhealthy. Client
write deadlines are not replayed because the server may already have applied
the write before the client observed the timeout.

```csharp
new GreptimeClientOptions
{
    Endpoints = new List<string> { "http://node-a:4001", "http://node-b:4001" },
    Failover = new FailoverOptions
    {
        MaxAttempts = 2, // defaults to trying each endpoint once
    },
};
```

Streaming and bulk writers are not automatically replayed after a transport
failure because doing so could duplicate writes. Their final transport outcome
still updates endpoint health, so creating a new writer can choose a healthy
endpoint.

## Features

- **Unary Write** - Simple single-request writes via gRPC
- **Streaming Write** - High-throughput streaming via gRPC for multiple tables
- **Bulk Write** - Maximum throughput via Apache Arrow Flight
- Multi-endpoint client-side load balancing (random / round-robin) with failover
- Type coercion between .NET and GreptimeDB types
- Health check
- DI integration

## Streaming Write

For high-throughput scenarios with multiple tables:

```csharp
await using var writer = client.CreateStreamIngestWriter();

// Write multiple tables in a single stream
await writer.WriteAsync(table1);
await writer.WriteAsync(table2);
await writer.WriteAsync(table3);

var affectedRows = await writer.CompleteAsync();
```

Custom stream options:

```csharp
await using var writer = client.CreateStreamIngestWriter(new StreamIngestWriterOptions
{
    BufferCapacity = 2000,
    WriteTimeout = TimeSpan.FromSeconds(60)
});
```

## Bulk Write (Arrow Flight)

For maximum throughput using Apache Arrow Flight protocol:

```csharp
// Convenience helper for single-table bulk write
var affectedRows = await client.BulkWriteAsync(table);
```

Or manage the writer lifetime yourself:

```csharp
// Note: Tables must exist before using BulkWriter
await using var writer = client.CreateBulkWriter();

await writer.WriteAsync(table);

var affectedRows = await writer.CompleteAsync();
```

> **Note**: Unlike regular gRPC writes, Arrow Flight `DoPut` does not auto-create tables.
> Ensure tables exist before using `BulkWriter`. A `BulkWriter` instance is bound to a single
> table; create a new writer per table when bulk writing multiple tables.

## Delete Data

```csharp
var deleteTable = new TableBuilder("cpu_metrics")
    .AddTag("host", ColumnDataType.String)
    .AddTimestamp("ts", ColumnDataType.TimestampMillisecond)
    .AddRow("server1", DateTime.UtcNow)
    .Build();

var affectedRows = await client.DeleteAsync(deleteTable);
```

## Health Check

```csharp
var healthy = await client.HealthCheckAsync();
```

## Error Handling

```csharp
try
{
    await client.WriteAsync(table);
}
catch (GreptimeDB.Ingester.Exceptions.GreptimeException ex)
{
    Console.Error.WriteLine(ex.Message);
}
```

## Type Notes

- `DateTime` maps to microsecond timestamp semantics.
- `Timestamp*` types preserve explicit precision (`Second`, `Millisecond`, `Microsecond`, `Nanosecond`).
- `Json` is sent as JSON string content.

## DI Integration

```csharp
services.AddGreptimeClient(options =>
{
    options.Endpoints = new List<string> { "http://localhost:4001" };
    options.Database = "public";
});
```

## Examples

See the [examples](examples/) directory for runnable scripts including a quick-start test and a performance benchmark. Requires .NET 10 SDK.

## License

Apache License 2.0
