# GreptimeDB .NET Ingester

[![CI](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/GreptimeDB.Ingester.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester)
[![NuGet Downloads](https://img.shields.io/nuget/dt/GreptimeDB.Ingester.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester)
[![NuGet Grpc](https://img.shields.io/nuget/v/GreptimeDB.Ingester.Grpc.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester.Grpc)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![.NET](https://img.shields.io/badge/.NET-6.0%20%7C%207.0%20%7C%208.0%20%7C%209.0%20%7C%2010.0-purple)

.NET SDK for writing data to [GreptimeDB](https://github.com/GreptimeTeam/greptimedb).

> **Warning**
> This project is under heavy development. APIs may change without notice.
> Use at your own risk in production environments.

## Installation

```bash
dotnet add package GreptimeDB.Ingester
```

## Quick Start

```csharp
using GreptimeDB.Ingester.Client;
using GreptimeDB.Ingester.Table;
using GreptimeDB.Ingester.Types;

// Create client
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoint = "http://localhost:4001",
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
    Endpoint = "http://localhost:4001",
    Database = "public",
    ConnectTimeout = TimeSpan.FromSeconds(5),
    WriteTimeout = TimeSpan.FromSeconds(30)
});
```

With basic auth:

```csharp
var client = new GreptimeClient(new GreptimeClientOptions
{
    Endpoint = "http://localhost:4001",
    Database = "public",
    Authentication = new AuthenticationOptions
    {
        Username = "greptime_user",
        Password = "greptime_password"
    }
});
```

## Features

- **Unary Write** - Simple single-request writes via gRPC
- **Streaming Write** - High-throughput streaming via gRPC for multiple tables
- **Bulk Write** - Maximum throughput via Apache Arrow Flight
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
    options.Endpoint = "http://localhost:4001";
    options.Database = "public";
});
```

## License

Apache License 2.0
