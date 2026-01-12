# GreptimeDB .NET Ingester

[![CI](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/GreptimeDB.Ingester.svg)](https://www.nuget.org/packages/GreptimeDB.Ingester)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![.NET](https://img.shields.io/badge/.NET-6.0%20%7C%207.0%20%7C%208.0%20%7C%209.0-purple)

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

## Features

- Unary write via gRPC
- Type coercion between .NET and GreptimeDB types
- Health check
- DI integration

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
