# Examples

Runnable examples using [.NET 10 file-based apps](https://learn.microsoft.com/dotnet/core/whats-new/dotnet-10#file-based-apps). Requires .NET 10 SDK.

## Prerequisites

- .NET 10 SDK
- A running GreptimeDB instance (gRPC on port 4001, MySQL on port 4002)

## Quick Test

A simple end-to-end test that writes 100 rows via gRPC and queries the results via MySQL protocol.

```bash
dotnet run examples/quick-test.cs
```

## Benchmark

Performance benchmark comparing all three write methods across different batch sizes and concurrency levels.

- **gRPC Unary Write** (`WriteAsync`)
- **gRPC Streaming Write** (`StreamIngestWriter`)
- **Arrow Flight Bulk Write** (`BulkWriteAsync`)

Each row is ~70 bytes (2 string tags, 5 float64 fields, 1 int32 field, 1 timestamp).

```bash
dotnet run examples/benchmark.cs
```

### Sample Results

Measured on Apple M1 Max with GreptimeDB v1.0 RC1 running locally (standalone mode). 100,000 rows per task.

#### gRPC Unary Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 500 | 100,000 | 0.31 | 324,057 |
| 1 | 5,000 | 100,000 | 0.23 | 430,482 |
| 4 | 1,000 | 400,000 | 0.42 | 956,373 |
| 8 | 500 | 800,000 | 0.91 | 876,400 |
| 8 | 1,000 | 800,000 | 0.90 | 890,292 |

#### gRPC Streaming Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 500 | 100,000 | 0.24 | 425,191 |
| 1 | 5,000 | 100,000 | 0.21 | 470,134 |
| 4 | 1,000 | 400,000 | 0.35 | 1,134,473 |
| 8 | 500 | 800,000 | 0.59 | 1,344,639 |
| 8 | 1,000 | 800,000 | 0.71 | 1,119,510 |

#### Arrow Flight Bulk Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 5,000 | 100,000 | 0.09 | 1,093,392 |
| 2 | 5,000 | 200,000 | 0.17 | 1,184,602 |
| 4 | 1,000 | 400,000 | 0.24 | 1,693,443 |
| 8 | 1,000 | 800,000 | 0.39 | **2,067,287** |
| 8 | 5,000 | 800,000 | 0.46 | 1,722,818 |

Arrow Flight achieves the highest throughput (~2M rows/s) at high concurrency with large batches due to efficient columnar data transfer.
