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

Measured on Apple M1 Max with GreptimeDB v1.0 RC1 running locally (standalone mode).

#### gRPC Unary Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 100 | 10,000 | 0.19 | 53,918 |
| 1 | 1,000 | 10,000 | 0.05 | 199,112 |
| 1 | 5,000 | 10,000 | 0.03 | 325,781 |
| 4 | 500 | 40,000 | 0.05 | 815,327 |
| 8 | 500 | 80,000 | 0.08 | 998,317 |

#### gRPC Streaming Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 100 | 10,000 | 0.06 | 175,124 |
| 1 | 1,000 | 10,000 | 0.02 | 456,679 |
| 4 | 1,000 | 40,000 | 0.04 | 993,443 |
| 8 | 500 | 80,000 | 0.07 | 1,133,136 |
| 8 | 1,000 | 80,000 | 0.07 | 1,128,571 |

#### Arrow Flight Bulk Write

| Concurrency | Batch | Total Rows | Time (s) | Rows/s |
|:-----------:|------:|-----------:|---------:|-------:|
| 1 | 5,000 | 10,000 | 0.01 | 886,156 |
| 2 | 5,000 | 20,000 | 0.02 | 1,159,541 |
| 4 | 1,000 | 40,000 | 0.03 | 1,309,818 |
| 8 | 1,000 | 80,000 | 0.04 | **2,036,126** |
| 8 | 5,000 | 80,000 | 0.05 | 1,676,593 |

Arrow Flight achieves the highest throughput (~2M rows/s) at high concurrency with large batches due to efficient columnar data transfer.
