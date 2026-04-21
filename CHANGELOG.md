# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Multi-endpoint support via `GreptimeClientOptions.Endpoints` (`IList<string>`).
  Supplying more than one endpoint enables client-side round-robin load
  balancing with automatic failover across endpoints. Single-element lists
  behave as the previous single-node case. Backed by
  `Grpc.Net.Client.Balancer`.

### Changed

- **BREAKING:** Dropped `net6.0` and `net7.0` target frameworks. Minimum
  supported runtime is now `net8.0`. Both removed TFMs are past Microsoft's
  end-of-support, and `Grpc.Net.Client.Balancer` (required for the new
  multi-endpoint client-side load balancer) is only shipped in the package's
  `net8.0+` build, not its `netstandard2.1` build.

### Deprecated

- `GreptimeClientOptions.Endpoint` (single-endpoint string). Use
  `GreptimeClientOptions.Endpoints` instead. The property is retained for
  backward compatibility and will be removed in a future release.
