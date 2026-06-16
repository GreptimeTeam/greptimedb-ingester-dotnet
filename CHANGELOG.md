# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-06-16

### Added

- `GreptimeClientOptions.Failover` (`FailoverOptions`) to control request-level
  failover for multi-endpoint clients. Unary write/delete requests are retried
  against another endpoint after safe transient transport failures or retryable
  GreptimeDB server status codes. Knobs: `Enabled`, `MaxAttempts`,
  `ConsecutiveFailuresBeforeEjection`, `BaseEjectionDelay`, `MaxEjectionDelay`.
  Endpoints that fail repeatedly are ejected from selection with exponential
  backoff and reinstated automatically.
- `GreptimeServerException`, surfacing the GreptimeDB server status code returned
  in the response trailer so callers can distinguish retryable from terminal
  server errors.

### Changed

- Reworked multi-endpoint handling. Replaced the `Grpc.Net.Client.Balancer`
  round-robin channel with a client-side endpoint selector that performs
  request-level failover. Only unary writes/deletes are replayed; client-side
  write deadlines are not replayed because the outcome is ambiguous. Streaming
  and bulk writers are not replayed automatically — their final transport
  outcome updates endpoint health so the next writer can pick a healthy
  endpoint. `LoadBalancing` (`Random` / `RoundRobin`) still selects the
  per-request endpoint.
- Duplicate entries in `Endpoints` are now rejected during validation.

## [0.2.0] - 2026-04-21

### Added

- Multi-endpoint support via `GreptimeClientOptions.Endpoints` (`IList<string>`).
  Supplying more than one endpoint enables client-side load balancing with
  automatic failover across endpoints. Single-element lists behave as the
  previous single-node case. Backed by `Grpc.Net.Client.Balancer`.
- `GreptimeClientOptions.LoadBalancing` (`LoadBalancingStrategy`) selects the
  multi-endpoint balancing policy. Supported: `Random` (default — picks a
  ready endpoint uniformly at random per call, avoiding the herding pattern
  that round-robin can produce when many short-lived clients start at the
  same time) and `RoundRobin`.

### Changed

- **BREAKING:** Dropped `net6.0` and `net7.0` target frameworks. Minimum
  supported runtime is now `net8.0`. Both removed TFMs are past Microsoft's
  end-of-support, and `Grpc.Net.Client.Balancer` (required for the new
  multi-endpoint client-side load balancer) is only shipped in the package's
  `net8.0+` build, not its `netstandard2.1` build.
  Users on `net6.0` / `net7.0` should pin to the `0.1.x` line, which keeps
  those TFMs supported.

### Deprecated

- `GreptimeClientOptions.Endpoint` (single-endpoint string). Use
  `GreptimeClientOptions.Endpoints` instead. The property is retained for
  backward compatibility and will be removed in a future release.
