using Apache.Arrow.Flight.Client;
using Greptime.V1;
using GreptimeDB.Ingester.Exceptions;
using GreptimeDB.Ingester.Internal;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Client for writing data to GreptimeDB via gRPC.
/// </summary>
public sealed partial class GreptimeClient : IAsyncDisposable, IDisposable
{
    private readonly GreptimeClientOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly Dictionary<string, EndpointConnection> _connections;
    private readonly EndpointSelector _endpointSelector;
    private bool _disposed;

    /// <summary>
    /// Creates a new GreptimeClient with the specified options.
    /// </summary>
    /// <param name="options">Client configuration options.</param>
    /// <param name="loggerFactory">Optional logger factory for creating category-specific loggers.</param>
    public GreptimeClient(GreptimeClientOptions options, ILoggerFactory? loggerFactory = null)
    {
        options.Validate();
        _options = options;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<GreptimeClient>();

        var endpoints = options.ResolveEndpoints();
        _connections = endpoints.ToDictionary(
            endpoint => endpoint,
            endpoint => new EndpointConnection(endpoint));
        _endpointSelector = new EndpointSelector(endpoints, options.LoadBalancing, options.Failover);

        LogClientCreated(_logger, endpoints.Count, endpoints[0]);
    }

    /// <summary>
    /// Writes one or more tables to GreptimeDB.
    /// </summary>
    /// <param name="tables">The tables to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<uint> WriteAsync(
        IEnumerable<Table.Table> tables,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var tableList = tables as IList<Table.Table> ?? tables.ToList();
        if (tableList.Count == 0)
        {
            return 0;
        }

        var rowInserts = RequestBuilder.BuildRowInsertRequests(tableList);
        var request = new GreptimeRequest
        {
            Header = BuildRequestHeader(),
            RowInserts = rowInserts
        };

        var totalRows = tableList.Sum(t => t.RowCount);
        LogWriteStarted(_logger, tableList.Count, totalRows);

        var response = await ExecuteDatabaseRequestAsync(request, cancellationToken).ConfigureAwait(false);

        CheckResponse(response);

        var affectedRows = response.AffectedRows?.Value ?? 0;
        LogWriteCompleted(_logger, affectedRows);

        return affectedRows;
    }

    /// <summary>
    /// Writes a single table to GreptimeDB.
    /// </summary>
    /// <param name="table">The table to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public Task<uint> WriteAsync(Table.Table table, CancellationToken cancellationToken = default)
    {
        return WriteAsync(new[] { table }, cancellationToken);
    }

    /// <summary>
    /// Deletes rows from one or more tables based on tag and timestamp values.
    /// </summary>
    /// <param name="tables">The tables containing rows to delete (only Tag and Timestamp columns are used).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<uint> DeleteAsync(
        IEnumerable<Table.Table> tables,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var tableList = tables as IList<Table.Table> ?? tables.ToList();
        if (tableList.Count == 0)
        {
            return 0;
        }

        var rowDeletes = RequestBuilder.BuildRowDeleteRequests(tableList);
        var request = new GreptimeRequest
        {
            Header = BuildRequestHeader(),
            RowDeletes = rowDeletes
        };

        var totalRows = tableList.Sum(t => t.RowCount);
        LogDeleteStarted(_logger, tableList.Count, totalRows);

        var response = await ExecuteDatabaseRequestAsync(request, cancellationToken).ConfigureAwait(false);

        CheckResponse(response);

        var affectedRows = response.AffectedRows?.Value ?? 0;
        LogDeleteCompleted(_logger, affectedRows);

        return affectedRows;
    }

    /// <summary>
    /// Deletes rows from a single table.
    /// </summary>
    /// <param name="table">The table containing rows to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public Task<uint> DeleteAsync(Table.Table table, CancellationToken cancellationToken = default)
    {
        return DeleteAsync(new[] { table }, cancellationToken);
    }

    /// <summary>
    /// Creates a new streaming writer for high-throughput data ingestion.
    /// The writer supports concurrent writes from multiple threads and provides
    /// automatic backpressure handling.
    /// </summary>
    /// <param name="options">Optional configuration for the stream writer.</param>
    /// <returns>A new stream ingest writer instance.</returns>
    /// <example>
    /// <code>
    /// await using var writer = client.CreateStreamIngestWriter();
    ///
    /// // Write tables concurrently from multiple threads
    /// await writer.WriteAsync(table1);
    /// await writer.WriteAsync(table2);
    ///
    /// // Complete the stream and get the result
    /// var affectedRows = await writer.CompleteAsync();
    /// </code>
    /// </example>
    public IStreamIngestWriter CreateStreamIngestWriter(StreamIngestWriterOptions? options = null)
    {
        ThrowIfDisposed();

        options ??= new StreamIngestWriterOptions
        {
            WriteTimeout = _options.WriteTimeout
        };

        var endpoint = _endpointSelector.Select();
        var connection = GetConnection(endpoint);
        return new StreamIngestWriter(
            connection.DatabaseClient,
            options,
            BuildRequestHeader,
            error => _endpointSelector.ReportOutcome(endpoint, error),
            _loggerFactory.CreateLogger<StreamIngestWriter>());
    }

    /// <summary>
    /// Creates a new bulk writer for high-throughput data ingestion via Arrow Flight.
    /// The writer provides efficient columnar data transfer using Apache Arrow format.
    /// </summary>
    /// <returns>A new bulk writer instance.</returns>
    /// <example>
    /// <code>
    /// await using var writer = client.CreateBulkWriter();
    ///
    /// // Write tables
    /// await writer.WriteAsync(table1);
    /// await writer.WriteAsync(table2);
    ///
    /// // Complete the write and get the result
    /// var affectedRows = await writer.CompleteAsync();
    /// </code>
    /// </example>
    public IBulkWriter CreateBulkWriter()
    {
        ThrowIfDisposed();

        var endpoint = _endpointSelector.Select();
        var connection = GetConnection(endpoint);
        return new BulkWriter(
            connection.FlightClient,
            _options.Database,
            _options.Authentication,
            _options.WriteTimeout,
            error => _endpointSelector.ReportOutcome(endpoint, error),
            _loggerFactory.CreateLogger<BulkWriter>());
    }

    /// <summary>
    /// Writes a single table to GreptimeDB using Arrow Flight bulk write.
    /// This is a convenience method that creates a BulkWriter, writes the table,
    /// and completes the operation in a single call.
    /// </summary>
    /// <param name="table">The table to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public async Task<uint> BulkWriteAsync(Table.Table table, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        await using var writer = CreateBulkWriter();
        await writer.WriteAsync(table, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Checks the health of the GreptimeDB server.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the server is healthy, false otherwise.</returns>
    public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var endpoint = _endpointSelector.Select();
        try
        {
            var request = new HealthCheckRequest();
            var callOptions = new CallOptions(
                deadline: DateTime.UtcNow.Add(_options.ConnectTimeout),
                cancellationToken: cancellationToken);

            await GetConnection(endpoint).HealthClient.HealthCheckAsync(request, callOptions).ConfigureAwait(false);
            _endpointSelector.ReportSuccess(endpoint);
            return true;
        }
        catch (RpcException ex)
        {
            if (EndpointSelector.IsEndpointFailure(ex))
            {
                // HealthCheckAsync is observational and not part of the write
                // failover contract, so it does not spend retry attempts here.
                // The failure still feeds endpoint health for subsequent calls.
                _endpointSelector.ReportFailure(endpoint);
            }

            LogHealthCheckFailed(_logger, ex);
            return false;
        }
    }

    /// <summary>
    /// Closes the gRPC channel.
    /// </summary>
    public async Task CloseAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var connection in _connections.Values)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        LogClientClosed(_logger);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var connection in _connections.Values)
        {
            connection.Dispose();
        }
        LogClientDisposed(_logger);
    }

    private RequestHeader BuildRequestHeader()
    {
        var header = new RequestHeader
        {
            Dbname = _options.Database
        };

        if (_options.Authentication?.IsConfigured == true)
        {
            header.Authorization = new AuthHeader
            {
                Basic = new Basic
                {
                    Username = _options.Authentication.Username ?? string.Empty,
                    Password = _options.Authentication.Password ?? string.Empty
                }
            };
        }

        return header;
    }

    private async Task<GreptimeResponse> ExecuteDatabaseRequestAsync(
        GreptimeRequest request,
        CancellationToken cancellationToken)
    {
        var failedEndpoints = new HashSet<string>(StringComparer.Ordinal);
        Exception? lastEndpointFailure = null;
        var maxAttempts = _endpointSelector.MaxAttempts;
        var deadline = DateTime.UtcNow.Add(_options.WriteTimeout);

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var endpoint = _endpointSelector.Select(failedEndpoints);
            var connection = GetConnection(endpoint);
            try
            {
                var response = await connection.DatabaseClient
                    .HandleAsync(request, CreateCallOptions(deadline, cancellationToken))
                    .ConfigureAwait(false);
                _endpointSelector.ReportSuccess(endpoint);
                return response;
            }
            catch (RpcException ex) when (EndpointSelector.IsEndpointFailure(ex))
            {
                _endpointSelector.ReportFailure(endpoint);

                if (!IsRetryableUnaryWriteFailure(ex) || attempt == maxAttempts - 1)
                {
                    throw;
                }

                failedEndpoints.Add(endpoint);
                lastEndpointFailure = ex;
            }
        }

        throw lastEndpointFailure ?? new GreptimeException("No endpoint attempts were made.");
    }

    private EndpointConnection GetConnection(string endpoint)
    {
        return _connections[endpoint];
    }

    internal static bool IsRetryableUnaryWriteFailure(RpcException exception)
    {
        return exception.StatusCode is StatusCode.Unavailable
            or StatusCode.ResourceExhausted;
    }

    internal static CallOptions CreateCallOptions(DateTime deadline, CancellationToken cancellationToken)
    {
        return new CallOptions(
            deadline: deadline,
            cancellationToken: cancellationToken);
    }

    private sealed class EndpointConnection : IDisposable, IAsyncDisposable
    {
        private readonly Lazy<FlightClient> _flightClient;

        public EndpointConnection(string endpoint)
        {
            Channel = GrpcChannel.ForAddress(endpoint);
            DatabaseClient = new GreptimeDatabase.GreptimeDatabaseClient(Channel);
            HealthClient = new HealthCheck.HealthCheckClient(Channel);
            _flightClient = new Lazy<FlightClient>(() => new FlightClient(Channel));
        }

        public GrpcChannel Channel { get; }

        public GreptimeDatabase.GreptimeDatabaseClient DatabaseClient { get; }

        public HealthCheck.HealthCheckClient HealthClient { get; }

        public FlightClient FlightClient => _flightClient.Value;

        public async ValueTask DisposeAsync()
        {
            if (_flightClient.IsValueCreated)
            {
                switch (_flightClient.Value)
                {
                    case IAsyncDisposable asyncDisposable:
                        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        break;
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }

            await Channel.ShutdownAsync().ConfigureAwait(false);
            Channel.Dispose();
        }

        public void Dispose()
        {
            if (_flightClient.IsValueCreated)
            {
                switch (_flightClient.Value)
                {
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                    case IAsyncDisposable asyncDisposable:
                        asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                        break;
                }
            }

            Channel.Dispose();
        }
    }

    private static void CheckResponse(GreptimeResponse response)
    {
        var header = response.Header;
        if (header?.Status != null && header.Status.StatusCode != 0)
        {
            throw new GreptimeException(
                $"Request failed with status code {header.Status.StatusCode}: {header.Status.ErrMsg}");
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    #region Logging

    [LoggerMessage(Level = LogLevel.Debug, Message = "GreptimeClient created with {EndpointCount} endpoint(s); first: {FirstEndpoint}")]
    private static partial void LogClientCreated(ILogger logger, int endpointCount, string firstEndpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Writing {TableCount} tables with {RowCount} total rows")]
    private static partial void LogWriteStarted(ILogger logger, int tableCount, int rowCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Write completed, affected rows: {AffectedRows}")]
    private static partial void LogWriteCompleted(ILogger logger, uint affectedRows);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Deleting from {TableCount} tables with {RowCount} total rows")]
    private static partial void LogDeleteStarted(ILogger logger, int tableCount, int rowCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Delete completed, affected rows: {AffectedRows}")]
    private static partial void LogDeleteCompleted(ILogger logger, uint affectedRows);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Health check failed")]
    private static partial void LogHealthCheckFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GreptimeClient closed")]
    private static partial void LogClientClosed(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GreptimeClient disposed")]
    private static partial void LogClientDisposed(ILogger logger);

    #endregion
}
