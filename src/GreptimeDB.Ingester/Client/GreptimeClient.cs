using Greptime.V1;
using Grpc.Core;
using Grpc.Net.Client;
using GreptimeDB.Ingester.Exceptions;
using GreptimeDB.Ingester.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Client for writing data to GreptimeDB via gRPC.
/// </summary>
public sealed partial class GreptimeClient : IAsyncDisposable, IDisposable
{
    private readonly GreptimeClientOptions _options;
    private readonly ILogger<GreptimeClient> _logger;
    private readonly GrpcChannel _channel;
    private readonly GreptimeDatabase.GreptimeDatabaseClient _client;
    private readonly HealthCheck.HealthCheckClient _healthClient;
    private bool _disposed;

    /// <summary>
    /// Creates a new GreptimeClient with the specified options.
    /// </summary>
    /// <param name="options">Client configuration options.</param>
    /// <param name="logger">Optional logger.</param>
    public GreptimeClient(GreptimeClientOptions options, ILogger<GreptimeClient>? logger = null)
    {
        options.Validate();
        _options = options;
        _logger = logger ?? NullLogger<GreptimeClient>.Instance;

        var channelOptions = new GrpcChannelOptions();

        _channel = GrpcChannel.ForAddress(options.Endpoint, channelOptions);
        _client = new GreptimeDatabase.GreptimeDatabaseClient(_channel);
        _healthClient = new HealthCheck.HealthCheckClient(_channel);

        LogClientCreated(_logger, options.Endpoint);
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

        var callOptions = CreateCallOptions(cancellationToken);
        var response = await _client.HandleAsync(request, callOptions).ConfigureAwait(false);

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

        var callOptions = CreateCallOptions(cancellationToken);
        var response = await _client.HandleAsync(request, callOptions).ConfigureAwait(false);

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
    /// Checks the health of the GreptimeDB server.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the server is healthy, false otherwise.</returns>
    public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        try
        {
            var request = new HealthCheckRequest();
            var callOptions = new CallOptions(
                deadline: DateTime.UtcNow.Add(_options.ConnectTimeout),
                cancellationToken: cancellationToken);

            await _healthClient.HealthCheckAsync(request, callOptions).ConfigureAwait(false);
            return true;
        }
        catch (RpcException ex)
        {
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
        await _channel.ShutdownAsync().ConfigureAwait(false);
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
        _channel.Dispose();
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

    private CallOptions CreateCallOptions(CancellationToken cancellationToken)
    {
        return new CallOptions(
            deadline: DateTime.UtcNow.Add(_options.WriteTimeout),
            cancellationToken: cancellationToken);
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
#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(GreptimeClient));
        }
#endif
    }

    #region Logging

    [LoggerMessage(Level = LogLevel.Debug, Message = "GreptimeClient created for endpoint {Endpoint}")]
    private static partial void LogClientCreated(ILogger logger, string endpoint);

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
