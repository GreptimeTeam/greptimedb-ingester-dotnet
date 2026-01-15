using System.Threading.Channels;
using Greptime.V1;
using GreptimeDB.Ingester.Exceptions;
using GreptimeDB.Ingester.Internal;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// A thread-safe streaming writer for ingesting data into GreptimeDB.
/// Uses a bounded channel for backpressure and a background task for sending.
/// </summary>
public sealed partial class StreamIngestWriter : IStreamIngestWriter
{
    private readonly GreptimeDatabase.GreptimeDatabaseClient _client;
    private readonly StreamIngestWriterOptions _options;
    private readonly Func<RequestHeader> _headerFactory;
    private readonly ILogger<StreamIngestWriter> _logger;

    private readonly Channel<GreptimeRequest> _channel;
    private readonly Task<uint> _sendTask;
    private readonly CancellationTokenSource _cts;

    private int _completed; // 0 = not completed, 1 = completed
    private int _disposed;  // 0 = not disposed, 1 = disposed

    /// <summary>
    /// Creates a new StreamIngestWriter.
    /// </summary>
    internal StreamIngestWriter(
        GreptimeDatabase.GreptimeDatabaseClient client,
        StreamIngestWriterOptions options,
        Func<RequestHeader> headerFactory,
        ILogger<StreamIngestWriter>? logger = null)
    {
        options.Validate();

        _client = client;
        _options = options;
        _headerFactory = headerFactory;
        _logger = logger ?? NullLogger<StreamIngestWriter>.Instance;
        _cts = new CancellationTokenSource();

        var channelOptions = new BoundedChannelOptions(options.BufferCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };
        _channel = Channel.CreateBounded<GreptimeRequest>(channelOptions);

        // Start the background send task
        _sendTask = Task.Run(() => SendLoopAsync(_cts.Token));

        LogStreamWriterCreated(_logger, options.BufferCapacity);
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(Table.Table table, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ThrowIfCompleted();

        var request = BuildRequest(table);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, _cts.Token);

        await _channel.Writer.WriteAsync(request, linkedCts.Token).ConfigureAwait(false);

        LogTableEnqueued(_logger, table.Name, table.RowCount);
    }

    /// <inheritdoc />
    public bool TryWrite(Table.Table table)
    {
        ThrowIfDisposed();
        ThrowIfCompleted();

        var request = BuildRequest(table);
        var result = _channel.Writer.TryWrite(request);

        if (result)
        {
            LogTableEnqueued(_logger, table.Name, table.RowCount);
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask<uint> CompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            throw new InvalidOperationException("CompleteAsync has already been called.");
        }

        // Signal that no more items will be written
        _channel.Writer.Complete();

        LogStreamCompleting(_logger);

        try
        {
            // Wait for the send task to complete with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_options.WriteTimeout);

            var affectedRows = await _sendTask.WaitAsync(timeoutCts.Token).ConfigureAwait(false);

            LogStreamCompleted(_logger, affectedRows);
            return affectedRows;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(
                $"Stream write operation timed out after {_options.WriteTimeout.TotalSeconds} seconds.");
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        // Cancel the background task if not already completed
#if NET8_0_OR_GREATER
        await _cts.CancelAsync().ConfigureAwait(false);
#else
        _cts.Cancel();
#endif

        // Mark the channel as complete to unblock the reader
        _channel.Writer.TryComplete();

        // Wait for the send task to complete (with a reasonable timeout)
        try
        {
            await _sendTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch
        {
            // Ignore exceptions during disposal
        }

        _cts.Dispose();

        LogStreamWriterDisposed(_logger);
    }

    private async Task<uint> SendLoopAsync(CancellationToken cancellationToken)
    {
        // Note: Don't set a deadline here - streaming operations may run for extended periods.
        // Timeout is handled in CompleteAsync which waits for this task to finish.
        var callOptions = new CallOptions(cancellationToken: cancellationToken);

        using var call = _client.HandleRequests(callOptions);

        try
        {
            var requestStream = call.RequestStream;

            await foreach (var request in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await requestStream.WriteAsync(request, cancellationToken).ConfigureAwait(false);
                LogRequestSent(_logger);
            }

            // Complete the request stream
            await requestStream.CompleteAsync().ConfigureAwait(false);

            // Wait for the response
            var response = await call.ResponseAsync.ConfigureAwait(false);

            CheckResponse(response);

            return response.AffectedRows?.Value ?? 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            LogStreamCancelled(_logger);
            throw;
        }
        catch (RpcException ex)
        {
            LogStreamError(_logger, ex.Message);
            throw new GreptimeException($"Stream write failed: {ex.Message}", ex);
        }
    }

    private GreptimeRequest BuildRequest(Table.Table table)
    {
        var rowInserts = RequestBuilder.BuildRowInsertRequests(new[] { table });
        return new GreptimeRequest
        {
            Header = _headerFactory(),
            RowInserts = rowInserts
        };
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
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
#else
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(StreamIngestWriter));
        }
#endif
    }

    private void ThrowIfCompleted()
    {
        if (_completed == 1)
        {
            throw new InvalidOperationException(
                "Cannot write after CompleteAsync has been called.");
        }
    }

    #region Logging

    [LoggerMessage(Level = LogLevel.Debug, Message = "StreamIngestWriter created with buffer capacity {Capacity}")]
    private static partial void LogStreamWriterCreated(ILogger logger, int capacity);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Table {TableName} enqueued with {RowCount} rows")]
    private static partial void LogTableEnqueued(ILogger logger, string tableName, int rowCount);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Request sent to stream")]
    private static partial void LogRequestSent(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stream completing, waiting for pending writes")]
    private static partial void LogStreamCompleting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stream completed, affected rows: {AffectedRows}")]
    private static partial void LogStreamCompleted(ILogger logger, uint affectedRows);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stream cancelled")]
    private static partial void LogStreamCancelled(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "Stream error: {ErrorMessage}")]
    private static partial void LogStreamError(ILogger logger, string errorMessage);

    [LoggerMessage(Level = LogLevel.Debug, Message = "StreamIngestWriter disposed")]
    private static partial void LogStreamWriterDisposed(ILogger logger);

    #endregion
}
