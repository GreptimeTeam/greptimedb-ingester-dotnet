using System.Text;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using GreptimeDB.Ingester.Arrow;
using GreptimeDB.Ingester.Exceptions;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// A bulk writer for high-throughput data ingestion via Arrow Flight.
/// </summary>
public sealed partial class BulkWriter : IBulkWriter
{
    private readonly FlightClient _flightClient;
    private readonly string _database;
    private readonly AuthenticationOptions? _auth;
    private readonly ILogger _logger;
    private readonly RecordBatchBuilder _recordBatchBuilder;

    private FlightRecordBatchDuplexStreamingCall? _putCall;
    private string? _currentTableName;
    private uint _totalRowsWritten;
    private int _completed;
    private int _disposed;

    /// <summary>
    /// Creates a new BulkWriter.
    /// </summary>
    internal BulkWriter(
        FlightClient flightClient,
        string database,
        AuthenticationOptions? auth,
        ILogger? logger = null)
    {
        _flightClient = flightClient;
        _database = database;
        _auth = auth;
        _logger = logger ?? NullLogger.Instance;
        _recordBatchBuilder = new RecordBatchBuilder();

        LogBulkWriterCreated(_logger);
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(Table.Table table, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ThrowIfCompleted();

        // Build the RecordBatch from the table
        using var recordBatch = _recordBatchBuilder.Build(table);

        // Initialize the stream on first write
        if (_putCall == null)
        {
            await InitializeStreamAsync(table.Name, recordBatch.Schema, cancellationToken)
                .ConfigureAwait(false);
            _currentTableName = table.Name;
        }
        else if (_currentTableName != table.Name)
        {
            throw new InvalidOperationException(
                $"BulkWriter is bound to table '{_currentTableName}'. " +
                $"Cannot write to different table '{table.Name}'. " +
                "Create a new BulkWriter for each table.");
        }

        // Write the record batch. Note: gRPC/Arrow Flight do not support cancelling an individual
        // write once it has started; we only honor cancellation before beginning the write.
        cancellationToken.ThrowIfCancellationRequested();
        await _putCall!.RequestStream.WriteAsync(recordBatch).ConfigureAwait(false);

        // Track rows written (like Go ingester does)
        _totalRowsWritten += (uint)table.RowCount;

        LogRecordBatchWritten(_logger, table.Name, table.RowCount);
    }

    /// <inheritdoc />
    public async ValueTask<uint> CompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            throw new InvalidOperationException("CompleteAsync has already been called.");
        }

        if (_putCall == null)
        {
            // No data was written
            return 0;
        }

        LogBulkWriteCompleting(_logger);

        try
        {
            // Complete the request stream
            await _putCall.RequestStream.CompleteAsync().ConfigureAwait(false);

            // Read and consume the response stream (required to complete the RPC)
            while (await _putCall.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                // GreptimeDB doesn't return meaningful affected rows count in Arrow Flight response.
                // Like the Go ingester, we return the count of rows we sent.
            }

            LogBulkWriteCompleted(_logger, _totalRowsWritten);
            return _totalRowsWritten;
        }
        catch (RpcException ex)
        {
            LogBulkWriteError(_logger, ex.Message);
            throw new GreptimeException($"Bulk write failed: {ex.Message}", ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _putCall?.Dispose();
        _recordBatchBuilder.Dispose();

        LogBulkWriterDisposed(_logger);

        await ValueTask.CompletedTask;
    }

    private async Task InitializeStreamAsync(
        string tableName,
        Schema schema,
        CancellationToken cancellationToken)
    {
        // Create the FlightDescriptor with the table name
        var descriptor = FlightDescriptor.CreatePathDescriptor(tableName);

        // Build headers for authentication
        var headers = new Metadata();
        if (_auth?.IsConfigured == true)
        {
            var credentials = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{_auth.Username}:{_auth.Password}"));
            headers.Add("authorization", $"Basic {credentials}");
        }
        headers.Add("x-greptime-db-name", _database);

        // Start the DoPut call with schema
        _putCall = await _flightClient.StartPut(descriptor, schema, headers, deadline: null, cancellationToken)
            .ConfigureAwait(false);

        LogStreamInitialized(_logger, tableName);
    }

    private void ThrowIfDisposed()
    {
#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
#else
        if (_disposed == 1)
        {
            throw new ObjectDisposedException(nameof(BulkWriter));
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

    [LoggerMessage(Level = LogLevel.Debug, Message = "BulkWriter created")]
    private static partial void LogBulkWriterCreated(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stream initialized for table {TableName}")]
    private static partial void LogStreamInitialized(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Trace, Message = "RecordBatch written for table {TableName} with {RowCount} rows")]
    private static partial void LogRecordBatchWritten(ILogger logger, string tableName, int rowCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Bulk write completing")]
    private static partial void LogBulkWriteCompleting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Bulk write completed, affected rows: {AffectedRows}")]
    private static partial void LogBulkWriteCompleted(ILogger logger, uint affectedRows);

    [LoggerMessage(Level = LogLevel.Error, Message = "Bulk write error: {ErrorMessage}")]
    private static partial void LogBulkWriteError(ILogger logger, string errorMessage);

    [LoggerMessage(Level = LogLevel.Debug, Message = "BulkWriter disposed")]
    private static partial void LogBulkWriterDisposed(ILogger logger);

    #endregion
}
