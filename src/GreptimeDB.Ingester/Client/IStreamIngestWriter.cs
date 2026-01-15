namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Interface for streaming data ingestion into GreptimeDB.
/// Supports thread-safe concurrent writes with automatic backpressure handling.
/// </summary>
public interface IStreamIngestWriter : IAsyncDisposable
{
    /// <summary>
    /// Writes a table to the stream asynchronously.
    /// This method is thread-safe and supports concurrent calls from multiple threads.
    /// When the internal buffer is full, this method will wait until space becomes available.
    /// </summary>
    /// <param name="table">The table to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the table has been enqueued.</returns>
    ValueTask WriteAsync(Table.Table table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to write a table to the stream without blocking.
    /// This method is thread-safe and supports concurrent calls from multiple threads.
    /// </summary>
    /// <param name="table">The table to write.</param>
    /// <returns>True if the table was enqueued successfully, false if the buffer is full.</returns>
    bool TryWrite(Table.Table table);

    /// <summary>
    /// Completes the stream and waits for all pending writes to be sent to the server.
    /// Returns the total number of affected rows.
    /// This method should be called exactly once before disposing the writer.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The total number of affected rows.</returns>
    ValueTask<uint> CompleteAsync(CancellationToken cancellationToken = default);
}
