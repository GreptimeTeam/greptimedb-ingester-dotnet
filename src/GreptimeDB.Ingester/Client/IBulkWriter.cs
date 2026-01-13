namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Interface for bulk data ingestion into GreptimeDB via Arrow Flight.
/// Provides high-throughput data ingestion using Apache Arrow format.
/// </summary>
public interface IBulkWriter : IAsyncDisposable
{
    /// <summary>
    /// Writes a table to the bulk stream.
    /// The first call initializes the stream with the table's schema.
    /// Subsequent calls must use tables with the same schema.
    /// </summary>
    /// <param name="table">The table to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the table has been sent.</returns>
    ValueTask WriteAsync(Table.Table table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes the bulk write operation and returns the number of affected rows.
    /// This method must be called exactly once before disposing the writer.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The total number of affected rows.</returns>
    ValueTask<uint> CompleteAsync(CancellationToken cancellationToken = default);
}
