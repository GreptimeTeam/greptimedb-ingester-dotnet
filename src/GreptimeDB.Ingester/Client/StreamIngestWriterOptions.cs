namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Configuration options for <see cref="StreamIngestWriter"/>.
/// </summary>
public sealed class StreamIngestWriterOptions
{
    /// <summary>
    /// The maximum number of tables that can be buffered before backpressure is applied.
    /// When the buffer is full, <see cref="IStreamIngestWriter.WriteAsync"/> will wait
    /// until space becomes available.
    /// Default: 1000.
    /// </summary>
    public int BufferCapacity { get; set; } = 1000;

    /// <summary>
    /// The timeout for completing the stream write operation.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan WriteTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Validates the options and throws if any values are invalid.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when a value is out of valid range.</exception>
    public void Validate()
    {
        if (BufferCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BufferCapacity),
                BufferCapacity,
                "Buffer capacity must be greater than zero.");
        }

        if (WriteTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WriteTimeout),
                WriteTimeout,
                "Write timeout must be greater than zero.");
        }
    }
}
