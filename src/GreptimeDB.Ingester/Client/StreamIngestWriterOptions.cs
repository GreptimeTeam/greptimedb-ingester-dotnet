using GreptimeDB.Ingester.Internal;

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
    /// Request hints sent as the <c>x-greptime-hints</c> header when the stream is opened,
    /// for example <c>append_mode=true</c>. They apply to every write on the stream.
    /// Default: none.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Hints { get; set; }

    /// <summary>
    /// Validates the options and throws if any values are invalid.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when a value is out of valid range.</exception>
    /// <exception cref="ArgumentException">Thrown when a hint key or value cannot be encoded.</exception>
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

        if (Hints is not null)
        {
            RequestHints.Validate(Hints, nameof(Hints));
        }
    }
}
