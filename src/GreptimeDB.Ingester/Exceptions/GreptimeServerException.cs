namespace GreptimeDB.Ingester.Exceptions;

/// <summary>
/// Business-level error returned by GreptimeDB in a successful gRPC response.
/// </summary>
public sealed class GreptimeServerException : GreptimeException
{
    /// <summary>
    /// Initializes a new instance of <see cref="GreptimeServerException"/>.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="statusCode">The GreptimeDB server status code.</param>
    public GreptimeServerException(string message, uint statusCode)
        : base(message)
    {
        StatusCode = statusCode;
    }

    /// <summary>
    /// Gets the GreptimeDB server status code.
    /// </summary>
    public uint StatusCode { get; }
}
