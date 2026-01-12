namespace GreptimeDB.Ingester.Exceptions;

/// <summary>
/// Base exception for all GreptimeDB SDK errors.
/// </summary>
public class GreptimeException : Exception
{
    /// <summary>
    /// Initializes a new instance of <see cref="GreptimeException"/>.
    /// </summary>
    public GreptimeException()
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="GreptimeException"/> with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public GreptimeException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="GreptimeException"/> with a message and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public GreptimeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
