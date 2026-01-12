namespace GreptimeDB.Ingester.Exceptions;

/// <summary>
/// Thrown when validation fails (schema, naming, etc.).
/// </summary>
public sealed class ValidationException : GreptimeException
{
    /// <summary>
    /// Initializes a new instance of <see cref="ValidationException"/>.
    /// </summary>
    /// <param name="message">The validation error message.</param>
    public ValidationException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ValidationException"/> with an inner exception.
    /// </summary>
    /// <param name="message">The validation error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
