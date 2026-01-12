using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Exceptions;

/// <summary>
/// Thrown when type coercion fails.
/// </summary>
public sealed class TypeMismatchException : GreptimeException
{
    /// <summary>
    /// Gets the column name where the type mismatch occurred.
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// Gets the source type that could not be converted.
    /// </summary>
    public Type SourceType { get; }

    /// <summary>
    /// Gets the target column data type.
    /// </summary>
    public ColumnDataType TargetType { get; }

    /// <summary>
    /// Initializes a new instance of <see cref="TypeMismatchException"/>.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="sourceType">The source type.</param>
    /// <param name="targetType">The target column data type.</param>
    public TypeMismatchException(string columnName, Type sourceType, ColumnDataType targetType)
        : base($"Cannot convert {sourceType.Name} to {targetType} for column '{columnName}'.")
    {
        ColumnName = columnName;
        SourceType = sourceType;
        TargetType = targetType;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="TypeMismatchException"/> with additional details.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="sourceType">The source type.</param>
    /// <param name="targetType">The target column data type.</param>
    /// <param name="details">Additional error details.</param>
    public TypeMismatchException(
        string columnName,
        Type sourceType,
        ColumnDataType targetType,
        string details)
        : base($"Cannot convert {sourceType.Name} to {targetType} for column '{columnName}': {details}")
    {
        ColumnName = columnName;
        SourceType = sourceType;
        TargetType = targetType;
    }
}
