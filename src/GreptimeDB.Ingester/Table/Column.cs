using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Table;

/// <summary>
/// Represents a column definition in a GreptimeDB table.
/// Immutable after construction.
/// </summary>
public sealed class Column
{
    /// <summary>
    /// Gets the sanitized column name (snake_case, max 99 chars).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the original column name before sanitization.
    /// </summary>
    public string OriginalName { get; }

    /// <summary>
    /// Gets the column data type.
    /// </summary>
    public ColumnDataType DataType { get; }

    /// <summary>
    /// Gets the semantic classification (Tag/Field/Timestamp).
    /// </summary>
    public SemanticType SemanticType { get; }

    internal Column(
        string name,
        string originalName,
        ColumnDataType dataType,
        SemanticType semanticType)
    {
        Name = name;
        OriginalName = originalName;
        DataType = dataType;
        SemanticType = semanticType;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Name} ({DataType}, {SemanticType})";
}
