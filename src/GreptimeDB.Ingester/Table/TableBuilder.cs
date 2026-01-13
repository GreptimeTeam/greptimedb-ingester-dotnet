using GreptimeDB.Ingester.Exceptions;
using GreptimeDB.Ingester.Internal;
using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Table;

/// <summary>
/// Fluent builder for constructing Table instances.
/// </summary>
public sealed class TableBuilder
{
    private readonly string _tableName;
    private readonly string _originalTableName;
    private readonly List<Column> _columns = new();
    private readonly List<object?[]> _rows = new();
    private bool _hasTimestamp;

    /// <summary>
    /// Creates a new TableBuilder with the specified table name.
    /// </summary>
    /// <param name="tableName">The table name (will be sanitized to snake_case).</param>
    public TableBuilder(string tableName)
    {
        _originalTableName = tableName;
        _tableName = NameSanitizer.Sanitize(tableName);
    }

    /// <summary>
    /// Adds a Tag column.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="dataType">Data type (defaults to String).</param>
    /// <returns>This builder for chaining.</returns>
    public TableBuilder AddTag(string name, ColumnDataType dataType = ColumnDataType.String)
    {
        return AddColumn(name, dataType, SemanticType.Tag);
    }

    /// <summary>
    /// Adds a Field column.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="dataType">Data type.</param>
    /// <returns>This builder for chaining.</returns>
    public TableBuilder AddField(string name, ColumnDataType dataType)
    {
        return AddColumn(name, dataType, SemanticType.Field);
    }

    /// <summary>
    /// Adds the Timestamp column.
    /// </summary>
    /// <param name="name">Column name (defaults to "ts").</param>
    /// <param name="dataType">Timestamp precision (defaults to TimestampMillisecond).</param>
    /// <returns>This builder for chaining.</returns>
    /// <exception cref="ValidationException">Thrown if a timestamp column already exists.</exception>
    public TableBuilder AddTimestamp(
        string name = "ts",
        ColumnDataType dataType = ColumnDataType.TimestampMillisecond)
    {
        if (_hasTimestamp)
        {
            throw new ValidationException("Table can only have one timestamp column.");
        }

        if (!dataType.IsTimestamp())
        {
            throw new ValidationException(
                $"Timestamp column must use a timestamp data type, got {dataType}.");
        }

        _hasTimestamp = true;
        return AddColumn(name, dataType, SemanticType.Timestamp);
    }

    /// <summary>
    /// Adds a column with explicit semantic type.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="dataType">Data type.</param>
    /// <param name="semanticType">Semantic type.</param>
    /// <returns>This builder for chaining.</returns>
    public TableBuilder AddColumn(string name, ColumnDataType dataType, SemanticType semanticType)
    {
        var sanitizedName = NameSanitizer.Sanitize(name);

        // Check for duplicate column names
        if (_columns.Any(c => c.Name == sanitizedName))
        {
            throw new ValidationException($"Duplicate column name: '{sanitizedName}'.");
        }

        if (semanticType == SemanticType.Timestamp)
        {
            if (_hasTimestamp && _columns.All(c => c.SemanticType != SemanticType.Timestamp))
            {
                // _hasTimestamp was set but no column yet, this is fine
            }
            else if (_columns.Any(c => c.SemanticType == SemanticType.Timestamp))
            {
                throw new ValidationException("Table can only have one timestamp column.");
            }
            _hasTimestamp = true;
        }

        _columns.Add(new Column(sanitizedName, name, dataType, semanticType));
        return this;
    }

    /// <summary>
    /// Adds a row of values. Values must match column order.
    /// </summary>
    /// <param name="values">Values for each column.</param>
    /// <returns>This builder for chaining.</returns>
    /// <exception cref="ValidationException">Thrown if value count doesn't match column count.</exception>
    public TableBuilder AddRow(params object?[] values)
    {
        if (values.Length != _columns.Count)
        {
            throw new ValidationException(
                $"Row has {values.Length} values but table has {_columns.Count} columns.");
        }

        // Coerce values to target types
        var coercedValues = new object?[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            coercedValues[i] = TypeCoercion.Coerce(
                values[i],
                _columns[i].DataType,
                _columns[i].Name);
        }

        _rows.Add(coercedValues);
        return this;
    }

    /// <summary>
    /// Gets the current column definitions.
    /// Useful for inspecting the schema before building.
    /// </summary>
    /// <returns>Read-only list of column definitions.</returns>
    public IReadOnlyList<Column> GetColumnDefinitions()
    {
        return _columns.AsReadOnly();
    }

    /// <summary>
    /// Builds the immutable Table instance.
    /// </summary>
    /// <returns>The constructed Table.</returns>
    /// <exception cref="ValidationException">Thrown if the table has no columns or no timestamp.</exception>
    public Table Build()
    {
        if (_columns.Count == 0)
        {
            throw new ValidationException("Table must have at least one column.");
        }

        if (!_hasTimestamp)
        {
            throw new ValidationException("Table must have a timestamp column.");
        }

        return new Table(
            _tableName,
            _originalTableName,
            _columns.AsReadOnly(),
            _rows.AsReadOnly());
    }
}
