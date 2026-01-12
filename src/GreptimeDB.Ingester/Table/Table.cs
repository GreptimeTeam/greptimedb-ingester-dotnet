using GreptimeDB.Ingester.Types;

namespace GreptimeDB.Ingester.Table;

/// <summary>
/// Represents a table with schema and row data for writing to GreptimeDB.
/// Immutable after construction via TableBuilder.
/// </summary>
public sealed class Table
{
    /// <summary>
    /// Gets the sanitized table name (snake_case, max 99 chars).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the original table name before sanitization.
    /// </summary>
    public string OriginalName { get; }

    /// <summary>
    /// Gets the column definitions.
    /// </summary>
    public IReadOnlyList<Column> Columns { get; }

    /// <summary>
    /// Gets the row data. Each row is an array of values matching the column order.
    /// </summary>
    public IReadOnlyList<object?[]> Rows { get; }

    /// <summary>
    /// Gets the number of rows in the table.
    /// </summary>
    public int RowCount => Rows.Count;

    /// <summary>
    /// Gets the number of columns in the table.
    /// </summary>
    public int ColumnCount => Columns.Count;

    internal Table(
        string name,
        string originalName,
        IReadOnlyList<Column> columns,
        IReadOnlyList<object?[]> rows)
    {
        Name = name;
        OriginalName = originalName;
        Columns = columns;
        Rows = rows;
    }

    /// <summary>
    /// Gets the index of a column by name.
    /// </summary>
    /// <param name="columnName">The column name (original or sanitized).</param>
    /// <returns>The column index, or -1 if not found.</returns>
    public int GetColumnIndex(string columnName)
    {
        for (var i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Name == columnName || Columns[i].OriginalName == columnName)
            {
                return i;
            }
        }
        return -1;
    }

    /// <summary>
    /// Gets a column by name.
    /// </summary>
    /// <param name="columnName">The column name (original or sanitized).</param>
    /// <returns>The column, or null if not found.</returns>
    public Column? GetColumn(string columnName)
    {
        var index = GetColumnIndex(columnName);
        return index >= 0 ? Columns[index] : null;
    }

    /// <inheritdoc />
    public override string ToString() => $"Table '{Name}' ({ColumnCount} columns, {RowCount} rows)";
}
