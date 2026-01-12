namespace GreptimeDB.Ingester.Types;

/// <summary>
/// Column semantic classification in GreptimeDB.
/// </summary>
public enum SemanticType
{
    /// <summary>
    /// Tag columns are indexed for filtering.
    /// Used for dimensions/labels in time-series data.
    /// </summary>
    Tag = 0,

    /// <summary>
    /// Field columns store metric values.
    /// Used for measured values in time-series data.
    /// </summary>
    Field = 1,

    /// <summary>
    /// Timestamp column. Exactly one required per table.
    /// </summary>
    Timestamp = 2,
}
