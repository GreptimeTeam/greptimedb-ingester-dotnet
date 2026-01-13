using System.Text;
using System.Text.RegularExpressions;

namespace GreptimeDB.Ingester.Internal;

/// <summary>
/// Sanitizes table and column names to GreptimeDB conventions.
/// </summary>
internal static class NameSanitizer
{
    private const int MaxLength = 99;

    private static readonly Regex MultipleUnderscoreRegex = new(@"_+", RegexOptions.Compiled);
    private static readonly Regex ValidIdentifierRegex = new(@"^[a-z_][a-z0-9_]*$", RegexOptions.Compiled);

    /// <summary>
    /// Sanitize name to snake_case lowercase, max 99 chars.
    /// </summary>
    /// <param name="name">The name to sanitize.</param>
    /// <returns>The sanitized name.</returns>
    /// <exception cref="ArgumentException">Thrown when name is null, empty, or cannot be sanitized.</exception>
    public static string Sanitize(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Name cannot be null or empty.", nameof(name));
        }

        var trimmed = name.Trim();
        var snakeCase = ToSnakeCase(trimmed);
        var lower = snakeCase.ToLowerInvariant();

        if (lower.Length > MaxLength)
        {
            lower = lower[..MaxLength];
        }

        if (!IsValidIdentifier(lower))
        {
            throw new ArgumentException(
                $"Name '{name}' cannot be sanitized to a valid identifier. Result: '{lower}'",
                nameof(name));
        }

        return lower;
    }

    private static string ToSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            return input;
        }

        var sb = new StringBuilder(input.Length + 10);

        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];

            if (char.IsUpper(c))
            {
                // Add underscore before uppercase if:
                // - Not at start
                // - Previous char was not uppercase (handles "XMLParser" -> "xml_parser")
                // - Or next char is lowercase (handles "XMLParser" -> "xml_parser")
                if (i > 0)
                {
                    var prevIsUpper = char.IsUpper(input[i - 1]);
                    var nextIsLower = i + 1 < input.Length && char.IsLower(input[i + 1]);

                    if (!prevIsUpper || nextIsLower)
                    {
                        sb.Append('_');
                    }
                }
                sb.Append(char.ToLowerInvariant(c));
            }
            else if (c is ' ' or '-')
            {
                sb.Append('_');
            }
            else if (char.IsLetterOrDigit(c) || c == '_')
            {
                sb.Append(c);
            }
            // Skip other characters
        }

        // Remove consecutive underscores and leading/trailing underscores
        var result = sb.ToString();
        result = MultipleUnderscoreRegex.Replace(result, "_");
        result = result.Trim('_');

        return result;
    }

    private static bool IsValidIdentifier(string name)
    {
        return !string.IsNullOrEmpty(name) && ValidIdentifierRegex.IsMatch(name);
    }
}
