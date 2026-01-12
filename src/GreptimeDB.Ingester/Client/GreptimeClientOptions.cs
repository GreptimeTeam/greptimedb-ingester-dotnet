namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Configuration options for GreptimeClient.
/// </summary>
public sealed class GreptimeClientOptions
{
    /// <summary>
    /// Gets or sets the GreptimeDB endpoint (e.g., "http://localhost:4001").
    /// </summary>
    public string Endpoint { get; set; } = "http://localhost:4001";

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public string Database { get; set; } = "public";

    /// <summary>
    /// Gets or sets the authentication credentials.
    /// </summary>
    public AuthenticationOptions? Authentication { get; set; }

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the request timeout for write operations.
    /// </summary>
    public TimeSpan WriteTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Validates the options and throws if invalid.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Endpoint))
        {
            throw new ArgumentException("Endpoint is required.", nameof(Endpoint));
        }

        if (string.IsNullOrWhiteSpace(Database))
        {
            throw new ArgumentException("Database is required.", nameof(Database));
        }

        if (ConnectTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentException("ConnectTimeout must be positive.", nameof(ConnectTimeout));
        }

        if (WriteTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentException("WriteTimeout must be positive.", nameof(WriteTimeout));
        }

        Authentication?.Validate();
    }
}

/// <summary>
/// Authentication options for GreptimeDB.
/// </summary>
public sealed class AuthenticationOptions
{
    /// <summary>
    /// Gets or sets the username.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Gets or sets the password.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Validates the authentication options.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        if (!string.IsNullOrEmpty(Username) && Password == null)
        {
            throw new ArgumentException("Password is required when username is provided.");
        }
    }

    /// <summary>
    /// Gets whether authentication is configured.
    /// </summary>
    public bool IsConfigured => !string.IsNullOrEmpty(Username);
}
