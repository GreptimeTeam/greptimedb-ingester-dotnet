namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Configuration options for GreptimeClient.
/// </summary>
public sealed class GreptimeClientOptions
{
    /// <summary>
    /// Deprecated single-endpoint shorthand. Use <see cref="Endpoints"/>
    /// instead; this property will be removed in a future release.
    /// When <see cref="Endpoints"/> contains any non-whitespace entry it
    /// takes precedence and this value is ignored.
    /// </summary>
    [Obsolete("Use Endpoints instead. Endpoint is retained only for backward compatibility and will be removed in a future release.")]
    public string Endpoint { get; set; } = "http://localhost:4001";

    /// <summary>
    /// Gets or sets the list of GreptimeDB endpoints.
    /// A single-element list is the single-node case; multiple entries enable
    /// round-robin client-side load balancing with automatic failover across
    /// endpoints.
    /// All entries must share the same URI scheme (all http or all https).
    /// </summary>
    public IList<string> Endpoints { get; set; } = new List<string>();

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
    /// Returns the effective endpoint list: trimmed, non-whitespace entries of
    /// <see cref="Endpoints"/> when the caller populated that list. Falls back
    /// to a single-element list containing the deprecated <see cref="Endpoint"/>
    /// value only when <see cref="Endpoints"/> is null or empty — never when
    /// <see cref="Endpoints"/> was set but contained only whitespace, so silent
    /// fallback cannot mask a misconfigured endpoint list (e.g. blank env vars).
    /// </summary>
    internal IReadOnlyList<string> ResolveEndpoints()
    {
        if (Endpoints != null && Endpoints.Count > 0)
        {
            return Endpoints
                .Where(e => !string.IsNullOrWhiteSpace(e))
                .Select(e => e.Trim())
                .ToArray();
        }

#pragma warning disable CS0618 // Endpoint is deprecated but still honored as fallback for back-compat.
        return string.IsNullOrWhiteSpace(Endpoint)
            ? Array.Empty<string>()
            : new[] { Endpoint.Trim() };
#pragma warning restore CS0618
    }

    /// <summary>
    /// Validates the options and throws if invalid.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        var endpoints = ResolveEndpoints();
        if (endpoints.Count == 0)
        {
            var allWhitespace = Endpoints != null && Endpoints.Count > 0;
            throw new ArgumentException(
                allWhitespace
                    ? "Endpoints was set but contained no non-whitespace entries."
                    : "At least one endpoint is required (set Endpoints).",
                nameof(Endpoints));
        }

        string? firstScheme = null;
        foreach (var endpoint in endpoints)
        {
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            {
                throw new ArgumentException(
                    $"Invalid endpoint URI: '{endpoint}'. Expected an absolute URI like 'http://host:port'.",
                    nameof(Endpoints));
            }

            if (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)
            {
                throw new ArgumentException(
                    $"Endpoint '{endpoint}' has unsupported scheme '{uri.Scheme}'. Use http or https.",
                    nameof(Endpoints));
            }

            firstScheme ??= uri.Scheme;
            if (!string.Equals(uri.Scheme, firstScheme, StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    "All endpoints must share the same scheme (all http or all https).",
                    nameof(Endpoints));
            }
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
