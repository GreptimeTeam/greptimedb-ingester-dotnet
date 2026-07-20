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
    /// client-side endpoint selection with request-level failover across
    /// endpoints for unary writes/deletes.
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
    /// Gets or sets the load-balancing strategy used when multiple
    /// <see cref="Endpoints"/> are configured. Defaults to
    /// <see cref="LoadBalancingStrategy.Random"/>.
    /// </summary>
    public LoadBalancingStrategy LoadBalancing { get; set; } = LoadBalancingStrategy.Random;

    /// <summary>
    /// Gets or sets request-level failover behavior for multi-endpoint clients.
    /// Unary write/delete requests can be retried against another endpoint after
    /// safe transient transport failures or retryable GreptimeDB server status
    /// codes. Client-side write deadlines are not replayed because the write
    /// outcome is ambiguous. Streaming and bulk writers are not replayed
    /// automatically; their final transport outcome updates endpoint health so
    /// the next writer can pick a different endpoint.
    /// </summary>
    public FailoverOptions Failover { get; set; } = new();

    /// <summary>
    /// Gets or sets HTTP/2 keepalive for the gRPC connections shared by all write
    /// paths (unary, streaming, bulk). Enabled by default to detect silent
    /// connection resets by intermediate load balancers, NAT gateways, or
    /// firewalls before the next write hits a dead connection.
    /// </summary>
    public KeepAliveOptions KeepAlive { get; set; } = new();

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
            if (Endpoints != null && Endpoints.Count > 0)
            {
                throw new ArgumentException(
                    "Endpoints was set but contained no non-whitespace entries.",
                    nameof(Endpoints));
            }

            throw new ArgumentException(
                "At least one endpoint is required. Set Endpoints; the deprecated Endpoint property is empty or whitespace.",
                nameof(Endpoints));
        }

        string? firstScheme = null;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var endpoint in endpoints)
        {
            if (!seen.Add(endpoint))
            {
                throw new ArgumentException(
                    $"Duplicate endpoint '{endpoint}'. Each endpoint must be unique.",
                    nameof(Endpoints));
            }

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

            // Reject path/query/fragment: the multi-endpoint balancer uses host:port only
            // (BalancerAddress), so a path would silently diverge from the single-endpoint case.
            if (uri.AbsolutePath is not ("" or "/") || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
            {
                throw new ArgumentException(
                    $"Endpoint '{endpoint}' must be a host:port URI without a path, query, or fragment.",
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
        if (Failover == null)
        {
            throw new ArgumentException("Failover is required.", nameof(Failover));
        }

        Failover.Validate();

        if (KeepAlive == null)
        {
            throw new ArgumentException("KeepAlive is required.", nameof(KeepAlive));
        }

        KeepAlive.Validate();
    }
}

/// <summary>
/// Client-side load-balancing strategy used when multiple endpoints are
/// configured.
/// </summary>
public enum LoadBalancingStrategy
{
    /// <summary>
    /// Pick an available endpoint uniformly at random for each call. Default.
    /// Avoids the lock-step herding pattern that round-robin can produce when
    /// many short-lived clients start at the same time.
    /// </summary>
    Random = 0,

    /// <summary>
    /// Cycle through ready endpoints in order.
    /// </summary>
    RoundRobin = 1,
}

/// <summary>
/// Request-level failover options for multi-endpoint clients.
/// </summary>
public sealed class FailoverOptions
{
    /// <summary>
    /// Gets or sets whether request-level failover is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum number of endpoint attempts for a single unary
    /// write/delete request. When null, the client tries at most each configured
    /// endpoint once.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// Gets or sets how many consecutive endpoint-level transport failures eject
    /// an endpoint from normal selection.
    /// </summary>
    public int ConsecutiveFailuresBeforeEjection { get; set; } = 5;

    /// <summary>
    /// Gets or sets the first ejection delay. Repeated ejections double this
    /// delay up to <see cref="MaxEjectionDelay"/>.
    /// </summary>
    public TimeSpan BaseEjectionDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum delay for a single endpoint ejection.
    /// </summary>
    public TimeSpan MaxEjectionDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Validates the failover options and throws if invalid.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        if (MaxAttempts is <= 0)
        {
            throw new ArgumentException("MaxAttempts must be positive when set.", nameof(MaxAttempts));
        }

        if (ConsecutiveFailuresBeforeEjection <= 0)
        {
            throw new ArgumentException(
                "ConsecutiveFailuresBeforeEjection must be positive.",
                nameof(ConsecutiveFailuresBeforeEjection));
        }

        if (BaseEjectionDelay <= TimeSpan.Zero)
        {
            throw new ArgumentException("BaseEjectionDelay must be positive.", nameof(BaseEjectionDelay));
        }

        if (MaxEjectionDelay <= TimeSpan.Zero)
        {
            throw new ArgumentException("MaxEjectionDelay must be positive.", nameof(MaxEjectionDelay));
        }

        if (MaxEjectionDelay < BaseEjectionDelay)
        {
            throw new ArgumentException(
                "MaxEjectionDelay must be greater than or equal to BaseEjectionDelay.",
                nameof(MaxEjectionDelay));
        }
    }
}

/// <summary>
/// HTTP/2 keepalive options for the gRPC connections.
/// </summary>
public sealed class KeepAliveOptions
{
    // SocketsHttpHandler rejects ping intervals below 1s; validate rather than let it throw at runtime.
    private static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets whether keepalive pings are sent. Enabled by default.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the idle delay before the client sends a keepalive ping.
    /// Defaults to 30 seconds, matching GreptimeDB's internal gRPC client.
    /// </summary>
    public TimeSpan PingDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets how long to wait for a ping acknowledgement before closing
    /// the connection. Defaults to 10 seconds.
    /// </summary>
    public TimeSpan PingTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets whether to ping while the connection is idle (no active
    /// calls). Defaults to true; safe because GreptimeDB's server does not
    /// enforce a minimum ping interval. This is what protects idle streaming
    /// connections from silent resets.
    /// </summary>
    public bool PingWhileIdle { get; set; } = true;

    /// <summary>
    /// Validates the keepalive options and throws if invalid.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        if (!Enabled)
        {
            return;
        }

        if (PingDelay < MinInterval)
        {
            throw new ArgumentException(
                $"PingDelay must be at least {MinInterval.TotalSeconds} second(s) when keepalive is enabled.",
                nameof(PingDelay));
        }

        if (PingTimeout < MinInterval)
        {
            throw new ArgumentException(
                $"PingTimeout must be at least {MinInterval.TotalSeconds} second(s) when keepalive is enabled.",
                nameof(PingTimeout));
        }
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
