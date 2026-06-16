using Grpc.Core;

namespace GreptimeDB.Ingester.Client;

internal sealed class EndpointSelector
{
    private readonly object _gate = new();
    private readonly EndpointState[] _endpoints;
    private readonly LoadBalancingStrategy _strategy;
    private readonly FailoverOptions _options;
    private int _nextRoundRobinIndex;

    public EndpointSelector(
        IReadOnlyList<string> endpoints,
        LoadBalancingStrategy strategy,
        FailoverOptions options)
    {
        _endpoints = endpoints.Select(endpoint => new EndpointState(endpoint)).ToArray();
        _strategy = strategy;
        _options = options;
    }

    public int Count => _endpoints.Length;

    public int MaxAttempts
    {
        get
        {
            if (!_options.Enabled)
            {
                return 1;
            }

            return Math.Min(_options.MaxAttempts ?? _endpoints.Length, _endpoints.Length);
        }
    }

    public string Select(IReadOnlySet<string>? exclude = null)
    {
        lock (_gate)
        {
            var candidates = GetCandidates(exclude, excludeEjected: true);
            if (candidates.Length == 0)
            {
                candidates = GetCandidates(exclude, excludeEjected: false);
            }

            if (candidates.Length == 0)
            {
                candidates = _endpoints;
            }

            return Pick(candidates).Endpoint;
        }
    }

    public void ReportSuccess(string endpoint)
    {
        lock (_gate)
        {
            var state = Find(endpoint);
            if (state == null)
            {
                return;
            }

            state.ConsecutiveFailures = 0;
            state.EjectedUntilUtc = DateTimeOffset.MinValue;
        }
    }

    public void ReportFailure(string endpoint)
    {
        lock (_gate)
        {
            var state = Find(endpoint);
            if (state == null)
            {
                return;
            }

            state.ConsecutiveFailures++;
            var now = DateTimeOffset.UtcNow;
            if (state.ConsecutiveFailures < _options.ConsecutiveFailuresBeforeEjection ||
                state.EjectedUntilUtc > now)
            {
                return;
            }

            var multiplier = Math.Pow(2, state.EjectionCount);
            var delayTicks = (long)Math.Min(
                _options.MaxEjectionDelay.Ticks,
                _options.BaseEjectionDelay.Ticks * multiplier);

            state.EjectedUntilUtc = now.AddTicks(delayTicks);
            state.EjectionCount++;
            state.ConsecutiveFailures = 0;
        }
    }

    public void ReportOutcome(string endpoint, Exception? error)
    {
        if (error == null)
        {
            ReportSuccess(endpoint);
            return;
        }

        if (IsEndpointFailure(error))
        {
            ReportFailure(endpoint);
        }
    }

    public static bool IsEndpointFailure(Exception exception)
    {
        if (exception is TimeoutException)
        {
            return true;
        }

        return exception is RpcException rpcException && IsEndpointFailureStatus(rpcException.StatusCode);
    }

    private static bool IsEndpointFailureStatus(StatusCode statusCode)
    {
        return statusCode is StatusCode.Unavailable
            or StatusCode.DeadlineExceeded
            or StatusCode.ResourceExhausted;
    }

    private EndpointState[] GetCandidates(IReadOnlySet<string>? exclude, bool excludeEjected)
    {
        var now = DateTimeOffset.UtcNow;
        return _endpoints
            .Where(endpoint =>
                (exclude == null || !exclude.Contains(endpoint.Endpoint)) &&
                (!excludeEjected || endpoint.EjectedUntilUtc <= now))
            .ToArray();
    }

    private EndpointState Pick(IReadOnlyList<EndpointState> candidates)
    {
        return _strategy switch
        {
            LoadBalancingStrategy.Random => candidates[Random.Shared.Next(candidates.Count)],
            LoadBalancingStrategy.RoundRobin => candidates[GetNextRoundRobinIndex(candidates.Count)],
            _ => throw new InvalidOperationException($"Unsupported load-balancing strategy: {_strategy}."),
        };
    }

    private int GetNextRoundRobinIndex(int count)
    {
        var value = Interlocked.Increment(ref _nextRoundRobinIndex) - 1;
        return (value & int.MaxValue) % count;
    }

    private EndpointState? Find(string endpoint)
    {
        return _endpoints.FirstOrDefault(state => string.Equals(state.Endpoint, endpoint, StringComparison.Ordinal));
    }

    private sealed class EndpointState
    {
        public EndpointState(string endpoint)
        {
            Endpoint = endpoint;
        }

        public string Endpoint { get; }

        public int ConsecutiveFailures { get; set; }

        public int EjectionCount { get; set; }

        public DateTimeOffset EjectedUntilUtc { get; set; }
    }
}
