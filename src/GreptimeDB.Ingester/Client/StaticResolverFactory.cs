using Grpc.Net.Client.Balancer;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// Emits a fixed list of <see cref="BalancerAddress"/> entries as the resolver
/// result for a gRPC channel, enabling static client-side load balancing.
/// Matches URIs with scheme <c>static</c>.
/// </summary>
internal sealed class StaticResolverFactory : ResolverFactory
{
    private readonly IReadOnlyList<BalancerAddress> _addresses;

    public StaticResolverFactory(IReadOnlyList<BalancerAddress> addresses)
    {
        _addresses = addresses;
    }

    public override string Name => "static";

    public override Resolver Create(ResolverOptions options)
    {
        return new StaticResolver(_addresses);
    }

    private sealed class StaticResolver : Resolver
    {
        private readonly IReadOnlyList<BalancerAddress> _addresses;

        public StaticResolver(IReadOnlyList<BalancerAddress> addresses)
        {
            _addresses = addresses;
        }

        public override void Start(Action<ResolverResult> listener)
        {
            listener(ResolverResult.ForResult(_addresses));
        }
    }
}
