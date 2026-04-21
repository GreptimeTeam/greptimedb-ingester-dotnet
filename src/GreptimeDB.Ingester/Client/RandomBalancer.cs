using Grpc.Net.Client.Balancer;
using Grpc.Net.Client.Configuration;
using Microsoft.Extensions.Logging;

namespace GreptimeDB.Ingester.Client;

/// <summary>
/// <see cref="LoadBalancerFactory"/> for the <c>random</c> policy: each pick selects
/// uniformly at random from the ready subchannels. Spreads load with no shared state
/// or coordination, which avoids the lock-step herding pattern that round-robin can
/// produce when many short-lived clients start at the same time.
/// </summary>
internal sealed class RandomBalancerFactory : LoadBalancerFactory
{
    public static readonly RandomBalancerFactory Instance = new();

    public override string Name => RandomConfig.Name;

    public override LoadBalancer Create(LoadBalancerOptions options)
    {
        return new RandomBalancer(options.Controller, options.LoggerFactory);
    }
}

internal sealed class RandomConfig : LoadBalancingConfig
{
    public const string Name = "random";

    public RandomConfig() : base(Name) { }
}

internal sealed class RandomBalancer : SubchannelsLoadBalancer
{
    public RandomBalancer(IChannelControlHelper controller, ILoggerFactory loggerFactory)
        : base(controller, loggerFactory) { }

    protected override SubchannelPicker CreatePicker(IReadOnlyList<Subchannel> readySubchannels)
    {
        return new RandomPicker(readySubchannels);
    }
}

internal sealed class RandomPicker : SubchannelPicker
{
    private readonly IReadOnlyList<Subchannel> _subchannels;

    public RandomPicker(IReadOnlyList<Subchannel> subchannels)
    {
        _subchannels = subchannels;
    }

    public override PickResult Pick(PickContext context)
    {
        // Random.Shared is thread-safe; pickers are invoked concurrently from many RPCs.
        var index = Random.Shared.Next(_subchannels.Count);
        return PickResult.ForSubchannel(_subchannels[index]);
    }
}
