using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

public class GreptimeDbFixture : IAsyncLifetime
{
    private readonly IContainer _container;

    // GreptimeDB ports
    private const int HttpPort = 4000;
    private const int GrpcPort = 4001;
    private const int MysqlPort = 4002;
    private const int PostgresPort = 4003;

    public GreptimeDbFixture()
    {
        _container = new ContainerBuilder(new DockerImage("greptime/greptimedb:v1.0.0-beta.4"))
            .WithCommand("standalone", "start", "--http-addr", "0.0.0.0:4000", "--rpc-bind-addr", "0.0.0.0:4001", "--mysql-addr", "0.0.0.0:4002", "--postgres-addr", "0.0.0.0:4003")
            .WithPortBinding(HttpPort, true)
            .WithPortBinding(GrpcPort, true)
            .WithPortBinding(MysqlPort, true)
            .WithPortBinding(PostgresPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(HttpPort).ForPath("/health")))
            .Build();
    }

    public string GetEndpoint()
    {
        var host = _container.Hostname;
        var port = _container.GetMappedPublicPort(GrpcPort);
        return $"http://{host}:{port}";
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}
