using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

public sealed class GreptimeDbFixture : IAsyncLifetime
{
    private readonly IContainer? _container;
    private readonly string? _externalEndpoint;
    private readonly HttpClient _httpClient = new();

    // GreptimeDB ports
    private const int HttpPort = 4000;
    private const int GrpcPort = 4001;
    private const int MysqlPort = 4002;
    private const int PostgresPort = 4003;

    public GreptimeDbFixture()
    {
        var envEndpoint = Environment.GetEnvironmentVariable("GREPTIMEDB_ENDPOINT");
        if (!string.IsNullOrEmpty(envEndpoint))
        {
            _externalEndpoint = envEndpoint;
            _container = null;
        }
        else
        {
            _container = new ContainerBuilder(new DockerImage("greptime/greptimedb:latest"))
                .WithCommand("standalone", "start", "--http-addr", "0.0.0.0:4000", "--rpc-bind-addr", "0.0.0.0:4001", "--mysql-addr", "0.0.0.0:4002", "--postgres-addr", "0.0.0.0:4003")
                .WithPortBinding(HttpPort, true)
                .WithPortBinding(GrpcPort, true)
                .WithPortBinding(MysqlPort, true)
                .WithPortBinding(PostgresPort, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(HttpPort).ForPath("/health")))
                .Build();
        }
    }

    public string GetEndpoint()
    {
        if (!string.IsNullOrEmpty(_externalEndpoint))
        {
            return _externalEndpoint;
        }

        if (_container == null)
        {
            throw new InvalidOperationException("Container is not initialized and no external endpoint provided.");
        }

        var host = _container.Hostname;
        var port = _container.GetMappedPublicPort(GrpcPort);
        return $"http://{host}:{port}";
    }

    /// <summary>
    /// Runs SQL through the HTTP API and returns the rows of the first result set.
    /// With an external GreptimeDB, the HTTP endpoint is read from GREPTIMEDB_HTTP_ENDPOINT.
    /// </summary>
    public async Task<JsonElement> QueryAsync(string sql, string database = "public")
    {
        var response = await _httpClient.PostAsync(
            $"{GetHttpEndpoint()}/v1/sql?db={Uri.EscapeDataString(database)}",
            new FormUrlEncodedContent(new Dictionary<string, string> { ["sql"] = sql }));
        var body = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"SQL query failed ({(int)response.StatusCode}): {body}");
        }

        using var document = JsonDocument.Parse(body);
        return document.RootElement.GetProperty("output")[0].GetProperty("records").GetProperty("rows").Clone();
    }

    private string GetHttpEndpoint()
    {
        if (!string.IsNullOrEmpty(_externalEndpoint))
        {
            return Environment.GetEnvironmentVariable("GREPTIMEDB_HTTP_ENDPOINT")
                ?? throw new InvalidOperationException(
                    "GREPTIMEDB_HTTP_ENDPOINT must be set when GREPTIMEDB_ENDPOINT is used.");
        }

        if (_container == null)
        {
            throw new InvalidOperationException("Container is not initialized and no external endpoint provided.");
        }

        return $"http://{_container.Hostname}:{_container.GetMappedPublicPort(HttpPort)}";
    }

    public async ValueTask InitializeAsync()
    {
        if (_container != null)
        {
            await _container.StartAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }
}
