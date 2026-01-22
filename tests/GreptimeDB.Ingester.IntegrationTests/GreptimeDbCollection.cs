using Xunit;

namespace GreptimeDB.Ingester.IntegrationTests;

#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
[CollectionDefinition("GreptimeDB Integration")]
public class GreptimeDbCollection : ICollectionFixture<GreptimeDbFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}
