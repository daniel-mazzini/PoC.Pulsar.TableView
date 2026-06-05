namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

[CollectionDefinition(Name)]
public sealed class PulsarCollection : ICollectionFixture<PulsarContainerFixture>
{
    public const string Name = "pulsar-integration";
}
