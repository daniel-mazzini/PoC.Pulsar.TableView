using DotPulsar;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Publisher;

[Collection(PulsarCollection.Name)]
public sealed class DotPulsarPublisherIntegrationTests
{
    private readonly PulsarContainerFixture _fixture;

    public DotPulsarPublisherIntegrationTests(PulsarContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task property_taxonomy_view_publisher_should_publish_message_to_topic()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.CreateNamespaceAsync("tableview-outputs");
        await _fixture.CreateTopicAsync(topicNamespace, PulsarTopics.CountryTaxonomyViews);
        await using var client = await _fixture.CreateClientAsync();
        await using var publisher = new DotPulsarPropertyTaxonomyViewPublisher(client, topicNamespace, avroSerializer);
        await using var reader = client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.CountryTaxonomyViews))
            .StartMessageId(MessageId.Earliest)
            .Create();
        var expected = IntegrationTestData.TaxonomyView("sport-1");

        await publisher.PublishAsync(expected, CancellationToken.None);
        var message = await reader.Receive(CancellationToken.None);
        var actual = avroSerializer.Deserialize<GeoTaxonomyViewMessage>(message.Data);

        Assert.Equal(expected.SportId, message.Key);
        Assert.Equal(expected.SportId, actual.SportId);
    }

    [Fact]
    public async Task property_taxonomy_view_publisher_should_publish_tombstone_message()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.CreateNamespaceAsync("tableview-outputs");
        await _fixture.CreateTopicAsync(topicNamespace, PulsarTopics.CountryTaxonomyViews);
        await using var client = await _fixture.CreateClientAsync();
        await using var publisher = new DotPulsarPropertyTaxonomyViewPublisher(client, topicNamespace, avroSerializer);
        await using var reader = client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.CountryTaxonomyViews))
            .StartMessageId(MessageId.Earliest)
            .Create();

        await publisher.PublishDeleteMessageAsync("sport-delete", DateTimeOffset.UtcNow, CancellationToken.None);
        var message = await reader.Receive(CancellationToken.None);

        Assert.Equal("sport-delete", message.Key);
        Assert.Equal(0, message.Data.Length);
    }

    [Fact]
    public async Task rejected_message_publisher_should_publish_sport_rejected_message()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.CreateNamespaceAsync("tableview-outputs");
        await _fixture.CreateTopicAsync(topicNamespace, PulsarTopics.SportsRejected);
        await using var client = await _fixture.CreateClientAsync();
        await using var publisher = new DotPulsarRejectedMessagePublisher(client, topicNamespace, avroSerializer);
        await using var reader = client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.SportsRejected))
            .StartMessageId(MessageId.Earliest)
            .Create();
        var rejected = new Rejected<SportMessage>(Guid.CreateVersion7(),
                                                  "persistent://public/tableview-inputs/sports",
                                                  0,
                                                  "1:1:0:0",
                                                  "sport-1",
                                                  new RejectedReason("invalid", "invalid sport"),
                                                  IntegrationTestData.Sport("sport-1"),
                                                  DateTimeOffset.UtcNow,
                                                  "correlation-id",
                                                  "causation-id",
                                                  "message-id");

        await publisher.PublishAsync(rejected, CreateRejectedHeaders(), CancellationToken.None);
        var message = await reader.Receive(CancellationToken.None);
        var payload = avroSerializer.Deserialize<SportRejectedMessage>(message.Data);

        Assert.Equal("sport-1", message.Key);
        Assert.Equal(rejected.OriginalMessageKey, payload.OriginalMessageKey);
    }

    [Fact]
    public async Task rejected_message_publisher_should_publish_category_rejected_message()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.CreateNamespaceAsync("tableview-outputs");
        await _fixture.CreateTopicAsync(topicNamespace, PulsarTopics.CategoriesRejected);
        await using var client = await _fixture.CreateClientAsync();
        await using var publisher = new DotPulsarRejectedMessagePublisher(client, topicNamespace, avroSerializer);
        await using var reader = client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(topicNamespace, PulsarTopics.CategoriesRejected))
            .StartMessageId(MessageId.Earliest)
            .Create();
        var rejected = new Rejected<RawCategoryMessage>(Guid.CreateVersion7(),
                                                        "persistent://public/tableview-inputs/categories",
                                                        0,
                                                        "1:2:0:0",
                                                        "category-1",
                                                        new RejectedReason("invalid", "invalid category"),
                                                        IntegrationTestData.Category("category-1", "sport-1"),
                                                        DateTimeOffset.UtcNow,
                                                        "correlation-id",
                                                        "causation-id",
                                                        "message-id");

        await publisher.PublishAsync(rejected, CreateRejectedHeaders(), CancellationToken.None);
        var message = await reader.Receive(CancellationToken.None);
        var payload = avroSerializer.Deserialize<RawCategoryRejectedMessage>(message.Data);

        Assert.Equal("category-1", message.Key);
        Assert.Equal(rejected.OriginalMessageKey, payload.OriginalMessageKey);
    }

    private static Dictionary<string, string> CreateRejectedHeaders()
        => new(StringComparer.Ordinal)
        {
            ["type"] = "Rejected",
            ["event-type"] = "rejected",
            ["message-id"] = Guid.CreateVersion7().ToString("D")
        };
}
