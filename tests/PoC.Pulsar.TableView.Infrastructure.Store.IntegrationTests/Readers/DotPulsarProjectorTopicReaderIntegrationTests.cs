using DotPulsar;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.TableViewAppliers;
using System.Buffers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Readers;

[Collection(PulsarCollection.Name)]
public sealed class DotPulsarProjectorTopicReaderIntegrationTests
{
    private readonly PulsarContainerFixture _fixture;

    public DotPulsarProjectorTopicReaderIntegrationTests(PulsarContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task capture_high_watermark_async_should_return_last_message_id_for_isolated_topic()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.GetOrCreateSharedNamespaceAsync("tableview-inputs");
        var topicName = $"sports-{Guid.CreateVersion7():N}".ToLowerInvariant();
        await _fixture.CreatePartitionedTopicAsync(topicNamespace, topicName, 1);
        await using var client = await _fixture.CreateClientAsync();
        await using var producer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, topicName))
            .Create();

        await PublishSportAsync(producer, avroSerializer, IntegrationTestData.Sport("sport-1", 1));
        await PublishSportAsync(producer, avroSerializer, IntegrationTestData.Sport("sport-2", 2));

        var factory = new DotPulsarProjectorTopicReaderFactory(client, topicNamespace);
        var highWatermark = await factory.CaptureHighWatermarkAsync(topicName, CancellationToken.None);

        Assert.True(highWatermark.HasMessages);
        Assert.Single(highWatermark.Shards);
        Assert.Equal(0, highWatermark.Shards.Single().PartitionId);
    }

    [Fact]
    public async Task create_reader_async_should_read_published_sport_message()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.GetOrCreateSharedNamespaceAsync("tableview-inputs");
        var topicName = $"sports-{Guid.CreateVersion7():N}".ToLowerInvariant();
        await _fixture.CreatePartitionedTopicAsync(topicNamespace, topicName, 1);
        await using var client = await _fixture.CreateClientAsync();
        await using var producer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, topicName))
            .Create();
        var expected = IntegrationTestData.Sport("sport-1", 3);

        await PublishSportAsync(producer, avroSerializer, expected);

        var factory = new DotPulsarProjectorTopicReaderFactory(client, topicNamespace);
        await using var reader = await factory.CreateReaderAsync(TopicShard.Partition(topicName, 0), MessageId.Earliest, CancellationToken.None);
        var message = await reader.ReceiveAsync(CancellationToken.None);
        var payload = avroSerializer.Deserialize<SportMessage>(message.Data);

        Assert.Equal(topicName, message.TopicName);
        Assert.Equal(expected.Id, message.Key);
        Assert.Equal(expected.Version, payload.Version);
    }

    [Fact]
    public async Task create_reader_async_should_read_published_raw_category_message()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.GetOrCreateSharedNamespaceAsync("tableview-inputs");
        var topicName = $"categories-{Guid.CreateVersion7():N}".ToLowerInvariant();
        await _fixture.CreatePartitionedTopicAsync(topicNamespace, topicName, 1);
        await using var client = await _fixture.CreateClientAsync();
        await using var producer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, topicName))
            .Create();
        var expected = IntegrationTestData.Category("category-1", "sport-1", version: 3);

        await PublishCategoryAsync(producer, avroSerializer, expected);

        var factory = new DotPulsarProjectorTopicReaderFactory(client, topicNamespace);
        await using var reader = await factory.CreateReaderAsync(TopicShard.Partition(topicName, 0), MessageId.Earliest, CancellationToken.None);
        var message = await reader.ReceiveAsync(CancellationToken.None);
        var payload = avroSerializer.Deserialize<RawCategoryMessage>(message.Data);

        Assert.Equal(topicName, message.TopicName);
        Assert.Equal(expected.Id, message.Key);
        Assert.Equal(expected.SportId, payload.SportId);
        Assert.Equal(expected.Version, payload.Version);
    }

    [Fact]
    public async Task bootstrap_should_read_only_until_captured_high_watermark()
    {
        var avroSerializer = IntegrationAvroSerializerFactory.Create();
        var topicNamespace = await _fixture.GetOrCreateSharedNamespaceAsync("tableview-inputs");
        var topicName = $"sports-{Guid.CreateVersion7():N}".ToLowerInvariant();
        await _fixture.CreatePartitionedTopicAsync(topicNamespace, topicName, 1);
        await using var client = await _fixture.CreateClientAsync();
        await using var producer = client.NewProducer(Schema.ByteSequence)
            .Topic(PulsarTopics.Qualify(topicNamespace, topicName))
            .Create();

        await PublishSportAsync(producer, avroSerializer, IntegrationTestData.Sport("sport-1", 1));
        await PublishSportAsync(producer, avroSerializer, IntegrationTestData.Sport("sport-1", 2));

        using var context = new TsavoriteIntegrationContext(nameof(bootstrap_should_read_only_until_captured_high_watermark));
        var metadata = StoreMetadata.CreateDefault();
        var extraMessage = IntegrationTestData.Sport("sport-1", 3);
        var innerFactory = new DotPulsarProjectorTopicReaderFactory(client, topicNamespace);
        var highWatermarkFactory = new PublishAfterHighWatermarkFactory(innerFactory,
                                                                        producer,
                                                                        avroSerializer,
                                                                        extraMessage,
                                                                        topicName);
        var view = new PulsarTableView<SportMessage>(topicName,
                                                     highWatermarkFactory,
                                                     context.UnitOfWorkFactory,
                                                     avroSerializer,
                                                     new SportMessageApplier(new NoOpRejectedMessagePublisher()),
                                                     metadata,
                                                     NullLogger<PulsarTableView<SportMessage>>.Instance);

        await view.StartBootstrapAsync(CancellationToken.None);
        var entry = await view.GetEntry("sport-1", CancellationToken.None);

        Assert.NotNull(entry);
        Assert.Equal(2, entry.Version);
    }

    private static async Task PublishSportAsync(IProducer<ReadOnlySequence<byte>> producer, PoC.Pulsar.TableView.Domain.Serializers.IAvroSerializer avroSerializer, SportMessage message)
    {
        using var stream = new MemoryStream();
        avroSerializer.Serialize(message, stream);
        var payload = new ReadOnlySequence<byte>(stream.ToArray());
        var metadata = new MessageMetadata { Key = message.Id };
        await producer.Send(metadata, payload, CancellationToken.None);
    }

    private static async Task PublishCategoryAsync(IProducer<ReadOnlySequence<byte>> producer, PoC.Pulsar.TableView.Domain.Serializers.IAvroSerializer avroSerializer, RawCategoryMessage message)
    {
        using var stream = new MemoryStream();
        avroSerializer.Serialize(message, stream);
        var payload = new ReadOnlySequence<byte>(stream.ToArray());
        var metadata = new MessageMetadata { Key = message.Id };
        await producer.Send(metadata, payload, CancellationToken.None);
    }

    private sealed class PublishAfterHighWatermarkFactory : ITopicShardReaderStrategy
    {
        private readonly DotPulsarProjectorTopicReaderFactory _innerFactory;
        private readonly IProducer<ReadOnlySequence<byte>> _producer;
        private readonly PoC.Pulsar.TableView.Domain.Serializers.IAvroSerializer _avroSerializer;
        private readonly SportMessage _message;
        private readonly string _topicName;
        private int _published;

        public PublishAfterHighWatermarkFactory(DotPulsarProjectorTopicReaderFactory innerFactory,
                                                IProducer<ReadOnlySequence<byte>> producer,
                                                PoC.Pulsar.TableView.Domain.Serializers.IAvroSerializer avroSerializer,
                                                SportMessage message,
                                                string topicName)
        {
            _innerFactory = innerFactory;
            _producer = producer;
            _avroSerializer = avroSerializer;
            _message = message;
            _topicName = topicName;
        }

        public async Task<TopicHighWatermark> CaptureHighWatermarkAsync(string topicName, CancellationToken cancellationToken)
        {
            var highWatermark = await _innerFactory.CaptureHighWatermarkAsync(topicName, cancellationToken);
            if (Interlocked.Exchange(ref _published, 1) == 0)
            {
                using var stream = new MemoryStream();
                _avroSerializer.Serialize(_message, stream);
                var metadata = new MessageMetadata { Key = _message.Id };
                await _producer.Send(metadata, new ReadOnlySequence<byte>(stream.ToArray()), cancellationToken);
            }

            Assert.Equal(_topicName, topicName);
            return highWatermark;
        }

        public Task<IReadOnlyCollection<TopicShard>> DiscoverShardsAsync(string logicalTopic, CancellationToken cancellationToken)
            => _innerFactory.DiscoverShardsAsync(logicalTopic, cancellationToken);

        public Task<IProjectorTopicReader> CreateReaderAsync(TopicShard shard, MessageId startMessageId, CancellationToken cancellationToken)
            => _innerFactory.CreateReaderAsync(shard, startMessageId, cancellationToken);
    }
}
