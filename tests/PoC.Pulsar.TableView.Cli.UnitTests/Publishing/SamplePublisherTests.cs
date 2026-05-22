using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Publishing;
using PoC.Pulsar.TableView.Cli.Pulsar;
using PoC.Pulsar.TableView.Cli.Samples;
using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Publishing;

public sealed class SamplePublisherTests
{
    [Fact]
    public async Task publish_async_should_create_expected_producer_topics()
    {
        var factory = new FakePulsarMessageProducerFactory();
        var publisher = CreatePublisher(factory: factory);

        await publisher.PublishAsync();

        Assert.Equal(
            ["persistent://public/default/sports", "persistent://public/default/categories"],
            factory.CreatedTopics);
    }

    [Fact]
    public async Task publish_async_should_publish_sports_before_categories()
    {
        var factory = new FakePulsarMessageProducerFactory();
        var publisher = CreatePublisher(factory: factory);

        await publisher.PublishAsync();

        Assert.Equal("persistent://public/default/sports", factory.SentMessages[0].Topic);
        Assert.Equal("persistent://public/default/sports", factory.SentMessages[2].Topic);
        Assert.Equal("persistent://public/default/categories", factory.SentMessages[3].Topic);
    }

    [Fact]
    public async Task publish_async_should_send_entity_id_as_message_key()
    {
        var factory = new FakePulsarMessageProducerFactory();
        var publisher = CreatePublisher(factory: factory);

        await publisher.PublishAsync();

        Assert.Contains(factory.SentMessages, message => message.Key == "sport-1");
        Assert.Contains(factory.SentMessages, message => message.Key == "category-1");
    }

    [Fact]
    public async Task publish_async_should_publish_three_versions_per_message()
    {
        var serializer = new FakeSampleSchemaSerializer();
        var publisher = CreatePublisher(serializer: serializer);

        await publisher.PublishAsync();

        Assert.Equal([1, 2, 3, 1, 2, 3], serializer.SerializedMessages.Select(message => message.Version));
    }

    [Fact]
    public async Task publish_async_should_add_expected_properties()
    {
        var factory = new FakePulsarMessageProducerFactory();
        var publisher = CreatePublisher(factory: factory);

        await publisher.PublishAsync();

        var sportMessage = factory.SentMessages[0];
        Assert.Equal("SportMessage", sportMessage.Properties["type"]);
        Assert.Equal("sport-updated", sportMessage.Properties["event-type"]);

        var categoryMessage = factory.SentMessages[3];
        Assert.Equal("RawCategoryMessage", categoryMessage.Properties["type"]);
        Assert.Equal("category-updated", categoryMessage.Properties["event-type"]);
    }

    [Fact]
    public async Task publish_async_should_use_deterministic_timestamps()
    {
        var factory = new FakePulsarMessageProducerFactory();
        var publisher = CreatePublisher(factory: factory);

        await publisher.PublishAsync();

        Assert.Equal("2024-01-01T00:01:00.0000000+00:00", factory.SentMessages[0].Properties["timestamp"]);
        Assert.Equal("2024-01-01T00:03:00.0000000+00:00", factory.SentMessages[2].Properties["timestamp"]);
        Assert.Equal("2024-01-01T00:01:00.0000000+00:00", factory.SentMessages[3].Properties["timestamp"]);
    }

    [Fact]
    public async Task publish_async_should_serialize_with_expected_schema_files()
    {
        var serializer = new FakeSampleSchemaSerializer();
        var publisher = CreatePublisher(serializer: serializer);

        await publisher.PublishAsync();

        Assert.Equal(
            ["SportMessage.avsc", "SportMessage.avsc", "SportMessage.avsc", "RawCategoryMessage.avsc", "RawCategoryMessage.avsc", "RawCategoryMessage.avsc"],
            serializer.SerializedMessages.Select(message => message.SchemaFileName));
    }

    private static SamplePublisher CreatePublisher(
        FakeSampleDataLoader? loader = null,
        FakeSampleSchemaSerializer? serializer = null,
        FakePulsarMessageProducerFactory? factory = null)
    {
        return new SamplePublisher(
            Options.Create(new PulsarPublishOptions
            {
                ServiceUrl = "pulsar://localhost:6650",
                InputNamespace = "public/default"
            }),
            loader ?? new FakeSampleDataLoader(),
            serializer ?? new FakeSampleSchemaSerializer(),
            factory ?? new FakePulsarMessageProducerFactory(),
            NullLogger<SamplePublisher>.Instance);
    }

    private sealed class FakeSampleDataLoader : ISampleDataLoader
    {
        public IReadOnlyList<SportMessage> LoadSports()
        {
            return
            [
                new SportMessage
                {
                    Id = "sport-1",
                    Provider = "provider",
                    EntityCoverage = "covered",
                    Name = "Soccer",
                    Version = 0,
                    SportType = "SOCCER"
                }
            ];
        }

        public IReadOnlyList<RawCategoryMessage> LoadCategories()
        {
            return
            [
                new RawCategoryMessage
                {
                    Id = "category-1",
                    Provider = "provider",
                    EntityCoverage = "covered",
                    Name = "Spain",
                    Version = 0,
                    SportId = "sport-1",
                    CountryCode = "ES"
                }
            ];
        }
    }

    private sealed class FakeSampleSchemaSerializer : ISampleSchemaSerializer
    {
        public List<SerializedMessage> SerializedMessages { get; } = [];

        public Task<byte[]> SerializeAsync<T>(T value, string schemaFileName)
            where T : class
        {
            var message = Assert.IsAssignableFrom<OfferHierarchyEntity>(value);
            SerializedMessages.Add(new SerializedMessage(schemaFileName, message.Id, message.Version));
            return Task.FromResult(new byte[] { (byte)message.Version });
        }
    }

    private sealed class FakePulsarMessageProducerFactory : IPulsarMessageProducerFactory
    {
        private readonly List<PublishedMessage> _sentMessages = [];

        public List<string> CreatedTopics { get; } = [];

        public IReadOnlyList<PublishedMessage> SentMessages => _sentMessages;

        public Task<IPulsarMessageProducer> CreateAsync(string topic)
        {
            CreatedTopics.Add(topic);
            return Task.FromResult<IPulsarMessageProducer>(new FakePulsarMessageProducer(topic, _sentMessages));
        }
    }

    private sealed class FakePulsarMessageProducer : IPulsarMessageProducer
    {
        private readonly string _topic;
        private readonly List<PublishedMessage> _sentMessages;

        public FakePulsarMessageProducer(string topic, List<PublishedMessage> sentMessages)
        {
            _topic = topic;
            _sentMessages = sentMessages;
        }

        public Task SendAsync(string key, IReadOnlyDictionary<string, string> properties, byte[] payload)
        {
            _sentMessages.Add(new PublishedMessage(_topic, key, new Dictionary<string, string>(properties), payload));
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed record SerializedMessage(string SchemaFileName, string Id, int Version);

    private sealed record PublishedMessage(string Topic, string Key, IReadOnlyDictionary<string, string> Properties, byte[] Payload);
}
