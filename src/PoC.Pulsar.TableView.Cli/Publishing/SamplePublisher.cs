using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Pulsar;
using PoC.Pulsar.TableView.Cli.Samples;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Cli.Publishing;

internal sealed class SamplePublisher : ISamplePublisher
{
    private const int VersionCount = 3;
    private static readonly DateTimeOffset TimestampSeed = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private const string SportsTopicName = "sports";
    private const string CategoriesTopicName = "categories";

    private readonly PulsarPublishOptions _options;
    private readonly ISampleDataLoader _sampleDataLoader;
    private readonly ISampleSchemaSerializer _serializer;
    private readonly IPulsarMessageProducerFactory _producerFactory;
    private readonly ILogger<SamplePublisher> _logger;

    public SamplePublisher(
        IOptions<PulsarPublishOptions> options,
        ISampleDataLoader sampleDataLoader,
        ISampleSchemaSerializer serializer,
        IPulsarMessageProducerFactory producerFactory,
        ILogger<SamplePublisher> logger)
    {
        _options = options.Value;
        _sampleDataLoader = sampleDataLoader;
        _serializer = serializer;
        _producerFactory = producerFactory;
        _logger = logger;
    }

    public async Task PublishAsync()
    {
        var sportMessages = _sampleDataLoader.LoadSports();
        var categoryMessages = _sampleDataLoader.LoadCategories();

        await using var sportProducer = await _producerFactory.CreateAsync(BuildTopic(_options.InputNamespace, SportsTopicName));
        await using var categoryProducer = await _producerFactory.CreateAsync(BuildTopic(_options.InputNamespace, CategoriesTopicName));

        await PublishRepeatedlyAsync(sportProducer, sportMessages, "SportMessage.avsc", "SportMessage", "sport-updated", "sports");
        await PublishRepeatedlyAsync(categoryProducer, categoryMessages, "RawCategoryMessage.avsc", "RawCategoryMessage", "category-updated", "categories");
    }

    private async Task PublishRepeatedlyAsync<T>(
        IPulsarMessageProducer producer,
        IReadOnlyList<T> messages,
        string schemaFileName,
        string messageType,
        string eventType,
        string topicLabel)
        where T : OfferHierarchyEntity
    {
        for (var messageIndex = 0; messageIndex < messages.Count; messageIndex++)
        {
            var sample = messages[messageIndex];

            for (var version = 1; version <= VersionCount; version++)
            {
                var current = CloneWithVersion(sample, version);
                var payload = await _serializer.SerializeAsync(current, schemaFileName);
                var timestamp = TimestampSeed.AddMinutes(messageIndex * VersionCount + version).ToString("O");
                var properties = new Dictionary<string, string>
                {
                    ["type"] = messageType,
                    ["event-type"] = eventType,
                    ["timestamp"] = timestamp
                };

                await producer.SendAsync(current.Id, properties, payload);

                _logger.LogInformation("Published {TopicLabel} {EntityId} v{Version}", topicLabel, current.Id, current.Version);
            }
        }
    }

    private static T CloneWithVersion<T>(T sample, int version)
        where T : OfferHierarchyEntity
    {
        var clone = (T?)Activator.CreateInstance(sample.GetType())
            ?? throw new InvalidOperationException($"Could not clone {sample.GetType().FullName}.");

        foreach (var property in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (!property.CanRead || !property.CanWrite)
            {
                continue;
            }

            property.SetValue(clone, property.Name == nameof(OfferHierarchyEntity.Version)
                ? version
                : property.GetValue(sample));
        }

        return clone;
    }

    private static string BuildTopic(string @namespace, string topicName)
    {
        return $"persistent://{@namespace}/{topicName}";
    }
}
