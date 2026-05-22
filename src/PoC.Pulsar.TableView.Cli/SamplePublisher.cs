using System.Buffers;
using System.Reflection;
using System.Text.Json;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Contracts;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Cli;

internal sealed class SamplePublisher
{
    private const int VersionCount = 3;
    private static readonly BindingFlags MemberVisibility = BindingFlags.Instance | BindingFlags.Public;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static readonly DateTimeOffset TimestampSeed = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private const string SportsTopicName = "sports";
    private const string CategoriesTopicName = "categories";

    public async Task PublishAsync()
    {
        var sampleFolder = WorkspacePaths.ResolveSampleFolder();
        var schemaFolder = WorkspacePaths.ResolveSchemaFolder();
        var serviceUrl = Environment.GetEnvironmentVariable("PULSAR_SERVICE_URL")
            ?? throw new InvalidOperationException("PULSAR_SERVICE_URL is required.");
        var inputNamespace = Environment.GetEnvironmentVariable("PULSAR_INPUT_NAMESPACE")
            ?? throw new InvalidOperationException("PULSAR_INPUT_NAMESPACE is required.");

        var sportMessages = LoadMessages<SportMessage>(Path.Combine(sampleFolder, "sports_mock_data.json"));
        var categoryMessages = LoadMessages<RawCategoryMessage>(Path.Combine(sampleFolder, "categories_mock_data.json"));

        await using var client = PulsarClient.Builder()
            .ServiceUrl(new Uri(serviceUrl))
            .Build();

        await using var sportProducer = client.NewProducer(DotPulsar.Schema.ByteSequence)
            .Topic(BuildTopic(inputNamespace, SportsTopicName))
            .Create();

        await using var categoryProducer = client.NewProducer(DotPulsar.Schema.ByteSequence)
            .Topic(BuildTopic(inputNamespace, CategoriesTopicName))
            .Create();

        await PublishRepeatedlyAsync(sportProducer, sportMessages, Path.Combine(schemaFolder, "SportMessage.avsc"), "SportMessage", "sport-updated", "sports");
        await PublishRepeatedlyAsync(categoryProducer, categoryMessages, Path.Combine(schemaFolder, "RawCategoryMessage.avsc"), "RawCategoryMessage", "category-updated", "categories");
    }

    private static IReadOnlyList<T> LoadMessages<T>(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<List<T>>(json, JsonOptions)
            ?? throw new InvalidOperationException($"Could not load sample data from {path}.");
    }

    private static async Task PublishRepeatedlyAsync<T>(IProducer<ReadOnlySequence<byte>> producer, IReadOnlyList<T> messages, string schemaPath, string messageType, string eventType, string topicLabel)
        where T : OfferHierarchyEntity
    {
        var serializer = await BuildSerializerAsync<T>(schemaPath);

        for (var messageIndex = 0; messageIndex < messages.Count; messageIndex++)
        {
            var sample = messages[messageIndex];

            for (var version = 1; version <= VersionCount; version++)
            {
                var current = CloneWithVersion(sample, version);
                var payload = Serialize(current, serializer);
                var timestamp = TimestampSeed.AddMinutes(messageIndex * VersionCount + version).ToString("O");

                await producer.NewMessage()
                    .Key(current.Id)
                    .Property("type", messageType)
                    .Property("event-type", eventType)
                    .Property("timestamp", timestamp)
                    .Send(payload);

                Console.WriteLine($"Published {topicLabel} {current.Id} v{current.Version}");
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

    private static async Task<BinarySerializer<T>> BuildSerializerAsync<T>(string schemaPath)
        where T : class
    {
        var schemaJson = await File.ReadAllTextAsync(schemaPath);
        var schema = new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());

        return new BinarySerializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinarySerializerBuilderContext(null));
    }

    private static byte[] Serialize<T>(T value, BinarySerializer<T> serializer)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        serializer(value, writer);

        return stream.ToArray();
    }

    private static string BuildTopic(string @namespace, string topicName)
    {
        return $"persistent://{@namespace}/{topicName}";
    }
}
