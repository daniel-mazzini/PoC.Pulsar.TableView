using System.Buffers;
using System.Reflection;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Avro.IO;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Cli;

internal sealed class SamplePublisher
{
    private const int VersionCount = 3;
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
        var schema = (RecordSchema)Avro.Schema.Parse(await File.ReadAllTextAsync(schemaPath));

        for (var messageIndex = 0; messageIndex < messages.Count; messageIndex++)
        {
            var sample = messages[messageIndex];

            for (var version = 1; version <= VersionCount; version++)
            {
                var current = CloneWithVersion(sample, version);
                var payload = Serialize(current, schema);
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

    private static byte[] Serialize<T>(T value, RecordSchema schema)
        where T : class
    {
        var record = ToRecord(value, schema);

        using var stream = new MemoryStream();
        var writer = new GenericDatumWriter<GenericRecord>(schema);
        writer.Write(record, new BinaryEncoder(stream));

        return stream.ToArray();
    }

    private static GenericRecord ToRecord(object value, RecordSchema schema)
    {
        var record = new GenericRecord(schema);
        var properties = value.GetType()
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property);

        foreach (var field in schema.Fields)
        {
            var property = properties[field.Name];
            record.Add(field.Name, ToAvroValue(property.GetValue(value), field.Schema));
        }

        return record;
    }

    private static object? ToAvroValue(object? value, Avro.Schema schema)
    {
        schema = UnwrapNullable(schema);

        if (value is null)
        {
            return null;
        }

        return schema switch
        {
            RecordSchema recordSchema => ToRecord(value, recordSchema),
            ArraySchema arraySchema => ToAvroArray(value, arraySchema),
            _ => value
        };
    }

    private static Array ToAvroArray(object? value, ArraySchema arraySchema)
    {
        var values = new List<object?>();
        var enumerable = (System.Collections.IEnumerable?)value;

        if (enumerable is null)
        {
            return Array.Empty<object>();
        }

        foreach (var item in enumerable)
        {
            values.Add(ToAvroValue(item, arraySchema.ItemSchema));
        }

        return values.ToArray();
    }

    private static Avro.Schema UnwrapNullable(Avro.Schema schema)
    {
        if (schema is UnionSchema unionSchema)
        {
            return unionSchema.Schemas.First(candidate => candidate.Tag != Avro.Schema.Type.Null);
        }

        return schema;
    }

    private static string BuildTopic(string @namespace, string topicName)
    {
        return $"persistent://{@namespace}/{topicName}";
    }
}
