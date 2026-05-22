using System.Reflection;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Cli.Samples;

internal sealed class AvroSampleSchemaSerializer : ISampleSchemaSerializer
{
    private static readonly BindingFlags MemberVisibility = BindingFlags.Instance | BindingFlags.Public;

    public async Task<byte[]> SerializeAsync<T>(T value, string schemaFileName)
        where T : class
    {
        var schemaPath = Path.Combine(WorkspacePaths.ResolveSchemaFolder(), schemaFileName);
        var schemaJson = await File.ReadAllTextAsync(schemaPath);
        var schema = new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());
        var serializer = new BinarySerializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinarySerializerBuilderContext(null));

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        serializer(value, writer);

        return stream.ToArray();
    }
}
