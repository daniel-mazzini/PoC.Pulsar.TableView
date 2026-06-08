using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Processor.Benchmarks;

internal sealed class DefaultAvroSerializer<T>
    where T : class
{
    private static readonly System.Reflection.BindingFlags MemberVisibility = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public;
    private readonly Delegate _serializer;

    public DefaultAvroSerializer(string schemaFileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "AvroSchemas", schemaFileName);
        var schemaJson = File.ReadAllText(schemaPath);
        var schema = new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());

        _serializer = new BinarySerializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinarySerializerBuilderContext(null));
    }

    public byte[] Serialize(T message)
    {
        using var stream = new MemoryStream();
        Serialize(message, stream);
        return stream.ToArray();
    }

    public void Serialize(T message, Stream output)
    {
        var serializerAction = (BinarySerializer<T>)_serializer;
        using var writer = new BinaryWriter(output);
        serializerAction(message, writer);
    }

    public void Serialize(T message, System.IO.Pipelines.PipeWriter writer)
    {
        using var stream = writer.AsStream(leaveOpen: true);
        Serialize(message, stream);
    }
}
