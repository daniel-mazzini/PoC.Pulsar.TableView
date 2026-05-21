using System.Buffers;
using System.Reflection;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class AvroMessageDeserializer<T> : IAvroSerializer<T>
    where T : class
{
    private static readonly BindingFlags MemberVisibility = BindingFlags.Instance | BindingFlags.Public;

    private readonly BinaryDeserializer<T> _deserializer;
    private readonly BinarySerializer<T> _serializer;

    public AvroMessageDeserializer(string schemaFileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "Schemas", schemaFileName);
        var schemaJson = File.ReadAllText(schemaPath);
        var schema = new JsonSchemaReader((IJsonDeserializerBuilder?)null).Read(schemaJson, new JsonSchemaReaderContext());

        _deserializer = new BinaryDeserializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinaryDeserializerBuilderContext(null));

        _serializer = new BinarySerializerBuilder(MemberVisibility)
            .BuildDelegate<T>(schema, new BinarySerializerBuilderContext(null));
    }

    public T Deserialize(ReadOnlySequence<byte> data)
    {
        var reader = new BinaryReader(data.ToArray());
        return _deserializer(ref reader);
    }

    public byte[] Serialize(T obj)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        _serializer(obj, writer);

        return stream.ToArray();
    }
}
