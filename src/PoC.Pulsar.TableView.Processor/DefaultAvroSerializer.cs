using System.Buffers;
using System.IO.Pipelines;
using System.Reflection;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Microsoft.IO;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class DefaultAvroSerializer<T> : IAvroSerializer<T>
    where T : class
{
    private static readonly BindingFlags MemberVisibility = BindingFlags.Instance | BindingFlags.Public;

    private readonly BinaryDeserializer<T> _deserializer;
    private readonly BinarySerializer<T> _serializer;

    // RecyclableMemoryStreamManager to avoid large object heap fragmentation when serializing messages
    private static readonly RecyclableMemoryStreamManager _streamManager = new();

    public DefaultAvroSerializer(string schemaFileName)
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
        if (data.IsSingleSegment)
        {
            var reader = new BinaryReader(data.FirstSpan);
            return _deserializer(ref reader);
        }

        var fallbackReader = new BinaryReader(data.ToArray());
        return _deserializer(ref fallbackReader);
    }

    public byte[] Serialize(T obj)
    {
        using var stream = _streamManager.GetStream();
        using var writer = new BinaryWriter(stream);

        _serializer(obj, writer);

        return stream.GetBuffer();
    }

    /// <summary>
    /// Serializes the given object to Avro format and writes it to the provided stream. 
    /// The caller is responsible for managing the stream's lifecycle and ensuring it is properly disposed of after use.
    /// </summary>
    /// <param name="obj"></param>
    /// <param name="destinationStream"></param>
    public void Serialize(T value, PipeWriter writer)
    {
        using var stream = writer.AsStream();
        _serializer(value, new BinaryWriter(stream));
    }

    public void Serialize(T value, Stream stream)
    {
        _serializer(value, new BinaryWriter(stream));
    }
}
