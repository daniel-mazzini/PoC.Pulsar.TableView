using Chr.Avro.Serialization;
using System.Collections.Concurrent;
using System.IO;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;



public class AvroManager : IAvroSerializer
{
    private readonly AvroSchemaRegistry _registry;
    private readonly BinarySerializerBuilder _serializerBuilder = new();
    private readonly BinaryDeserializerBuilder _deserializerBuilder = new();

    private readonly ConcurrentDictionary<Type, object> _serializationCache = new();
    private readonly ConcurrentDictionary<Type, object> _deserialization = new();

    public AvroManager(AvroSchemaRegistry registry)
    {
        _registry = registry;
    }

    public void Serialize<T>(T message, Stream output)
    {
        var serializerAction = (Action<T, BinaryWriter>)_serializationCache.GetOrAdd(typeof(T), _ =>
        {
            var schema = _registry.GetSchema(typeof(T).Name);
            return _serializerBuilder.BuildDelegate<T>(schema);
        });

        var writer = new BinaryWriter(output);
        serializerAction(message, writer);
    }
    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        var deserializer = (BinaryDeserializer<T>)_deserialization.GetOrAdd(typeof(T), _ =>
        {
            var schema = _registry.GetSchema(typeof(T).Name);
            return _deserializerBuilder.BuildDelegate<T>(schema);
        });

        var reader = new BinaryReader(data);
        return deserializer(ref reader);
    }
    public async Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken)
    {
        if (stream is MemoryStream ms && ms.TryGetBuffer(out var segment))
        {
            return Deserialize<T>(segment.AsSpan((int)ms.Position, (int)(ms.Length - ms.Position)));
        }
        using var msBuffer = new MemoryStream();
        await stream.CopyToAsync(msBuffer, cancellationToken);
        return Deserialize<T>(msBuffer.GetBuffer().AsSpan(0, (int)msBuffer.Length));
    }
}