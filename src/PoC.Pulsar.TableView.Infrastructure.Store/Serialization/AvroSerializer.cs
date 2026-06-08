using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using System.Buffers;
using System.Collections.Immutable;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

public class AvroSerializer : IAvroSerializer
{
    private readonly BinarySerializerBuilder _serializerBuilder = new();
    private readonly BinaryDeserializerBuilder _deserializerBuilder = new();

    private readonly ConcurrentDictionary<Type, object> _serializationCache = new();
    private readonly ConcurrentDictionary<Type, object> _deserialization = new();

    ImmutableDictionary<Type, Schema> _registerSchemas;

    public AvroSerializer(IDictionary<Type, Schema> schemasRegistered)
    {
        _registerSchemas = schemasRegistered.ToImmutableDictionary();
    }

    public void Serialize<T>(T message, Stream output)
    {
        var serializerAction = (Delegate)_serializationCache.GetOrAdd(typeof(T), _ =>
        {
            var schema = GetSchema<T>();
            return _serializerBuilder.BuildDelegate<T>(schema);
        });

        var writer = new BinaryWriter(output);
        serializerAction.DynamicInvoke(message, writer);
    }

    public T Deserialize<T>(ReadOnlySequence<byte> data)
    {
        var deserializer = (BinaryDeserializer<T>)_deserialization.GetOrAdd(typeof(T), _ =>
        {
            var schema = GetSchema<T>();
            return _deserializerBuilder.BuildDelegate<T>(schema);
        });

        if (data.IsSingleSegment)
        {
            var reader = new BinaryReader(data.FirstSpan);
            return deserializer(ref reader);
        }

        int totalLength = (int)data.Length;
        byte[] rentedArray = ArrayPool<byte>.Shared.Rent(totalLength);

        try
        {
            data.CopyTo(rentedArray);
            var contiguousSpan = rentedArray.AsSpan(0, totalLength);
            var reader = new BinaryReader(contiguousSpan);
            return deserializer(ref reader);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentedArray);
        }
    }

    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        var deserializer = (BinaryDeserializer<T>)_deserialization.GetOrAdd(typeof(T), _ =>
        {
            var schema = GetSchema<T>();
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

    private Schema GetSchema<T>() =>
        _registerSchemas.TryGetValue(typeof(T), out var s) ? s : throw new Exception($"Schema {typeof(T).Name} no registered.");
}
