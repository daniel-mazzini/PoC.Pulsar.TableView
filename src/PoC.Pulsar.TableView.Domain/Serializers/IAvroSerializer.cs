using System.Buffers;

namespace PoC.Pulsar.TableView.Domain.Serializers;

public interface IAvroSerializer
{
    T Deserialize<T>(ReadOnlySpan<byte> data);
    T Deserialize<T>(ReadOnlySequence<byte> data);
    Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken);
    void Serialize<T>(T message, Stream output);
}
