using System.Buffers;
using System.IO;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

public interface IAvroSerializer
{
    T Deserialize<T>(ReadOnlySpan<byte> data);
    T Deserialize<T>(ReadOnlySequence<byte> data);
    Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken);
    void Serialize<T>(T message, Stream output);
}
