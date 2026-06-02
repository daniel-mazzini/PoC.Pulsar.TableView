using MemoryPack;
using PoC.Pulsar.TableView.Domain.Serializers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

public class MemoryPackWrapper : IStateSerializer
{
    public T? Deserialize<T>(ReadOnlySpan<byte> data) => MemoryPackSerializer.Deserialize<T>(data);
    public byte[] Serialize<T>(T value) => MemoryPackSerializer.Serialize(value);
}
