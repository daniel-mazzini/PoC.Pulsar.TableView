using System.Buffers;
using System.IO.Pipelines;

namespace PoC.Pulsar.TableView.Processor;

public interface IAvroSerializer<T>
{
    byte[] Serialize(T obj);

    void Serialize(T obj, PipeWriter writer);
    T Deserialize(ReadOnlySequence<byte> data);
}
