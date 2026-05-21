namespace PoC.Pulsar.TableView.Processor;

public interface IAvroSerializer<T>
{
    byte[] Serialize(T obj);
}
