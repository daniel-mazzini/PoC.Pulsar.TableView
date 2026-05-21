namespace PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

public interface ICheckpointStore
{
    void SaveCheckpoint(PulsarMessageId id);

    PulsarMessageId? GetLastCheckpoint();
}
