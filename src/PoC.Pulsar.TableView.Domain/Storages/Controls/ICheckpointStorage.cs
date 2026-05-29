using PoC.Pulsar.TableView.Domain.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.Controls;

public interface ICheckpointStorage
{
    Task SaveCheckpointAsync(string topicName, int partitionId, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken);
    ValueTask<TopicCheckpoint?> GetLastCheckpoint(string topicName, int partitionId, CancellationToken cancellationToken);
}
