using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Checkpoints;

public interface ICheckpointStorage
{
    Task SaveCheckpointAsync(string topicName, int partitionId, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken);
    ValueTask<TopicCheckpoint?> GetLastCheckpoint(string topicName, int partitionId, CancellationToken cancellationToken);
    Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken);
    ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken);
}
