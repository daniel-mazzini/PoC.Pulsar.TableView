namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public interface IProjectorTopicReaderFactory
{
    Task<TopicHighWatermark> CaptureHighWatermarkAsync(string topicName, CancellationToken cancellationToken);

    Task<IProjectorTopicReader> CreateReaderAsync(
        string topicName,
        int partitionId,
        DotPulsar.MessageId startMessageId,
        CancellationToken cancellationToken);
}
