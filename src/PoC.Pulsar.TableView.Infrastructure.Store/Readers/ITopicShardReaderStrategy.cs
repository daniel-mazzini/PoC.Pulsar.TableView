using DotPulsar;
using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public interface ITopicShardReaderStrategy
{
    Task<TopicHighWatermark> CaptureHighWatermarkAsync(string logicalTopic, CancellationToken cancellationToken);

    Task<IReadOnlyCollection<TopicShard>> DiscoverShardsAsync(string logicalTopic, CancellationToken cancellationToken);

    Task<IProjectorTopicReader> CreateReaderAsync(
        TopicShard shard,
        MessageId startMessageId,
        CancellationToken cancellationToken);
}
