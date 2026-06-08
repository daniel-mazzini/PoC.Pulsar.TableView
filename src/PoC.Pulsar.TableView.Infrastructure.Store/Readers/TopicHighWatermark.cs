using DotPulsar;
using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public sealed record TopicShardHighWatermark(TopicShard Shard, MessageId LastMessageId);

public sealed record TopicHighWatermark(string Topic, IReadOnlyCollection<TopicShardHighWatermark> ShardWatermarks)
{
    
    public bool HasMessages => ShardWatermarks.Count > 0;

    public MessageId GetShardHighWatermarkOrThrow(TopicShard shard)
    {
        MessageId? messageId = ShardWatermarks.FirstOrDefault(watermark => watermark.Shard == shard)?.LastMessageId;
        ArgumentNullException.ThrowIfNull(messageId, nameof(messageId));
        return messageId;

    }
    public IReadOnlyCollection<TopicShard> Shards => [.. ShardWatermarks.Select(watermark => watermark.Shard)];
}
