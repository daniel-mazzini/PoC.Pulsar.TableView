using DotPulsar;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public sealed record TopicHighWatermark(string Topic, IReadOnlyDictionary<int, MessageId> LastMessageIds)
{
    
    public bool HasMessages => LastMessageIds.Count > 0;

    public MessageId GetPartitionHighWatermarkOrThrow(int partitionId)
    {
        MessageId? messageId = LastMessageIds.GetValueOrDefault(partitionId);
        ArgumentNullException.ThrowIfNull(messageId, nameof(messageId));
        return messageId;

    }
    public IReadOnlyCollection<int> PartitionIds => [.. LastMessageIds.Keys];
}
