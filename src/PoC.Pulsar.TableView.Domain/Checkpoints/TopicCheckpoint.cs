using MemoryPack;
using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Checkpoints;

[MemoryPackable]
public sealed partial record TopicCheckpoint(
    string LogicalTopic,
    string PhysicalTopic,
    int PartitionId,
    bool IsPartitioned,
    PulsarMessageId LastProcessedMessageId,
    Guid StoreId,
    DateTimeOffset UpdatedAt)
{
    public TopicShard Shard => new(LogicalTopic, PhysicalTopic, PartitionId, IsPartitioned);
}
