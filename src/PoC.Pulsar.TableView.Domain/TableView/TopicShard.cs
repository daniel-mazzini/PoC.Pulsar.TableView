using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.TableView;

[MemoryPackable]
public sealed partial record TopicShard(
    string LogicalTopic,
    string PhysicalTopic,
    int PartitionId,
    bool IsPartitioned)
{
    public static TopicShard NonPartitioned(string logicalTopic)
        => new(logicalTopic, logicalTopic, 0, IsPartitioned: false);

    public static TopicShard Partition(string logicalTopic, int partitionId)
        => new(logicalTopic, $"{logicalTopic}-partition-{partitionId}", partitionId, IsPartitioned: true);
}
