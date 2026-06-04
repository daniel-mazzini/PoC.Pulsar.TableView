using System.Buffers;

namespace PoC.Pulsar.TableView.Domain.TableView;

public readonly record struct TableViewMessage(string TopicName,
                                                 int PartitionId,
                                                 string? Key,
                                                 ReadOnlySequence<byte> Data,
                                                 PulsarMessageId BrokerMessageId,
                                                 IReadOnlyDictionary<string, string>? Properties = null,
                                                 string? PhysicalTopicName = null,
                                                 bool IsPartitioned = false)
{
    public IReadOnlyDictionary<string, string> Headers { get; init; } = Properties ?? new Dictionary<string, string>(StringComparer.Ordinal);
    public string PhysicalTopic => PhysicalTopicName ?? TopicName;
    public TopicShard Shard => new(TopicName, PhysicalTopic, PartitionId, IsPartitioned);
}
