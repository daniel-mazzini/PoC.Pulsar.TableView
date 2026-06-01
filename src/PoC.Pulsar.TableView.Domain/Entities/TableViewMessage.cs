using System.Buffers;

namespace PoC.Pulsar.TableView.Domain.Entities;

public readonly record struct TableViewMessage(string TopicName,
                                                 int PartitionId,
                                                 string? Key,
                                                 ReadOnlySequence<byte> Data,
                                                 PulsarMessageId MessageId,
                                                 IReadOnlyDictionary<string, string>? Properties = null)
{
    public IReadOnlyDictionary<string, string> Headers { get; init; } = Properties ?? new Dictionary<string, string>(StringComparer.Ordinal);
}
