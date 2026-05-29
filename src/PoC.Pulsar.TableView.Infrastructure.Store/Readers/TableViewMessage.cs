using PoC.Pulsar.TableView.Domain.Entities;
using System.Buffers;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public readonly record struct TableViewMessage(string TopicName,
                                                 int PartitionId,
                                                 string? Key,
                                                 ReadOnlySequence<byte> Data,
                                                 PulsarMessageId MessageId,
                                                 IReadOnlyDictionary<string, string>? Properties = null)
{
    public IReadOnlyDictionary<string, string> Headers { get; init; } = Properties ?? new Dictionary<string, string>(StringComparer.Ordinal);
}
