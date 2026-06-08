using DotPulsar;
using DotPulsar.Abstractions;
using PoC.Pulsar.TableView.Domain.TableView;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around a real DotPulsar reader.")]
internal sealed class DotPulsarProjectorTopicReader : IProjectorTopicReader
{
    private readonly IReader<byte[]> _reader;
    private readonly TopicShard _shard;

    public DotPulsarProjectorTopicReader(TopicShard shard, IReader<byte[]> reader)
    {
        _shard = shard;
        _reader = reader;
    }

    public async Task<TableViewMessage> ReceiveAsync(CancellationToken cancellationToken)
    {
        var message = await _reader.Receive(cancellationToken);

        if (!message.HasKey)
        {
            throw new InvalidOperationException($"Received compacted message on topic '{_shard.LogicalTopic}' without a key.");
        }

        return new TableViewMessage(_shard.LogicalTopic,
                                    _shard.PartitionId,
                                    message.Key ?? string.Empty,
                                    message.Data,
                                    ToPulsarMessageId(message.MessageId),
                                    message.Properties,
                                    _shard.PhysicalTopic,
                                    _shard.IsPartitioned);
    }

    private static PulsarMessageId ToPulsarMessageId(MessageId messageId)
    {
        if (TryToPulsarMessageId(messageId, out var pulsarMessageId))
        {
            return pulsarMessageId;
        }

        throw new InvalidOperationException(
            $"Unsupported Pulsar message id {messageId.LedgerId}:{messageId.EntryId}:{messageId.Partition}.");
    }

    private static bool TryToPulsarMessageId(MessageId messageId, out PulsarMessageId pulsarMessageId)
    {
        if (messageId.LedgerId > long.MaxValue || messageId.EntryId > long.MaxValue)
        {
            pulsarMessageId = default;
            return false;
        }

        pulsarMessageId = new PulsarMessageId((long)messageId.LedgerId, (long)messageId.EntryId, messageId.Partition, messageId.BatchIndex);
        return true;
    }

    public ValueTask DisposeAsync() => _reader.DisposeAsync();

    public async IAsyncEnumerable<TableViewMessage> ReadAllAsync([EnumeratorCancellation] CancellationToken stopToken)
    {
        while (!stopToken.IsCancellationRequested)
        {
            yield return await ReceiveAsync(stopToken);
        }
    }
}
