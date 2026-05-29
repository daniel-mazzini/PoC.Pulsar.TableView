using DotPulsar;
using DotPulsar.Abstractions;
using PoC.Pulsar.TableView.Domain.Entities;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around a real DotPulsar reader.")]
internal sealed class DotPulsarProjectorTopicReader : IProjectorTopicReader
{
    private readonly string _topicName;
    private readonly int _partitionId;
    private readonly IReader<byte[]> _reader;

    public DotPulsarProjectorTopicReader(string topicName, int partitionId, IReader<byte[]> reader)
    {
        _topicName = topicName;
        _partitionId = partitionId;
        _reader = reader;
    }

    public async Task<TableViewMessage> ReceiveAsync(CancellationToken cancellationToken)
    {
        var message = await _reader.Receive(cancellationToken);

        if (!message.HasKey)
        {
            throw new InvalidOperationException($"Received compacted message on topic '{_topicName}' without a key.");
        }

        return new TableViewMessage(_topicName,
                                    _partitionId,
                                    message.Key ?? string.Empty,
                                    message.Data,
                                    ToPulsarMessageId(message.MessageId),
                                    message.Properties);
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

    public async IAsyncEnumerable<TableViewMessage> ReadAllAsync([EnumeratorCancellation]CancellationToken stopToken)
    {
        while (!stopToken.IsCancellationRequested)
        {
            yield return await ReceiveAsync(stopToken);
        }
    }
}