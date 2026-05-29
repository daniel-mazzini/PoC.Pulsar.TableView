using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around real DotPulsar readers.")]
public sealed class DotPulsarProjectorTopicReaderFactory : IProjectorTopicReaderFactory
{
    private readonly IPulsarClient _client;
    private readonly string _topicNamespace;

    public DotPulsarProjectorTopicReaderFactory(IPulsarClient client, string topicNamespace)
    {
        _client = client;
        _topicNamespace = topicNamespace;
    }

    public async Task<TopicHighWatermark> CaptureHighWatermarkAsync(string topicName, CancellationToken cancellationToken)
    {
        await using var reader = _client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(_topicNamespace, topicName))
            .StartMessageId(MessageId.Latest)
            .ReadCompacted(true)
            .Create();

        if (reader is not IGetLastMessageIds lastMessageIdReader)
        {
            throw new InvalidOperationException("The configured Pulsar reader does not expose last-message-id lookup.");
        }

        var lastMessageIds = (await lastMessageIdReader.GetLastMessageIds(cancellationToken)).ToArray();
        Dictionary<int, MessageId> partitions = lastMessageIds.ToDictionary(messageId => messageId.Partition);
        return new TopicHighWatermark(topicName, partitions);
    }

    public Task<IProjectorTopicReader> CreateReaderAsync(string topicName,
                                                         int partitionId,
                                                         MessageId startMessageId,
                                                         CancellationToken cancellationToken)
    {
        var reader = _client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.Qualify(_topicNamespace, PulsarTopics.Partition(topicName, partitionId)))
            .StartMessageId(startMessageId)
            .ReadCompacted(true)
            .Create();

        return Task.FromResult<IProjectorTopicReader>(new DotPulsarProjectorTopicReader(topicName, partitionId, reader));
    }
}
