using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using PoC.Pulsar.TableView.Domain.TableView;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

[ExcludeFromCodeCoverage(Justification = "Integration adapter around real DotPulsar readers.")]
public sealed class DotPulsarProjectorTopicReaderFactory : ITopicShardReaderStrategy
{
    private readonly IPulsarClient _client;
    private readonly string _topicNamespace;

    public DotPulsarProjectorTopicReaderFactory(IPulsarClient client, string topicNamespace)
    {
        _client = client;
        _topicNamespace = topicNamespace;
    }

    public async Task<TopicHighWatermark> CaptureHighWatermarkAsync(string logicalTopic, CancellationToken cancellationToken)
    {
        await using var reader = CreateLogicalReader(logicalTopic, MessageId.Latest);

        if (reader is not IGetLastMessageIds lastMessageIdReader)
        {
            throw new InvalidOperationException("The configured Pulsar reader does not expose last-message-id lookup.");
        }

        var watermarks = (await lastMessageIdReader.GetLastMessageIds(cancellationToken))
            .Where(messageId => messageId.EntryId >= 0)
            .Select(messageId => new TopicShardHighWatermark(ToShard(logicalTopic, messageId), messageId))
            .ToArray();

        return new TopicHighWatermark(logicalTopic, watermarks);
    }

    public async Task<IReadOnlyCollection<TopicShard>> DiscoverShardsAsync(string logicalTopic, CancellationToken cancellationToken)
    {
        await using var reader = CreateLogicalReader(logicalTopic, MessageId.Latest);

        if (reader is not IGetLastMessageIds lastMessageIdReader)
        {
            throw new InvalidOperationException("The configured Pulsar reader does not expose last-message-id lookup.");
        }

        var shards = (await lastMessageIdReader.GetLastMessageIds(cancellationToken))
            .Select(messageId => ToShard(logicalTopic, messageId))
            .Distinct()
            .OrderBy(shard => shard.PartitionId)
            .ToArray();

        return shards.Length > 0 ? shards : [TopicShard.NonPartitioned(logicalTopic)];
    }

    public Task<IProjectorTopicReader> CreateReaderAsync(
        TopicShard shard,
        MessageId startMessageId,
        CancellationToken cancellationToken)
    {
        var reader = _client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.QualifyIfNeeded(_topicNamespace, shard.PhysicalTopic))
            .StartMessageId(startMessageId)
            .ReadCompacted(true)
            .Create();

        return Task.FromResult<IProjectorTopicReader>(new DotPulsarProjectorTopicReader(shard, reader));
    }

    private IReader<byte[]> CreateLogicalReader(string logicalTopic, MessageId startMessageId)
        => _client.NewReader(Schema.ByteArray)
            .Topic(PulsarTopics.QualifyIfNeeded(_topicNamespace, logicalTopic))
            .StartMessageId(startMessageId)
            .ReadCompacted(true)
            .Create();

    private static TopicShard ToShard(string logicalTopic, MessageId messageId)
    {
        if (messageId.Partition < 0)
        {
            return TopicShard.NonPartitioned(logicalTopic);
        }

        string physicalTopic = string.IsNullOrWhiteSpace(messageId.Topic)
            ? PulsarTopics.Partition(logicalTopic, messageId.Partition)
            : messageId.Topic;

        return new TopicShard(logicalTopic, physicalTopic, messageId.Partition, IsPartitioned: true);
    }
}
