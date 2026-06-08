using DotPulsar;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class TopicHighWatermarkTests
{
    [Fact]
    public void has_messages_should_reflect_whether_shard_watermarks_exist()
    {
        var empty = new TopicHighWatermark("sports", []);
        var withWatermark = new TopicHighWatermark(
            "sports",
            [new TopicShardHighWatermark(TopicShard.Partition("sports", 0), MessageId.Latest)]);

        Assert.False(empty.HasMessages);
        Assert.True(withWatermark.HasMessages);
    }

    [Fact]
    public void get_shard_high_watermark_should_return_matching_message_id()
    {
        var shard = TopicShard.Partition("sports", 0);
        var highWatermark = new TopicHighWatermark(
            "sports",
            [new TopicShardHighWatermark(shard, MessageId.Latest)]);

        var result = highWatermark.GetShardHighWatermarkOrThrow(shard);

        Assert.Equal(MessageId.Latest, result);
    }

    [Fact]
    public void get_shard_high_watermark_should_throw_when_shard_is_missing()
    {
        var highWatermark = new TopicHighWatermark(
            "sports",
            [new TopicShardHighWatermark(TopicShard.Partition("sports", 0), MessageId.Latest)]);

        Assert.Throws<ArgumentNullException>(
            () => highWatermark.GetShardHighWatermarkOrThrow(TopicShard.Partition("sports", 1)));
    }

    [Fact]
    public void shards_should_project_watermark_shards()
    {
        var shard0 = TopicShard.Partition("sports", 0);
        var shard1 = TopicShard.Partition("sports", 1);
        var highWatermark = new TopicHighWatermark(
            "sports",
            [
                new TopicShardHighWatermark(shard0, MessageId.Latest),
                new TopicShardHighWatermark(shard1, MessageId.Latest)
            ]);

        Assert.Equal([shard0, shard1], highWatermark.Shards);
    }
}
