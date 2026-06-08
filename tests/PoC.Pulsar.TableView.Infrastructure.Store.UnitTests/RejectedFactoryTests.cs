using System.Buffers;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.TableView;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class RejectedFactoryTests
{
    [Fact]
    public void create_from_payload_should_copy_original_message_metadata_and_headers()
    {
        var payload = new SportMessage
        {
            Id = "sport-1",
            Name = "Soccer",
            SportType = "SOCCER"
        };
        var reason = new RejectedReason("invalid-sport", "Sport payload is invalid.");
        var message = Message(
            key: "sport-1",
            headers: new Dictionary<string, string>
            {
                ["correlation-id"] = "correlation-1",
                ["causation-id"] = "causation-1",
                ["message-id"] = "message-1"
            });
        var before = DateTimeOffset.UtcNow;

        var rejected = RejectedFactory.CreateFromPayload(payload, message, reason);

        Assert.NotEqual(Guid.Empty, rejected.RejectedId);
        Assert.Equal("sports", rejected.OriginalTopic);
        Assert.Equal(1, rejected.OriginalPartitionId);
        Assert.Equal("sport-1", rejected.OriginalMessageKey);
        Assert.Equal(new PulsarMessageId(10, 20, 1, 0).ToString(), rejected.OriginalBrokerMessageId);
        Assert.Same(payload, rejected.OriginalPayload);
        Assert.Same(reason, rejected.Reason);
        Assert.True(rejected.RejectedAt >= before);
        Assert.True(rejected.RejectedAt <= DateTimeOffset.UtcNow);
        Assert.Equal("correlation-1", rejected.OriginalCorrelationId);
        Assert.Equal("causation-1", rejected.OriginalCausationId);
        Assert.Equal("message-1", rejected.OriginalMessageId);
    }

    [Fact]
    public void create_from_tombstone_should_use_null_payload_and_ignore_blank_headers()
    {
        var reason = new RejectedReason("missing-key", "Tombstone key is missing.");
        var message = Message(
            key: "sport-1",
            headers: new Dictionary<string, string>
            {
                ["correlation-id"] = " ",
                ["causation-id"] = "",
                ["message-id"] = "message-1"
            });

        var rejected = RejectedFactory.CreateFromTombStone<SportMessage>(message, reason);

        Assert.Null(rejected.OriginalPayload);
        Assert.Same(reason, rejected.Reason);
        Assert.Null(rejected.OriginalCorrelationId);
        Assert.Null(rejected.OriginalCausationId);
        Assert.Equal("message-1", rejected.OriginalMessageId);
    }

    private static TableViewMessage Message(string key, IReadOnlyDictionary<string, string> headers)
        => new(
            TopicName: "sports",
            PartitionId: 1,
            Key: key,
            Data: ReadOnlySequence<byte>.Empty,
            BrokerMessageId: new PulsarMessageId(10, 20, 1, 0),
            Properties: headers,
            PhysicalTopicName: "sports-partition-1",
            IsPartitioned: true);
}
