using Xunit;

namespace PoC.Pulsar.TableView.Contracts.UnitTests;

public sealed class RejectedMessageTests
{
    [Fact]
    public void sport_rejected_message_should_store_original_payload_and_reason()
    {
        var reason = new RejectedReasonMessage("invalid_payload", "Payload is invalid.");
        var rejectedAt = new DateTimeOffset(2024, 01, 01, 0, 0, 0, TimeSpan.Zero);
        var payload = new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 1,
            SportType = "team"
        };

        var message = new SportRejectedMessage(
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            "sports",
            3,
            "message-1",
            "sport-1",
            reason,
            payload,
            rejectedAt,
            "correlation-1",
            "causation-1",
            "message-id-1");

        Assert.Equal(payload, message.OriginalPayload);
        Assert.Equal(reason, message.Reason);
        Assert.Equal("sports", message.OriginalTopic);
        Assert.Equal(3, message.OriginalPartitionId);
        Assert.Equal(rejectedAt, message.RejectedAt);
    }

    [Fact]
    public void raw_category_rejected_message_should_store_original_payload_and_reason()
    {
        var reason = new RejectedReasonMessage("invalid_payload", "Payload is invalid.");
        var payload = new RawCategoryMessage
        {
            Id = "category-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Premier League",
            Version = 1,
            SportId = "sport-1"
        };

        var message = new RawCategoryRejectedMessage(
            Guid.Parse("22222222-2222-2222-2222-222222222222"),
            "categories",
            4,
            "message-2",
            "category-1",
            reason,
            payload,
            DateTimeOffset.UtcNow,
            null,
            null,
            null);

        Assert.Equal(payload, message.OriginalPayload);
        Assert.Equal(reason, message.Reason);
        Assert.Equal("categories", message.OriginalTopic);
        Assert.Equal(4, message.OriginalPartitionId);
    }
}
