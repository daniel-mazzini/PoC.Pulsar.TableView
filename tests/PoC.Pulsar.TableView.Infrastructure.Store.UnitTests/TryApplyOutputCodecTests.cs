using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using Tsavorite.core;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class TryApplyOutputCodecTests
{
    [Theory]
    [InlineData(TableMessageApplyKind.Created, null, "")]
    [InlineData(TableMessageApplyKind.Updated, "", "")]
    [InlineData(TableMessageApplyKind.NoOp, "sport already contains the same data", "sport already contains the same data")]
    public void serialized_apply_decision_should_round_trip_kind_and_reason(
        TableMessageApplyKind kind,
        string? reason,
        string expectedReason)
    {
        var serialized = TryApplyOutputCodec.Serialize(new TableMessageApplyDecision(kind, reason));

        var result = TryApplyOutputCodec.Deserialize(serialized);

        Assert.Equal(kind, result.Kind);
        Assert.Equal(expectedReason, result.Reason);
    }

    [Fact]
    public void deserialize_should_reject_truncated_header()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => TryApplyOutputCodec.Deserialize(SpanByteAndMemory.FromPinnedSpan([1, 0, 0, 0])));

        Assert.Equal("Apply decision output was truncated.", exception.Message);
    }

    [Fact]
    public void deserialize_should_reject_truncated_reason()
    {
        var bytes = GC.AllocateUninitializedArray<byte>(8, pinned: true);
        BitConverter.GetBytes((int)TableMessageApplyKind.NoOp).CopyTo(bytes, 0);
        BitConverter.GetBytes(1).CopyTo(bytes, 4);

        var exception = Assert.Throws<InvalidOperationException>(
            () => TryApplyOutputCodec.Deserialize(SpanByteAndMemory.FromPinnedSpan(bytes)));

        Assert.Equal("Apply decision output reason was truncated.", exception.Message);
    }
}
