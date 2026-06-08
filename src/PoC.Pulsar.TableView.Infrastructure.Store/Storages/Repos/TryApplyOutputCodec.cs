using System.Buffers.Binary;
using System.Text;
using PoC.Pulsar.TableView.Domain.Projector;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

internal static class TryApplyOutputCodec
{
    public static SpanByteAndMemory Serialize(TableMessageApplyDecision result)
    {
        var reasonBytes = result.Reason is null
            ? []
            : Encoding.UTF8.GetBytes(result.Reason);

        var length = 2 * sizeof(int) + reasonBytes.Length;

        var pinnedBytes = GC.AllocateUninitializedArray<byte>(length, pinned: true);
        BinaryPrimitives.WriteInt32LittleEndian(pinnedBytes.AsSpan(0, sizeof(int)), (int)result.Kind);
        BinaryPrimitives.WriteInt32LittleEndian(pinnedBytes.AsSpan(sizeof(int), sizeof(int)), reasonBytes.Length);
        if (reasonBytes.Length > 0)
        {
            reasonBytes.CopyTo(pinnedBytes.AsSpan(2 * sizeof(int)));
        }

        return SpanByteAndMemory.FromPinnedSpan(pinnedBytes);
    }

    public static TableMessageApplyDecision Deserialize(SpanByteAndMemory output)
    {
        var span = output.AsReadOnlySpan();
        if (span.Length == 0)
        {
            throw new InvalidOperationException("Apply decision output was empty.");
        }

        if (span.Length < 2 * sizeof(int))
        {
            throw new InvalidOperationException("Apply decision output was truncated.");
        }

        var kind = (TableMessageApplyKind)BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0, sizeof(int)));
        var reasonLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(sizeof(int), sizeof(int)));
        if (reasonLength < 0 || span.Length < 2 * sizeof(int) + reasonLength)
        {
            throw new InvalidOperationException("Apply decision output reason was truncated.");
        }

        var reason = reasonLength == 0
            ? string.Empty
            : Encoding.UTF8.GetString(span.Slice(2 * sizeof(int), reasonLength));

        return new TableMessageApplyDecision(kind, reason);
    }
}
