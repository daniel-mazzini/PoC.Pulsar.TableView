using System.Buffers;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Projector;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

internal sealed class TryApplySportMessageFunctions : SessionFunctionsBase<SpanByte, SpanByte, TryApplySportMessageCommand, SpanByteAndMemory, Empty>
{
    private const string IncomingVersionNotGreaterThanCurrent = "incoming_version_not_greater_than_current";
    private readonly IStateSerializer _serializer;

    public TryApplySportMessageFunctions(IStateSerializer serializer)
    {
        _serializer = serializer;
    }

    public override bool InitialUpdater(ref SpanByte key,
                                        ref TryApplySportMessageCommand input,
                                        ref SpanByte value,
                                        ref SpanByteAndMemory output,
                                        ref RMWInfo rmwInfo,
                                        ref RecordInfo recordInfo)
    {
        if (!input.SerializedValue.TryCopyTo(ref value))
        {
            return false;
        }

        output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.Created());
        return true;
    }

    public override void PostInitialUpdater(ref SpanByte key,
                                            ref TryApplySportMessageCommand input,
                                            ref SpanByte value,
                                            ref SpanByteAndMemory output,
                                            ref RMWInfo rmwInfo)
    {
        output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.Created());
    }

    public override bool NeedInitialUpdate(ref SpanByte key,
                                           ref TryApplySportMessageCommand input,
                                           ref SpanByteAndMemory output,
                                           ref RMWInfo rmwInfo)
        => true;

    public override bool NeedCopyUpdate(ref SpanByte key,
                                        ref TryApplySportMessageCommand input,
                                        ref SpanByte oldValue,
                                        ref SpanByteAndMemory output,
                                        ref RMWInfo rmwInfo)
    {
        var current = _serializer.Deserialize<SportMessage>(oldValue.AsReadOnlySpan());
        return input.IncomingVersion > current.Version && input.SerializedValue.Length > oldValue.Length;
    }

    public override bool InPlaceUpdater(ref SpanByte key,
                                        ref TryApplySportMessageCommand input,
                                        ref SpanByte value,
                                        ref SpanByteAndMemory output,
                                        ref RMWInfo rmwInfo,
                                        ref RecordInfo recordInfo)
    {
        var current = _serializer.Deserialize<SportMessage>(value.AsReadOnlySpan());
        if (input.IncomingVersion <= current.Version)
        {
            output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.NoOp(IncomingVersionNotGreaterThanCurrent));
            return true;
        }

        if (!input.SerializedValue.TryCopyTo(ref value))
        {
            return false;
        }

        output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.Updated());
        return true;
    }

    public override bool CopyUpdater(ref SpanByte key,
                                     ref TryApplySportMessageCommand input,
                                     ref SpanByte oldValue,
                                     ref SpanByte newValue,
                                     ref SpanByteAndMemory output,
                                     ref RMWInfo rmwInfo,
                                     ref RecordInfo recordInfo)
    {
        var current = _serializer.Deserialize<SportMessage>(oldValue.AsReadOnlySpan());
        if (input.IncomingVersion <= current.Version)
        {
            if (!oldValue.TryCopyTo(ref newValue))
            {
                return false;
            }

            output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.NoOp(IncomingVersionNotGreaterThanCurrent));
            return true;
        }

        if (!input.SerializedValue.TryCopyTo(ref newValue))
        {
            return false;
        }

        output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.Updated());
        return true;
    }

    public override bool PostCopyUpdater(ref SpanByte key,
                                         ref TryApplySportMessageCommand input,
                                         ref SpanByte oldValue,
                                         ref SpanByte newValue,
                                         ref SpanByteAndMemory output,
                                         ref RMWInfo rmwInfo)
    {
        output = TryApplyOutputCodec.Serialize(TableMessageApplyDecision.Updated());
        return true;
    }

    public override int GetRMWInitialValueLength(ref TryApplySportMessageCommand input)
        => input.SerializedValue.Length;

    public override int GetRMWModifiedValueLength(ref SpanByte oldValue, ref TryApplySportMessageCommand input)
        => Math.Max(input.SerializedValue.Length, oldValue.Length);

    public override void ConvertOutputToHeap(ref TryApplySportMessageCommand input, ref SpanByteAndMemory output)
    {
        if (output.IsSpanByte)
        {
            output.CopyFrom(output.AsReadOnlySpan(), MemoryPool<byte>.Shared);
        }
    }
}
