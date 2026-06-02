namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;

public delegate void TsavoriteScanCallback(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
public delegate void TsavoriteValueScanCallback(ReadOnlySpan<byte> value);


public interface ITsavoriteEngine : IDisposable
{
    Task CompleteWriteAsync(CancellationToken cancellationToken);

    ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateBasicSession();

    ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateRmwSession<TInput, TOutput, TFunctions>(TFunctions customFunctions)
        where TFunctions : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, Empty>;
    IDisposable DeferDurableCheckpoints();
    Task FlushAsync(CancellationToken cancellationToken);
    Task<Guid> CheckpointAsync(CancellationToken ct = default);

    void ScanByPrefix(ReadOnlySpan<byte> prefix, TsavoriteScanCallback callback);

    void ScanByPrefixGetValueOnly(ReadOnlySpan<byte> prefix, TsavoriteValueScanCallback callback);
}
