namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;

public interface ITsavoriteEngine : IDisposable
{
    Task CompleteWriteAsync(CancellationToken cancellationToken);

    ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateBasicSession();

    ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateRmwSession<TInput, TOutput, TFunctions>(TFunctions customFunctions)
        where TFunctions : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, Empty>;
    IDisposable DeferDurableCheckpoints();
    Task FlushAsync(CancellationToken cancellationToken);
    Task<Guid> CheckpointAsync(CancellationToken ct = default);

}
