namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;

using StateSession = Tsavorite.core.ClientSession<
    Tsavorite.core.SpanByte,
    Tsavorite.core.SpanByte,
    Tsavorite.core.SpanByte,
    Tsavorite.core.SpanByteAndMemory,
    Tsavorite.core.Empty,
    Tsavorite.core.SpanByteFunctions<Tsavorite.core.Empty>,
    Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>,
    Tsavorite.core.SpanByteAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>>>;


internal interface ITsavoriteSessionProvider : IStateSession
{
    StateSession GetLightSession();
    ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> GetSession<TInput, TOutput, TFunctions>(TFunctions customFunctions = null)
        where TFunctions : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, Empty>;
}
