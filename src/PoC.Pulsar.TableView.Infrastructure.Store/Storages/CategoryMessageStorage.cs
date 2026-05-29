using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;

public sealed class CategoryMessageStorage : TsavoriteRepositoryBase, ICategoryMessageStorage
{
    private readonly ITsavoriteEngine _engine;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> _session;
    private bool _disposed;

    public CategoryMessageStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _session = engine.CreateBasicSession();
    }

    public async ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await DeleteFromSessionAsync(_session,
                                     StorageKey.CategoryMessage(id),
                                     cancellationToken);
    }


    public async ValueTask<RawCategoryMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        return await ReadFromSessionAsync<RawCategoryMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(_session,
                                                                                                                     StorageKey.CategoryMessage(id),
                                                                                                                     cancellationToken);
    }

    public async ValueTask UpsertAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await UpsertIntoSessionAsync(_session,
                                     StorageKey.SportMessage(message.Id),
                                     default,
                                     message,
                                     cancellationToken);
    }
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _session.Dispose();
        _disposed = true;
    }
}
