using System.Buffers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using BasicSession = ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;
public class RelationIndexStorage : TsavoriteRepositoryBase, IDisposable
{
    private readonly ITsavoriteEngine _engine;
    private readonly BasicSession _session;
    private bool _disposed;

    public RelationIndexStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _session = engine.CreateLightSession();
    }

    public async ValueTask<IReadOnlyList<string>> LoadStringRelationAsync(StorageKey key, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        return await ReadFromSessionAsync<string[], SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(_session,
                                                                                                           key.Value,
                                                                                                           cancellationToken) ?? [];
    }
    public async ValueTask AddStringAsync(StorageKey key, string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var current = await LoadStringRelationAsync(key, cancellationToken);
        await UpsertRelationAsync(key, StoredListMerge.Merge(current, id).ToArray(), cancellationToken);
    }

    public async ValueTask RemoveStringAsync(StorageKey key, string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var current = await LoadStringRelationAsync(key, cancellationToken);
        await UpsertRelationAsync(key, StoredListMerge.Remove(current, id).ToArray(), cancellationToken);
    }

    private async ValueTask UpsertRelationAsync<T>(StorageKey key, T[] values, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await UpsertIntoSessionAsync(_session,
                                     key.Value,
                                     default,
                                     values,
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
