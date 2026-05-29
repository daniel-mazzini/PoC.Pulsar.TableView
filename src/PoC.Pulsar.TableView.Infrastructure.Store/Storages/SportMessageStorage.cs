using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
public sealed class MetadataStorage : TsavoriteRepositoryBase, IMetadataStorage
{
    private readonly ITsavoriteEngine _engine;
    private readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> _session;
    private bool _disposed;
    private StoreMetadata? _metadata = null;

    public MetadataStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _session = engine.CreateBasicSession();
    }

    public async ValueTask<StoreMetadata> EnsureMetadataAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        if (_metadata is not null)
        {
            return _metadata;
        }

        var loadedMetadata = await TryLoadMetadataAsync(cancellationToken);
        if (loadedMetadata is not null)
        {
            _metadata = loadedMetadata!;
            return loadedMetadata;
        }

        var createdMetadata = StoreMetadata.CreateDefault();
        await SaveMetadataAsync(createdMetadata, cancellationToken);
        _metadata = createdMetadata;
        return createdMetadata;
    }

    private async ValueTask SaveMetadataAsync(StoreMetadata createdMetadata, CancellationToken cancellationToken)
    {
        await UpsertIntoSessionAsync(_session,
                                             StorageKey.StoreMetadata,
                                             default,
                                             createdMetadata,
                                             cancellationToken);
    }

    /// <summary>
    /// Reads the metadata record if it exists and matches the supported schema and required identity fields.
    /// </summary>
    public async ValueTask<StoreMetadata?> TryLoadMetadataAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var metadata = await ReadFromSessionAsync<StoreMetadata, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(_session,
                                                                                                               StorageKey.StoreMetadata,
                                                                                                               cancellationToken);
        return IsValidMetadata(metadata) ? metadata : null;
    }
    /// <summary>
    /// Set IsBoostrapCompleted in metadata to true
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async ValueTask MarkBootstrapCompletedAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var metadata = await EnsureMetadataAsync(cancellationToken);
        if (metadata.IsBoostrapCompleted)
        {
            return;
        }

        var completedMetadata = metadata with { IsBoostrapCompleted = true };
        await SaveMetadataAsync(completedMetadata, cancellationToken);
        _metadata = completedMetadata;
    }
    private static bool IsValidMetadata(StoreMetadata? metadata)
        => metadata is not null
            && metadata.SchemaVersion == 1
            && metadata.StoreGenerationId != Guid.Empty
            && metadata.CreatedAt != default;

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

public sealed class SportMessageStorage : TsavoriteRepositoryBase, ISportMessageStorage
{
    private bool _disposed;
    private readonly ITsavoriteSessionProvider _sessionProvider;

    public SportMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask<SportMessage?> TryLoadAsync(string sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<SportMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                               StorageKey.SportMessage(sportId),
                                                                                                               cancellationToken);
    }

    public async ValueTask UpsertAsync(SportMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync<SportMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                            StorageKey.SportMessage(message.Id),
                                                                                                            default,
                                                                                                            message,
                                                                                                            cancellationToken);
    }

    public async ValueTask DeleteAsync(string sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await DeleteFromSessionAsync<SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                   StorageKey.SportMessage(sportId),
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

        _disposed = true;
    }


}