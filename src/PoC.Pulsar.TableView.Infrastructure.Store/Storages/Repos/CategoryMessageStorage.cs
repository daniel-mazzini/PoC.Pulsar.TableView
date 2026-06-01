using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class CategoryMessageStorage : TsavoriteRepositoryBase, ICategoryMessageStorage
{
    private readonly ITsavoriteSessionProvider _sessionProvider;
    private bool _disposed;
    private readonly bool _ownsSession;

    public CategoryMessageStorage(ITsavoriteEngine engine, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(engine);
        _sessionProvider = new TsavoriteSessionWrapper(engine);
        _ownsSession = true;
    }

    public CategoryMessageStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await DeleteFromSessionAsync(session,
                                      StorageKey.CategoryMessage(id),
                                      cancellationToken);
    }


    public async ValueTask<RawCategoryMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<RawCategoryMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                      StorageKey.CategoryMessage(id),
                                                                                                                      cancellationToken);
    }

    public async ValueTask UpsertAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.CategoryMessage(message.Id),
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

        if (_ownsSession)
        {
            _sessionProvider.Dispose();
        }
        _disposed = true;
    }
}
