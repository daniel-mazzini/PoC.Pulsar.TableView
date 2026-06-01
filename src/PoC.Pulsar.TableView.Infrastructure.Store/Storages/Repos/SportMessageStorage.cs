using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

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