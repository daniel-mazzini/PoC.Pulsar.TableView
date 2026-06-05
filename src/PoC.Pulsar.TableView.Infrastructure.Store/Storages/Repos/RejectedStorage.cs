using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public class RejectedStorage :TsavoriteRepositoryBase, IRejectedStorage
{
    private bool _disposed;
    private readonly ITsavoriteSessionProvider _sessionProvider;

    public RejectedStorage(IStateSession session, IStateSerializer serializer) : base(serializer)
    {
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.RejectedRecord(rejectedProjection.MessageKey),
                                     default,
                                     rejectedProjection,
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
