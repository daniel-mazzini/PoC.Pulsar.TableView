using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public abstract class TsavoriteUnitOfWorkBase : IUnitOfWork
{
    protected readonly ITsavoriteEngine Engine;
    private readonly IDisposable _checkpointScope;
    protected readonly TsavoriteSessionWrapper SessionWrapper;
    private bool _isDisposed;
    private bool _isCommitted;

    protected TsavoriteUnitOfWorkBase(ITsavoriteEngine engine)
    {
        Engine = engine;
        _checkpointScope = Engine.DeferDurableCheckpoints();
        SessionWrapper = new TsavoriteSessionWrapper(Engine);
    }

    public async Task CommitAsync(CancellationToken ct)
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(TsavoriteUnitOfWorkBase));
        }
        if (_isCommitted)
        {
            return;
        }

        await Engine.CompleteWriteAsync(ct);

        _checkpointScope.Dispose();

        await Engine.FlushAsync(ct);

        _isCommitted = true;
    }

    public void Dispose()
    {
        if (_isDisposed) return;

        // we must still release the _checkpointScope if no commit was made, to reduce the counter and avoid blocking
        if (!_isCommitted)
        {
            _checkpointScope.Dispose();
        }

        // Close all Tsavorite session opened inside de UoW
        SessionWrapper.Dispose();

        _isDisposed = true;

        // Opcional, but quick release
        GC.SuppressFinalize(this);
    }
}
