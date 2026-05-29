using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

public sealed class SportTableViewUnitOfWork : TsavoriteUnitOfWorkBase, ITableViewUnitOfWork<SportMessage>
{
    public IMessageStorage<string, SportMessage> MessageStorage { get; }

    public ICheckpointStorage CheckpointStorage => throw new NotImplementedException();

    public SportTableViewUnitOfWork(TsavoriteEngine engine, IStateSerializer serializer)
        : base(engine)
    {

        MessageStorage = new SportMessageStorage(SessionWrapper, serializer);
    }
    public Task CommitAsync(CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }
}

public abstract class TsavoriteUnitOfWorkBase : IUnitOfWork
{
    protected readonly TsavoriteEngine Engine;
    private readonly IDisposable _checkpointScope;
    private readonly TsavoriteSessionWrapper SessionWrapper;
    private bool _isDisposed;
    private bool _isCommitted; 

    protected TsavoriteUnitOfWorkBase(TsavoriteEngine engine)
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
