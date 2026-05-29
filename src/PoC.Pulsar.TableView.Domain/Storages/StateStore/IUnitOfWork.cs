namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public interface IUnitOfWork :IDisposable
{
    Task CommitAsync(CancellationToken ct);
}
