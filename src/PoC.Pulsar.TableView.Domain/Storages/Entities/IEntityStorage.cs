namespace PoC.Pulsar.TableView.Domain.Storages.Entities;

public interface IEntityStorage<TKey, TValue>
{
    ValueTask<TValue?> TryLoadAsync(TKey id, CancellationToken cancellationToken);
    ValueTask UpsertAsync(TValue entity, CancellationToken cancellationToken);
}


