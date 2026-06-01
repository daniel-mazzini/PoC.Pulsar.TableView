namespace PoC.Pulsar.TableView.Domain.Storages.Entities;

public interface IMessageStorage<TKey, TMessage>
{
    ValueTask DeleteAsync(TKey id, CancellationToken cancellationToken);
    ValueTask<TMessage?> TryLoadAsync(TKey id, CancellationToken cancellationToken);
    ValueTask UpsertAsync(TMessage message, CancellationToken cancellationToken);
}


