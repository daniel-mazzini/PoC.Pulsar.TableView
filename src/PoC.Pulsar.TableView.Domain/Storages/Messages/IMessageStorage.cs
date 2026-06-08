using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Projector;

namespace PoC.Pulsar.TableView.Domain.Storages.Entities;

public interface IMessageStorage<TKey, TMessage>
{
    ValueTask DeleteAsync(TKey id, CancellationToken cancellationToken);
    ValueTask ClearAsync(CancellationToken cancellationToken);
    ValueTask<TMessage?> TryLoadAsync(TKey id, CancellationToken cancellationToken);
    ValueTask UpsertAsync(TMessage message, CancellationToken cancellationToken);
    ValueTask<TableMessageApplyDecision> TryApplyAsync(TMessage message, CancellationToken cancellationToken);

    Dictionary<string, TMessage> GetAll(IValuePredicate<TMessage>? valuePredicate = null);
}


