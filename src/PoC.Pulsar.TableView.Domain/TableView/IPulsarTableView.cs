using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Projector;

namespace PoC.Pulsar.TableView.Domain.TableView;

public interface IPulsarTableView<TMessage>
     where TMessage : class
{
    IObservable<TableEntryChange<TMessage>> OnChanges { get; }

    ValueTask<TMessage?> GetEntry(string key, CancellationToken cancellationToken);

    IDictionary<string, TMessage> GetSnapshot(IValuePredicate<TMessage>? filter = null);

    Task<TopicBootstrapResult<TMessage>> StartBootstrapAsync(CancellationToken cancellationToken = default);

    Task StartLiveTailAsync(CancellationToken cancellationToken);
}

