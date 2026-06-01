using PoC.Pulsar.TableView.Domain.Filter;

namespace PoC.Pulsar.TableView.Domain.Storages;

public interface IPulsarTableView<TMessage>
     where TMessage : class
{
    IObservable<Event<TMessage>> OnUpdate { get; }

    ValueTask<TMessage?> GetAsync(string key, CancellationToken cancellationToken);

    IDictionary<string, TMessage> GetLoadedOnBoostrap();
    IDictionary<string, TMessage> GetLoadedOnBoostrapFilterBy(IValuePredicate<TMessage> filter);

    Task StartBootstrapAsync(CancellationToken cancellationToken);

    Task StartLiveTailAsync(CancellationToken cancellationToken);
}




