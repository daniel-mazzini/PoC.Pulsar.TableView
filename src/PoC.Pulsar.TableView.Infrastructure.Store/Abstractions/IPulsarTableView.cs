namespace PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

public interface IPulsarTableView<TValue>
    where TValue : class
{
    IObservable<Event<TValue>> OnUpdate { get; }

    TValue? Get(string key);

    IAsyncEnumerable<TValue> GetAllAsync(CancellationToken cancellationToken = default);

    Task StartBootstrapAsync(CancellationToken cancellationToken = default);

    Task StartLiveTailAsync(CancellationToken cancellationToken);
}
