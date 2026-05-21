namespace PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

public interface IPulsarTableView<TValue>
    where TValue : class
{
    TValue? Get(string key);

    IEnumerable<TValue> GetAll();

    Task StartBootstrapAsync(CancellationToken cancellationToken = default);
}
