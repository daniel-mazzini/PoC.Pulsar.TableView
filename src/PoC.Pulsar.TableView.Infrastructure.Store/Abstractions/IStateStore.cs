namespace PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

public interface IStateStore<TKey, TValue> : ICheckpointStore
    where TKey : notnull
    where TValue : class
{
    TValue? Get(TKey key);

    void Upsert(TKey key, TValue value);

    bool Delete(TKey key);

    void Clear();

    IEnumerable<TValue> GetAll();
}
