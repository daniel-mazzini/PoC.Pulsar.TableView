using System.Collections.Concurrent;
using PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public sealed class InMemoryStateStore<TKey, TValue> : IStateStore<TKey, TValue>
    where TKey : notnull
    where TValue : class
{
    private readonly ConcurrentDictionary<TKey, TValue> _values = new();
    private readonly object _checkpointGate = new();
    private PulsarMessageId? _lastCheckpoint;

    public TValue? Get(TKey key)
    {
        return _values.TryGetValue(key, out var value) ? value : null;
    }

    public void Upsert(TKey key, TValue value)
    {
        _values[key] = value;
    }

    public bool Delete(TKey key)
    {
        return _values.TryRemove(key, out _);
    }

    public void Clear()
    {
        _values.Clear();
    }

    public async IAsyncEnumerable<TValue> GetAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var value in _values.Values.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
            await Task.Yield();
        }
    }

    public void SaveCheckpoint(PulsarMessageId id)
    {
        lock (_checkpointGate)
        {
            _lastCheckpoint = id;
        }
    }

    public PulsarMessageId? GetLastCheckpoint()
    {
        lock (_checkpointGate)
        {
            return _lastCheckpoint;
        }
    }


}
