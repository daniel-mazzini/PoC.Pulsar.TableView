using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class InMemoryStateStoreTests
{
    [Fact]
    public void upsert_stores_value_and_get_returns_it()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");

        Assert.Equal("Football", store.Get("sport-1"));
    }

    [Fact]
    public async Task delete_removes_value_from_store()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");

        var deleted = store.Delete("sport-1");

        Assert.True(deleted);
        Assert.Null(store.Get("sport-1"));
        Assert.Empty(await ToListAsync(store.GetAllAsync()));
    }

    [Fact]
    public async Task clear_removes_all_values()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");

        store.Clear();

        Assert.Null(store.Get("sport-1"));
        Assert.Null(store.Get("sport-2"));
        Assert.Empty(await ToListAsync(store.GetAllAsync()));
    }

    [Fact]
    public async Task get_all_returns_current_snapshot_of_values()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");

        var values = await ToListAsync(store.GetAllAsync());

        Assert.Equal(2, values.Count);
        Assert.Contains("Football", values);
        Assert.Contains("Tennis", values);
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();

        await foreach (var value in source)
        {
            values.Add(value);
        }

        return values;
    }
}
