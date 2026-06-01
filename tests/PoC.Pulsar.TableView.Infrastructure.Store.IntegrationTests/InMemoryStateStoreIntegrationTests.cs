using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests;

public sealed class InMemoryStateStoreIntegrationTests
{
    [Fact]
    public async Task state_store_should_persist_values_and_checkpoint_through_store_contract()
    {
        IStateStore<string, string> store = new InMemoryStateStore<string, string>();
        var checkpoint = new PulsarMessageId(3, 21, 0);

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");
        store.SaveCheckpoint(checkpoint);

        var values = await ToListAsync(store.GetAllAsync());

        Assert.Equal("Football", store.Get("sport-1"));
        Assert.Contains("Football", values);
        Assert.Contains("Tennis", values);
        Assert.Equal(checkpoint, store.GetLastCheckpoint());
    }

    [Fact]
    public async Task state_store_should_apply_delete_and_clear_through_store_contract()
    {
        IStateStore<string, string> store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");

        var deleted = store.Delete("sport-1");
        store.Clear();

        Assert.True(deleted);
        Assert.Null(store.Get("sport-1"));
        Assert.Null(store.Get("sport-2"));
        Assert.Empty(await ToListAsync(store.GetAllAsync()));
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
