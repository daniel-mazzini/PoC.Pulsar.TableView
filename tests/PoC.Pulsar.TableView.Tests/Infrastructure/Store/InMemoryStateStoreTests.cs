using PoC.Pulsar.TableView.Infrastructure.Store;
using Xunit;

namespace PoC.Pulsar.TableView.Tests.Infrastructure.Store;

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
    public void delete_removes_value_from_store()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");

        var deleted = store.Delete("sport-1");

        Assert.True(deleted);
        Assert.Null(store.Get("sport-1"));
        Assert.Empty(store.GetAll());
    }

    [Fact]
    public void clear_removes_all_values()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");

        store.Clear();

        Assert.Null(store.Get("sport-1"));
        Assert.Null(store.Get("sport-2"));
        Assert.Empty(store.GetAll());
    }

    [Fact]
    public void get_all_returns_current_snapshot_of_values()
    {
        var store = new InMemoryStateStore<string, string>();

        store.Upsert("sport-1", "Football");
        store.Upsert("sport-2", "Tennis");

        var values = store.GetAll().ToArray();

        Assert.Equal(2, values.Length);
        Assert.Contains("Football", values);
        Assert.Contains("Tennis", values);
    }
}
