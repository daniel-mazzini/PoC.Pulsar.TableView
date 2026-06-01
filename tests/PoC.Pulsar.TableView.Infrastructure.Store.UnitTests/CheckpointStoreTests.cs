using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class CheckpointStoreTests
{
    [Fact]
    public void save_checkpoint_persists_last_message_id()
    {
        var store = new InMemoryStateStore<string, string>();
        var checkpoint = new PulsarMessageId(12, 34, 2);

        store.SaveCheckpoint(checkpoint);

        Assert.Equal(checkpoint, store.GetLastCheckpoint());
    }

    [Fact]
    public void last_checkpoint_starts_empty()
    {
        var store = new InMemoryStateStore<string, string>();

        Assert.Null(store.GetLastCheckpoint());
    }
}
