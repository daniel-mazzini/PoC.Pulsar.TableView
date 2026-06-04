using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class CheckpointStorageTests
{
    [Fact]
    public async Task checkpoint_storage_should_persist_and_reload_topic_checkpoint()
    {
        using var context = new TsavoriteIntegrationContext(nameof(checkpoint_storage_should_persist_and_reload_topic_checkpoint));
        var storage = context.CreateCheckpointStorage();
        var topic = "persistent://public/tableview-inputs/sports";
        var messageId = IntegrationTestData.PulsarMessageId(1, 5);

        await storage.SaveCheckpointAsync(topic, 0, messageId, CancellationToken.None);
        var checkpoint = await storage.GetLastCheckpoint(topic, 0, CancellationToken.None);

        Assert.NotNull(checkpoint);
        Assert.Equal(topic, checkpoint.TopicName);
        Assert.Equal(messageId, checkpoint.LastProcessedMessageId);
        Assert.NotEqual(Guid.Empty, checkpoint.StoreId);
    }

    [Fact]
    public async Task checkpoint_storage_should_persist_and_reload_view_checkpoint()
    {
        using var context = new TsavoriteIntegrationContext(nameof(checkpoint_storage_should_persist_and_reload_view_checkpoint));
        var storage = context.CreateCheckpointStorage();

        await storage.SaveViewCheckpointAsync("taxonomy-view", CancellationToken.None);
        var checkpoint = await storage.GetViewCheckpointAsync("taxonomy-view", CancellationToken.None);

        Assert.NotNull(checkpoint);
        Assert.Equal("taxonomy-view", checkpoint.ViewName);
        Assert.True(checkpoint.BuildCompleted);
        Assert.False(string.IsNullOrWhiteSpace(checkpoint.StoreId));
    }
}
