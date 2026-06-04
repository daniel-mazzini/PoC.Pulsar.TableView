using System.Diagnostics;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class SportMessageStorageTests
{
    [Fact]
    public async Task sport_message_storage_should_upsert_and_load_message()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_upsert_and_load_message));
        var storage = context.CreateSportMessageStorage();
        var message = IntegrationTestData.Sport("sport-1");

        await storage.UpsertAsync(message, CancellationToken.None);
        var loaded = await storage.TryLoadAsync(message.Id, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(message.Id, loaded.Id);
        Assert.Equal(message.Version, loaded.Version);
    }

    [Fact]
    public async Task sport_message_storage_should_clear_only_sport_records()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_clear_only_sport_records));
        var sportStorage = context.CreateSportMessageStorage();
        var checkpointStorage = context.CreateCheckpointStorage();
        var shard = TopicShard.Partition("persistent://public/tableview-inputs/sports", 0);

        await sportStorage.UpsertAsync(IntegrationTestData.Sport("sport-1"), CancellationToken.None);
        await checkpointStorage.SaveCheckpointAsync(shard, IntegrationTestData.PulsarMessageId(), CancellationToken.None);

        await sportStorage.ClearAsync(CancellationToken.None);

        Assert.Null(await sportStorage.TryLoadAsync("sport-1", CancellationToken.None));
        Assert.NotNull(await checkpointStorage.GetLastCheckpoint(shard, CancellationToken.None));
    }

    [Fact]
    public async Task sport_message_storage_should_write_10_000_messages_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_write_10_000_messages_within_time_threshold));
        var storage = context.CreateSportMessageStorage();
        var write_time_threshold = TimeSpan.FromSeconds(10);

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            await storage.UpsertAsync(IntegrationTestData.Sport($"sport-{index}", index), CancellationToken.None);
        }
        stopwatch.Stop();

        var all = storage.GetAll();

        Assert.Equal(10_000, all.Count);
        Assert.True(stopwatch.Elapsed <= write_time_threshold, $"Expected writes to finish within {write_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task sport_message_storage_should_read_10_000_messages_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_read_10_000_messages_within_time_threshold));
        var storage = context.CreateSportMessageStorage();
        var read_time_threshold = TimeSpan.FromSeconds(10);

        for (var index = 0; index < 10_000; index++)
        {
            await storage.UpsertAsync(IntegrationTestData.Sport($"sport-{index}", index), CancellationToken.None);
        }

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            var loaded = await storage.TryLoadAsync($"sport-{index}", CancellationToken.None);
            Assert.NotNull(loaded);
        }
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed <= read_time_threshold, $"Expected reads to finish within {read_time_threshold}, but took {stopwatch.Elapsed}.");
    }
}
