using System.Diagnostics;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.UnitOfWorks;

public sealed class SportTableViewUnitOfWorkTests
{
    private readonly ITestOutputHelper _output;

    public SportTableViewUnitOfWorkTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task sport_unit_of_work_should_read_written_message_before_commit()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_unit_of_work_should_read_written_message_before_commit));
        using var unitOfWork = context.CreateSportUnitOfWork();
        var message = IntegrationTestData.Sport("sport-before-commit", 4);

        await unitOfWork.MessageStorage.UpsertAsync(message, CancellationToken.None);
        var loaded = await unitOfWork.MessageStorage.TryLoadAsync(message.Id, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(message.Version, loaded.Version);
    }

    [Fact]
    public async Task unit_of_work_should_read_written_checkpoint_before_commit()
    {
        using var context = new TsavoriteIntegrationContext(nameof(unit_of_work_should_read_written_checkpoint_before_commit));
        using var unitOfWork = context.CreateSportUnitOfWork();
        var topic = "persistent://public/tableview-inputs/sports";
        var shard = TopicShard.Partition(topic, 0);
        var messageId = IntegrationTestData.PulsarMessageId(2, 7);

        await unitOfWork.CheckpointStorage.SaveCheckpointAsync(shard, messageId, CancellationToken.None);
        var checkpoint = await unitOfWork.CheckpointStorage.GetLastCheckpoint(shard, CancellationToken.None);

        Assert.NotNull(checkpoint);
        Assert.Equal(messageId, checkpoint.LastProcessedMessageId);
    }

    [Fact]
    public async Task sport_unit_of_work_commit_should_persist_message_for_new_engine_instance()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(sport_unit_of_work_commit_should_persist_message_for_new_engine_instance));
        var serializer = new PoC.Pulsar.TableView.Infrastructure.Store.Serialization.MemoryPackWrapper();

        using (var engine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath))
        using (var unitOfWork = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks.SportTableViewUnitOfWork(engine, new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.MetadataStorage(engine, serializer), serializer))
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Sport("sport-committed", 9), CancellationToken.None);
            await unitOfWork.CommitAsync(CancellationToken.None);
        }

        using var reopenedEngine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath);
        var reopenedStorage = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.SportMessageStorage(new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session.TsavoriteSessionWrapper(reopenedEngine), serializer);

        var loaded = await reopenedStorage.TryLoadAsync("sport-committed", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(9, loaded.Version);
    }

    [Fact]
    public async Task unit_of_work_should_persist_checkpoint_and_message_in_same_commit()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(unit_of_work_should_persist_checkpoint_and_message_in_same_commit));
        var serializer = new PoC.Pulsar.TableView.Infrastructure.Store.Serialization.MemoryPackWrapper();
        var topic = "persistent://public/tableview-inputs/sports";
        var shard = TopicShard.Partition(topic, 0);
        var messageId = IntegrationTestData.PulsarMessageId(3, 10);

        using (var engine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath))
        using (var unitOfWork = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks.SportTableViewUnitOfWork(engine, new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.MetadataStorage(engine, serializer), serializer))
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Sport("sport-checkpoint", 2), CancellationToken.None);
            await unitOfWork.CheckpointStorage.SaveCheckpointAsync(shard, messageId, CancellationToken.None);
            await unitOfWork.CommitAsync(CancellationToken.None);
        }

        using var reopenedEngine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath);
        var reopenedMetadata = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.MetadataStorage(reopenedEngine, serializer);
        using var reopenedUnitOfWork = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks.SportTableViewUnitOfWork(reopenedEngine, reopenedMetadata, serializer);

        var loaded = await reopenedUnitOfWork.MessageStorage.TryLoadAsync("sport-checkpoint", CancellationToken.None);
        var checkpoint = await reopenedUnitOfWork.CheckpointStorage.GetLastCheckpoint(shard, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.NotNull(checkpoint);
        Assert.Equal(messageId, checkpoint.LastProcessedMessageId);
    }

    [Fact]
    public async Task sport_unit_of_work_should_write_10_000_messages_without_commit_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_unit_of_work_should_write_10_000_messages_without_commit_within_time_threshold));
        using var unitOfWork = context.CreateSportUnitOfWork();
        var write_time_threshold = TimeSpan.FromSeconds(10);

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Sport($"sport-{index}", index), CancellationToken.None);
        }
        stopwatch.Stop();
        _output.WriteLine($"write 10_000 pending messages elapsed: {stopwatch.Elapsed}");

        var loaded = await unitOfWork.MessageStorage.TryLoadAsync("sport-9999", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(9999, loaded.Version);
        Assert.True(stopwatch.Elapsed <= write_time_threshold, $"Expected pending writes to finish within {write_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task sport_unit_of_work_should_read_10_000_pending_messages_without_commit_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_unit_of_work_should_read_10_000_pending_messages_without_commit_within_time_threshold));
        using var unitOfWork = context.CreateSportUnitOfWork();
        var pending_read_time_threshold = TimeSpan.FromSeconds(10);

        for (var index = 0; index < 10_000; index++)
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Sport($"sport-{index}", index), CancellationToken.None);
        }

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            var loaded = await unitOfWork.MessageStorage.TryLoadAsync($"sport-{index}", CancellationToken.None);
            Assert.NotNull(loaded);
        }
        stopwatch.Stop();
        _output.WriteLine($"read 10_000 pending messages elapsed: {stopwatch.Elapsed}");

        Assert.True(stopwatch.Elapsed <= pending_read_time_threshold, $"Expected pending reads to finish within {pending_read_time_threshold}, but took {stopwatch.Elapsed}.");
    }
}
