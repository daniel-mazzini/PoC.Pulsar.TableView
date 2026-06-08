using System.Diagnostics;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class SportMessageStorageTests
{
    private readonly ITestOutputHelper _output;

    public SportMessageStorageTests(ITestOutputHelper output)
    {
        _output = output;
    }


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
    public async Task try_apply_async_should_create_when_message_does_not_exist()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_create_when_message_does_not_exist));
        var storage = context.CreateSportMessageStorage();
        var message = IntegrationTestData.Sport("sport-1", version: 1);

        var result = await storage.TryApplyAsync(message, CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.Created, result.Kind);
        Assert.Equal(message.Version, (await storage.TryLoadAsync(message.Id, CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task try_apply_async_should_return_noop_when_incoming_version_is_not_greater()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_return_noop_when_incoming_version_is_not_greater));
        var storage = context.CreateSportMessageStorage();
        var existing = IntegrationTestData.Sport("sport-1", version: 5);
        await storage.UpsertAsync(existing, CancellationToken.None);

        var result = await storage.TryApplyAsync(IntegrationTestData.Sport("sport-1", version: 5), CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.NoOp, result.Kind);
        Assert.Equal("incoming_version_not_greater_than_current", result.Reason);
        Assert.Equal(5, (await storage.TryLoadAsync("sport-1", CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task try_apply_async_should_update_when_incoming_version_is_greater()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_update_when_incoming_version_is_greater));
        var storage = context.CreateSportMessageStorage();
        var existing = IntegrationTestData.Sport("sport-1", version: 1);
        await storage.UpsertAsync(existing, CancellationToken.None);

        var incoming = IntegrationTestData.Sport("sport-1", version: 2);
        var result = await storage.TryApplyAsync(incoming, CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.Updated, result.Kind);
        Assert.Equal(2, (await storage.TryLoadAsync("sport-1", CancellationToken.None))!.Version);
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
        _output.WriteLine($"write 10_000 messages elapsed: {stopwatch.Elapsed} ms");

        var all = storage.GetAll();

        Assert.Equal(10_000, all.Count);
        Assert.True(stopwatch.Elapsed <= write_time_threshold, $"Expected writes to finish within {write_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task sport_message_storage_should_read_10_000_messages_in_memory_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_read_10_000_messages_in_memory_within_time_threshold));
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
        _output.WriteLine($"read 10_000 messages in memory elapsed: {stopwatch.Elapsed}");

        Assert.True(stopwatch.Elapsed <= read_time_threshold, $"Expected reads to finish within {read_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task sport_message_storage_should_read_10_000_messages_persisted_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_message_storage_should_read_10_000_messages_persisted_within_time_threshold));
        var read_time_threshold = TimeSpan.FromSeconds(1);
        var write_time_threshold = TimeSpan.FromSeconds(1);

        var uow_writer_stopwatch = Stopwatch.StartNew();
        long writer_in_memory_milliseconds = 0;
        using (var uow = context.CreateSportUnitOfWork())
        {
            var writer_in_memory_stopwatch = Stopwatch.StartNew();
            for (var index = 0; index < 10_000; index++)
            {
                await uow.MessageStorage.UpsertAsync(IntegrationTestData.Sport($"sport-{index}", index), CancellationToken.None);
            }
            writer_in_memory_stopwatch.Stop();
            _output.WriteLine($"write 10_000 messages in unit of work elapsed: {writer_in_memory_stopwatch.Elapsed}");
            writer_in_memory_milliseconds = writer_in_memory_stopwatch.ElapsedMilliseconds;
        }
        uow_writer_stopwatch.Stop();

        Assert.True(uow_writer_stopwatch.Elapsed <= write_time_threshold, $"Expected writes to finish within {write_time_threshold}, but took {uow_writer_stopwatch.Elapsed}.");

        var reader_stopwatch = Stopwatch.StartNew();
        using (var uow = context.CreateSportUnitOfWork())
        {
            for (var index = 0; index < 10_000; index++)
            {
                var loaded = await uow.MessageStorage.TryLoadAsync($"sport-{index}", CancellationToken.None);
                Assert.NotNull(loaded);
            }
            reader_stopwatch.Stop();
            _output.WriteLine($"read 10_000 persisted messages elapsed: {reader_stopwatch.Elapsed}");
            Assert.True(reader_stopwatch.Elapsed <= read_time_threshold, $"Expected reads to finish within {read_time_threshold}, but took {reader_stopwatch.Elapsed}.");
            await uow.CommitAsync(CancellationToken.None);
        }

        _output.WriteLine($"unit of work total elapsed: {uow_writer_stopwatch.Elapsed}");
        _output.WriteLine($"commit time: {uow_writer_stopwatch.Elapsed - TimeSpan.FromMilliseconds(writer_in_memory_milliseconds)}");
    }

    [Fact]
    public async Task try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates()
    {
        const int messageCount = 10_000;

        using var legacyContext = new TsavoriteIntegrationContext(nameof(try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates) + "_legacy");
        using var rmwContext = new TsavoriteIntegrationContext(nameof(try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates) + "_rmw");

        var legacyStorage = legacyContext.CreateSportMessageStorage();
        var rmwStorage = rmwContext.CreateSportMessageStorage();

        for (var index = 0; index < messageCount; index++)
        {
            var seeded = IntegrationTestData.Sport($"sport-{index}", version: 1);
            await legacyStorage.UpsertAsync(seeded, CancellationToken.None);
            await rmwStorage.UpsertAsync(seeded, CancellationToken.None);
        }

        var legacyStopwatch = Stopwatch.StartNew();
        for (var index = 0; index < messageCount; index++)
        {
            var incoming = IntegrationTestData.Sport($"sport-{index}", version: 2);
            var current = await legacyStorage.TryLoadAsync(incoming.Id, CancellationToken.None);
            if (current is null || incoming.Version > current.Version)
            {
                await legacyStorage.UpsertAsync(incoming, CancellationToken.None);
            }
        }
        legacyStopwatch.Stop();

        var rmwStopwatch = Stopwatch.StartNew();
        for (var index = 0; index < messageCount; index++)
        {
            await rmwStorage.TryApplyAsync(IntegrationTestData.Sport($"sport-{index}", version: 2), CancellationToken.None);
        }
        rmwStopwatch.Stop();

        _output.WriteLine($"legacy TryLoad+Upsert elapsed: {legacyStopwatch.Elapsed}");
        _output.WriteLine($"rmw TryApply elapsed: {rmwStopwatch.Elapsed}");

        Assert.Equal(messageCount, legacyStorage.GetAll().Count);
        Assert.Equal(messageCount, rmwStorage.GetAll().Count);
    }

    [Fact]
    public async Task try_apply_async_should_return_noop_when_incoming_version_is_lower()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_return_noop_when_incoming_version_is_lower));
        var storage = context.CreateSportMessageStorage();
        await storage.UpsertAsync(IntegrationTestData.Sport("sport-1", version: 7), CancellationToken.None);

        var result = await storage.TryApplyAsync(IntegrationTestData.Sport("sport-1", version: 6), CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.NoOp, result.Kind);
        Assert.Equal("incoming_version_not_greater_than_current", result.Reason);
        Assert.Equal(7, (await storage.TryLoadAsync("sport-1", CancellationToken.None))!.Version);
    }
}
