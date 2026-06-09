using System.Diagnostics;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class CategoryMessageStorageTests
{
    private readonly ITestOutputHelper _output;

    public CategoryMessageStorageTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task category_message_storage_should_upsert_and_load_message()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_message_storage_should_upsert_and_load_message));
        using var storage = context.CreateCategoryMessageStorage();
        var message = IntegrationTestData.Category("category-1", "sport-1");

        await storage.UpsertAsync(message, CancellationToken.None);
        var loaded = await storage.TryLoadAsync(message.Id, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(message.Id, loaded.Id);
        Assert.Equal(message.SportId, loaded.SportId);
    }

    [Fact]
    public async Task try_apply_async_should_create_when_message_does_not_exist()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_create_when_message_does_not_exist));
        using var storage = context.CreateCategoryMessageStorage();
        var message = IntegrationTestData.Category("category-1", "sport-1", version: 1);

        var result = await storage.TryApplyAsync(message, CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.Created, result.Kind);
        Assert.Equal(1, (await storage.TryLoadAsync(message.Id, CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task try_apply_async_should_return_noop_when_incoming_version_is_not_greater()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_return_noop_when_incoming_version_is_not_greater));
        using var storage = context.CreateCategoryMessageStorage();
        var existing = IntegrationTestData.Category("category-1", "sport-1", version: 3);
        await storage.UpsertAsync(existing, CancellationToken.None);

        var result = await storage.TryApplyAsync(IntegrationTestData.Category("category-1", "sport-1", version: 3), CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.NoOp, result.Kind);
        Assert.Equal("incoming_version_not_greater_than_current", result.Reason);
        Assert.Equal(3, (await storage.TryLoadAsync("category-1", CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task try_apply_async_should_update_when_incoming_version_is_greater()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_update_when_incoming_version_is_greater));
        using var storage = context.CreateCategoryMessageStorage();
        var existing = IntegrationTestData.Category("category-1", "sport-1", version: 1);
        await storage.UpsertAsync(existing, CancellationToken.None);

        var incoming = IntegrationTestData.Category("category-1", "sport-1", version: 2);
        var result = await storage.TryApplyAsync(incoming, CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.Updated, result.Kind);
        Assert.Equal(2, (await storage.TryLoadAsync("category-1", CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task try_apply_async_should_return_noop_when_incoming_version_is_lower()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_apply_async_should_return_noop_when_incoming_version_is_lower));
        using var storage = context.CreateCategoryMessageStorage();
        await storage.UpsertAsync(IntegrationTestData.Category("category-1", "sport-1", version: 4), CancellationToken.None);

        var result = await storage.TryApplyAsync(IntegrationTestData.Category("category-1", "sport-1", version: 3), CancellationToken.None);

        Assert.Equal(TableMessageApplyKind.NoOp, result.Kind);
        Assert.Equal("incoming_version_not_greater_than_current", result.Reason);
        Assert.Equal(4, (await storage.TryLoadAsync("category-1", CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task category_message_storage_should_write_10_000_messages_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_message_storage_should_write_10_000_messages_within_time_threshold));
        using var storage = context.CreateCategoryMessageStorage();
        var write_time_threshold = TimeSpan.FromSeconds(10);

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            await storage.UpsertAsync(IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: index), CancellationToken.None);
        }
        stopwatch.Stop();
        _output.WriteLine($"write 10_000 messages elapsed: {stopwatch.Elapsed}");

        var all = storage.GetAll();

        Assert.Equal(10_000, all.Count);
        Assert.True(stopwatch.Elapsed <= write_time_threshold, $"Expected writes to finish within {write_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task category_message_storage_should_read_10_000_messages_in_memory_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_message_storage_should_read_10_000_messages_in_memory_within_time_threshold));
        using var storage = context.CreateCategoryMessageStorage();
        var read_time_threshold = TimeSpan.FromSeconds(10);

        for (var index = 0; index < 10_000; index++)
        {
            await storage.UpsertAsync(IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: index), CancellationToken.None);
        }

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < 10_000; index++)
        {
            var loaded = await storage.TryLoadAsync($"category-{index}", CancellationToken.None);
            Assert.NotNull(loaded);
        }
        stopwatch.Stop();
        _output.WriteLine($"read 10_000 messages in memory elapsed: {stopwatch.Elapsed}");

        Assert.True(stopwatch.Elapsed <= read_time_threshold, $"Expected reads to finish within {read_time_threshold}, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task category_message_storage_should_read_10_000_messages_persisted_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_message_storage_should_read_10_000_messages_persisted_within_time_threshold));
        var read_time_threshold = TimeSpan.FromSeconds(1);
        var write_time_threshold = TimeSpan.FromSeconds(1);

        var unit_of_work_writer_stopwatch = Stopwatch.StartNew();
        long writer_in_memory_milliseconds = 0;
        using (var unitOfWork = context.CreateCategoryUnitOfWork())
        {
            var writer_in_memory_stopwatch = Stopwatch.StartNew();
            for (var index = 0; index < 10_000; index++)
            {
                await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: index), CancellationToken.None);
            }
            writer_in_memory_stopwatch.Stop();
            _output.WriteLine($"write 10_000 messages in unit of work elapsed: {writer_in_memory_stopwatch.Elapsed}");
            writer_in_memory_milliseconds = writer_in_memory_stopwatch.ElapsedMilliseconds;
        }
        unit_of_work_writer_stopwatch.Stop();

        Assert.True(unit_of_work_writer_stopwatch.Elapsed <= write_time_threshold,
                    $"Expected writes to finish within {write_time_threshold}, but took {unit_of_work_writer_stopwatch.Elapsed}.");

        var reader_stopwatch = Stopwatch.StartNew();
        using (var unitOfWork = context.CreateCategoryUnitOfWork())
        {
            for (var index = 0; index < 10_000; index++)
            {
                var loaded = await unitOfWork.MessageStorage.TryLoadAsync($"category-{index}", CancellationToken.None);
                Assert.NotNull(loaded);
            }
            reader_stopwatch.Stop();
            _output.WriteLine($"read 10_000 persisted messages elapsed: {reader_stopwatch.Elapsed}");
            Assert.True(reader_stopwatch.Elapsed <= read_time_threshold,
                        $"Expected reads to finish within {read_time_threshold}, but took {reader_stopwatch.Elapsed}.");
            await unitOfWork.CommitAsync(CancellationToken.None);
        }

        _output.WriteLine($"unit of work total elapsed: {unit_of_work_writer_stopwatch.Elapsed}");
        _output.WriteLine($"commit time: {unit_of_work_writer_stopwatch.Elapsed - TimeSpan.FromMilliseconds(writer_in_memory_milliseconds)}");
    }

    [Fact]
    public async Task try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates_with_10_000_messages()
    {
        const int messageCount = 10_000;

        using var legacyContext = new TsavoriteIntegrationContext(nameof(try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates_with_10_000_messages) + "_legacy");
        using var rmwContext = new TsavoriteIntegrationContext(nameof(try_apply_async_should_compare_performance_against_try_load_plus_upsert_for_updates_with_10_000_messages) + "_rmw");

        using var legacyStorage = legacyContext.CreateCategoryMessageStorage();
        using var rmwStorage = rmwContext.CreateCategoryMessageStorage();

        for (var index = 0; index < messageCount; index++)
        {
            var seeded = IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: 1);
            await legacyStorage.UpsertAsync(seeded, CancellationToken.None);
            await rmwStorage.UpsertAsync(seeded, CancellationToken.None);
        }

        var legacyStopwatch = Stopwatch.StartNew();
        for (var index = 0; index < messageCount; index++)
        {
            var incoming = IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: 2);
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
            await rmwStorage.TryApplyAsync(IntegrationTestData.Category($"category-{index}", $"sport-{index}", version: 2), CancellationToken.None);
        }
        rmwStopwatch.Stop();

        _output.WriteLine($"legacy TryLoad+Upsert elapsed: {legacyStopwatch.Elapsed}");
        _output.WriteLine($"rmw TryApply elapsed: {rmwStopwatch.Elapsed}");

        Assert.Equal(messageCount, legacyStorage.GetAll().Count);
        Assert.Equal(messageCount, rmwStorage.GetAll().Count);
    }
}
