using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class CategoryMessageStorageTests
{
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
}
