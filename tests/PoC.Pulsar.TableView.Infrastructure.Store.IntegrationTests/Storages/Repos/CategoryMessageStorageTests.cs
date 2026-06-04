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
}
