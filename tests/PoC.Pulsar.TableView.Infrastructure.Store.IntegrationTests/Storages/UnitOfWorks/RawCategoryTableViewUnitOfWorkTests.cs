using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.UnitOfWorks;

public sealed class RawCategoryTableViewUnitOfWorkTests
{
    [Fact]
    public async Task category_unit_of_work_should_read_written_message_before_commit()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_unit_of_work_should_read_written_message_before_commit));
        using var unitOfWork = context.CreateCategoryUnitOfWork();
        var message = IntegrationTestData.Category("category-before-commit", "sport-1", version: 5);

        await unitOfWork.MessageStorage.UpsertAsync(message, CancellationToken.None);
        var loaded = await unitOfWork.MessageStorage.TryLoadAsync(message.Id, CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(message.Version, loaded.Version);
    }

    [Fact]
    public async Task category_unit_of_work_commit_should_persist_message_for_new_engine_instance()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(category_unit_of_work_commit_should_persist_message_for_new_engine_instance));
        var serializer = new PoC.Pulsar.TableView.Infrastructure.Store.Serialization.MemoryPackWrapper();

        using (var engine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath))
        using (var unitOfWork = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks.RawCategoryTableViewUnitOfWork(engine, new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.MetadataStorage(engine, serializer), serializer))
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Category("category-committed", "sport-1", version: 6), CancellationToken.None);
            await unitOfWork.CommitAsync(CancellationToken.None);
        }

        using var reopenedEngine = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.TsavoriteEngine(storeScope.StorePath);
        using var reopenedStorage = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.CategoryMessageStorage(reopenedEngine, serializer);

        var loaded = await reopenedStorage.TryLoadAsync("category-committed", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal("sport-1", loaded.SportId);
    }
}
