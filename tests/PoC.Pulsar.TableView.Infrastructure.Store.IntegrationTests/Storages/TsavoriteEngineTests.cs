using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages;

public sealed class TsavoriteEngineTests
{
    [Fact]
    public async Task tsavorite_engine_should_recover_data_after_checkpoint_and_reopen()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(tsavorite_engine_should_recover_data_after_checkpoint_and_reopen));
        var serializer = new PoC.Pulsar.TableView.Infrastructure.Store.Serialization.MemoryPackWrapper();

        using (var engine = new TsavoriteEngine(storeScope.StorePath))
        using (var unitOfWork = new PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks.SportTableViewUnitOfWork(engine, new MetadataStorage(engine, serializer), serializer))
        {
            await unitOfWork.MessageStorage.UpsertAsync(IntegrationTestData.Sport("sport-1", 3), CancellationToken.None);
            await unitOfWork.CommitAsync(CancellationToken.None);
        }

        using var reopenedEngine = new TsavoriteEngine(storeScope.StorePath);
        var reopenedStorage = new SportMessageStorage(new PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session.TsavoriteSessionWrapper(reopenedEngine), serializer);

        var loaded = await reopenedStorage.TryLoadAsync("sport-1", CancellationToken.None);

        Assert.NotNull(loaded);
        Assert.Equal(3, loaded.Version);
    }
}
