using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class RejectedStorageTests
{
    [Fact]
    public async Task rejected_storage_should_persist_rejected_projection()
    {
        using var context = new TsavoriteIntegrationContext(nameof(rejected_storage_should_persist_rejected_projection));
        var storage = context.CreateRejectedStorage();
        var projection = IntegrationTestData.RejectedProjection("message-1");

        await storage.SaveRejectedRecordAsync(projection, CancellationToken.None);
        var stored = context.ReadSingleByPrefix<PoC.Pulsar.TableView.Domain.Rejected.RejectedProjection>(StorageKey.RejectedRecord(projection.MessageKey).Value);

        Assert.NotNull(stored);
        Assert.Equal(projection.MessageKey, stored.MessageKey);
        Assert.Equal(projection.Reason.Code, stored.Reason.Code);
    }
}
