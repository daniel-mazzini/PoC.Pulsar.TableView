using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Inspection;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages;

public sealed class TsavoriteViewerTests
{
    [Fact]
    public async Task list_should_scan_sports_by_known_prefix_and_deserialize_values()
    {
        using var context = new TsavoriteIntegrationContext(nameof(list_should_scan_sports_by_known_prefix_and_deserialize_values));
        var storage = context.CreateSportMessageStorage();
        await storage.UpsertAsync(IntegrationTestData.Sport("sport-1", version: 3), CancellationToken.None);
        await storage.UpsertAsync(IntegrationTestData.Sport("sport-2", version: 4), CancellationToken.None);
        var viewer = new TsavoriteViewer(context.Engine, context.StateSerializer);

        var entries = viewer.List("sports", limit: 1);

        var entry = Assert.Single(entries);
        Assert.StartsWith(StorageKey.SportMessagePrefix, entry.StorageKey);
        Assert.Equal("sports", entry.Type);
        Assert.IsType<SportMessage>(entry.Value);
    }

    [Fact]
    public async Task get_should_build_storage_key_from_logical_sport_id()
    {
        using var context = new TsavoriteIntegrationContext(nameof(get_should_build_storage_key_from_logical_sport_id));
        var storage = context.CreateSportMessageStorage();
        await storage.UpsertAsync(IntegrationTestData.Sport("sport-1", version: 3), CancellationToken.None);
        var viewer = new TsavoriteViewer(context.Engine, context.StateSerializer);

        var entry = viewer.Get("sports", "sport-1");

        Assert.NotNull(entry);
        Assert.Equal(StorageKey.SportMessage("sport-1").Value, entry!.StorageKey);
        Assert.Equal("sport-1", entry.LogicalKey);
        var value = Assert.IsType<SportMessage>(entry.Value);
        Assert.Equal("sport-1", value.Id);
    }

    [Fact]
    public async Task list_should_scan_categories_by_known_prefix_and_deserialize_values()
    {
        using var context = new TsavoriteIntegrationContext(nameof(list_should_scan_categories_by_known_prefix_and_deserialize_values));
        var storage = context.CreateCategoryMessageStorage();
        await storage.UpsertAsync(IntegrationTestData.Category("category-1", "sport-1"), CancellationToken.None);
        var viewer = new TsavoriteViewer(context.Engine, context.StateSerializer);

        var entry = Assert.Single(viewer.List("categories", limit: 100));

        Assert.StartsWith(StorageKey.CategoryMessagePrefix.Value, entry.StorageKey);
        Assert.IsType<RawCategoryMessage>(entry.Value);
    }

    [Fact]
    public async Task list_should_deserialize_rejected_records()
    {
        using var context = new TsavoriteIntegrationContext(nameof(list_should_deserialize_rejected_records));
        var storage = context.CreateRejectedStorage();
        await storage.SaveRejectedRecordAsync(IntegrationTestData.RejectedProjection("message-1"), CancellationToken.None);
        var viewer = new TsavoriteViewer(context.Engine, context.StateSerializer);

        var entry = Assert.Single(viewer.List("rejected", limit: 100));

        Assert.StartsWith(StorageKey.RejectedRecordPrefix, entry.StorageKey);
        Assert.IsType<RejectedProjection>(entry.Value);
    }

    [Fact]
    public async Task list_should_deserialize_store_metadata()
    {
        using var context = new TsavoriteIntegrationContext(nameof(list_should_deserialize_store_metadata));
        await context.MetadataStorage.EnsureMetadataAsync(CancellationToken.None);
        var viewer = new TsavoriteViewer(context.Engine, context.StateSerializer);

        var entry = Assert.Single(viewer.List("store-metadata", limit: 100));

        Assert.Equal(StorageKey.StoreMetadata, entry.StorageKey);
        Assert.IsType<StoreMetadata>(entry.Value);
    }
}
