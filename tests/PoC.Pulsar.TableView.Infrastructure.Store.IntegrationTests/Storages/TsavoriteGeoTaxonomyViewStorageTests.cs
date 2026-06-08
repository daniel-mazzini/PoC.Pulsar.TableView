using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages;

public sealed class TsavoriteGeoTaxonomyViewStorageTests
{
    [Fact]
    public async Task upsert_view_async_should_assign_initial_calculated_version_and_leave_published_version_unset()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_view_async_should_assign_initial_calculated_version_and_leave_published_version_unset));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var result = await storage.UpsertViewAsync(sportId,
                                                   GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                                   "build-00000000000000000000000000000001",
                                                   CancellationToken.None);

        Assert.Equal(1L, result.CalculatedVersion);
        Assert.Equal(0L, result.PublishedVersion);
        Assert.True(result.HasPendingPublish);
        Assert.Equal(1, result.View.Version);

        var view = await storage.GetViewAsync(sportId, CancellationToken.None);
        Assert.NotNull(view);
        Assert.Equal(1, view!.Version);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(1L, metadata!.CalculatedVersion);
        Assert.Equal(0L, metadata.PublishedVersion);
        Assert.True(metadata.HasPendingPublish);
    }

    [Fact]
    public async Task mark_view_published_async_should_advance_published_version_only_after_confirmation()
    {
        using var context = new TsavoriteIntegrationContext(nameof(mark_view_published_async_should_advance_published_version_only_after_confirmation));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var result = await storage.UpsertViewAsync(sportId,
                                                   GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                                   "build-00000000000000000000000000000001",
                                                   CancellationToken.None);

        await storage.MarkViewPublishedAsync(sportId, result.CalculatedVersion, result.BuildGenerationId, CancellationToken.None);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(1L, metadata!.PublishedVersion);
        Assert.False(metadata.HasPendingPublish);
        Assert.NotNull(metadata.PublishedAtUtc);
    }

    [Fact]
    public async Task upsert_sport_async_should_version_changes_and_keep_unchanged_values_stable()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_sport_async_should_version_changes_and_keep_unchanged_values_stable));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var created = await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);
        Assert.True(created.ViewExists);
        Assert.True(created.Changed);
        Assert.NotNull(created.View);
        Assert.Equal(1, created.View!.Version);

        var unchanged = await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);
        Assert.True(unchanged.ViewExists);
        Assert.False(unchanged.Changed);
        Assert.NotNull(unchanged.View);
        Assert.Equal(1, unchanged.View!.Version);

        var updated = await storage.UpsertSportAsync(sportId, "Soccer Updated", "SOCCER", CancellationToken.None);
        Assert.True(updated.ViewExists);
        Assert.True(updated.Changed);
        Assert.NotNull(updated.View);
        Assert.Equal(2, updated.View!.Version);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(2L, metadata!.CalculatedVersion);
        Assert.Equal(0L, metadata.PublishedVersion);
    }

    [Fact]
    public async Task upsert_category_async_should_require_existing_view_and_replace_matching_category()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_category_async_should_require_existing_view_and_replace_matching_category));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        var category = new GeoTaxonomyNode("category-1", "US");

        var missing = await storage.UpsertCategoryAsync(sportId, category, CancellationToken.None);
        Assert.False(missing.ViewExists);
        Assert.False(missing.Changed);
        Assert.Null(missing.View);

        await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);

        var created = await storage.UpsertCategoryAsync(sportId, category, CancellationToken.None);
        Assert.True(created.ViewExists);
        Assert.True(created.Changed);
        Assert.NotNull(created.View);
        Assert.Contains(category, created.View!.GeoCategories);

        var unchanged = await storage.UpsertCategoryAsync(sportId, category, CancellationToken.None);
        Assert.True(unchanged.ViewExists);
        Assert.False(unchanged.Changed);
        Assert.NotNull(unchanged.View);
        Assert.Single(unchanged.View!.GeoCategories);

        var updated = await storage.UpsertCategoryAsync(sportId, new GeoTaxonomyNode("category-1", "ES"), CancellationToken.None);
        Assert.True(updated.ViewExists);
        Assert.True(updated.Changed);
        Assert.NotNull(updated.View);
        Assert.Contains(new GeoTaxonomyNode("category-1", "ES"), updated.View!.GeoCategories);
        Assert.DoesNotContain(new GeoTaxonomyNode("category-1", "US"), updated.View.GeoCategories);
    }

    [Fact]
    public async Task remove_view_async_and_clear_async_should_delete_view_and_metadata_entries()
    {
        using var context = new TsavoriteIntegrationContext(nameof(remove_view_async_and_clear_async_should_delete_view_and_metadata_entries));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);
        await storage.UpsertCategoryAsync(sportId, new GeoTaxonomyNode("category-1", "US"), CancellationToken.None);

        var removed = await storage.RemoveViewAsync(sportId, CancellationToken.None);
        Assert.NotNull(removed);
        Assert.Null(await storage.GetViewAsync(sportId, CancellationToken.None));
        Assert.Null(await storage.GetMetadataAsync(sportId, CancellationToken.None));

        await storage.UpsertSportAsync(new SportId("sport-2"), "Basketball", "BASKETBALL", CancellationToken.None);
        await storage.ClearAsync(CancellationToken.None);

        Assert.Null(await storage.GetViewAsync(new SportId("sport-2"), CancellationToken.None));
        Assert.Null(await storage.GetMetadataAsync(new SportId("sport-2"), CancellationToken.None));
    }

    private static SportMessage Sport(string id, string name = "Soccer", string sportType = "SOCCER")
        => new()
        {
            Id = id,
            Name = name,
            SportType = sportType,
            Provider = "provider",
            EntityCoverage = "covered"
        };
}
