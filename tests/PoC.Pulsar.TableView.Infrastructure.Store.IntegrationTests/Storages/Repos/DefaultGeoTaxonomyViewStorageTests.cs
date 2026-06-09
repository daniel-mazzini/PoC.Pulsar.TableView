using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class DefaultGeoTaxonomyViewStorageTests
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

    [Fact]
    public async Task upsert_sport_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_sport_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var results = await Task.WhenAll(Enumerable.Range(1, 50)
                                                  .Select(index => storage.UpsertSportAsync(sportId,
                                                                                            $"Soccer {index}",
                                                                                            $"TYPE-{index}",
                                                                                            CancellationToken.None)
                                                                        .AsTask()));

        var view = await storage.GetViewAsync(sportId, CancellationToken.None);
        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);

        Assert.NotNull(view);
        Assert.NotNull(metadata);
        Assert.Equal(50, view!.Version);
        Assert.Equal(50L, metadata!.CalculatedVersion);
        Assert.Equal(Enumerable.Range(1, 50), results.Select(result => result.View!.Version).Order());
    }

    [Fact]
    public async Task upsert_view_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_view_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var results = await Task.WhenAll(Enumerable.Range(1, 50)
                                                  .Select(index => storage.UpsertViewAsync(sportId,
                                                                                           GeoTaxonomyViewMessage.CreateNew(sportId.Value, $"Soccer {index}", $"TYPE-{index}"),
                                                                                           $"build-{index:D32}",
                                                                                           CancellationToken.None)
                                                                        .AsTask()));

        var view = await storage.GetViewAsync(sportId, CancellationToken.None);
        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);

        Assert.NotNull(view);
        Assert.NotNull(metadata);
        Assert.Equal(50, view!.Version);
        Assert.Equal(50L, metadata!.CalculatedVersion);
        Assert.Equal(Enumerable.Range(1, 50), results.Select(result => (int)result.CalculatedVersion).Order());
    }

    [Fact]
    public async Task mark_view_published_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(mark_view_published_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        const string buildGenerationId = "build-00000000000000000000000000000001";

        for (var index = 1; index <= 50; index++)
        {
            await storage.UpsertViewAsync(sportId,
                                          GeoTaxonomyViewMessage.CreateNew(sportId.Value, $"Soccer {index}", $"TYPE-{index}"),
                                          buildGenerationId,
                                          CancellationToken.None);
        }

        await Task.WhenAll(Enumerable.Range(1, 50)
                                     .Select(index => storage.MarkViewPublishedAsync(sportId,
                                                                                      index,
                                                                                      buildGenerationId,
                                                                                      CancellationToken.None)
                                                           .AsTask()));

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);

        Assert.NotNull(metadata);
        Assert.Equal(50L, metadata!.CalculatedVersion);
        Assert.Equal(50L, metadata.PublishedVersion);
        Assert.NotNull(metadata.PublishedAtUtc);
        Assert.False(metadata.HasPendingPublish);
    }

    [Fact]
    public async Task upsert_category_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(upsert_category_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);

        var results = await Task.WhenAll(Enumerable.Range(1, 100)
                                                  .Select(index => storage.UpsertCategoryAsync(sportId,
                                                                                               new GeoTaxonomyNode($"category-{index}", $"C{index:D2}"),
                                                                                               CancellationToken.None)
                                                                        .AsTask()));

        var view = await storage.GetViewAsync(sportId, CancellationToken.None);
        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);

        Assert.NotNull(view);
        Assert.NotNull(metadata);
        Assert.Equal(101, view!.Version);
        Assert.Equal(100, view.GeoCategories.Count);
        Assert.Equal(101L, metadata!.CalculatedVersion);
        Assert.All(results, result => Assert.True(result.Changed));
    }

    [Fact]
    public async Task remove_category_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(remove_category_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);

        for (var index = 1; index <= 100; index++)
        {
            await storage.UpsertCategoryAsync(sportId, new GeoTaxonomyNode($"category-{index}", $"C{index:D2}"), CancellationToken.None);
        }

        var results = await Task.WhenAll(Enumerable.Range(1, 100)
                                                  .Select(index => storage.RemoveCategoryAsync(sportId,
                                                                                               new CategoryId($"category-{index}"),
                                                                                               CancellationToken.None)
                                                                        .AsTask()));

        var view = await storage.GetViewAsync(sportId, CancellationToken.None);
        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);

        Assert.NotNull(view);
        Assert.NotNull(metadata);
        Assert.Equal(201, view!.Version);
        Assert.Empty(view.GeoCategories);
        Assert.Equal(201L, metadata!.CalculatedVersion);
        Assert.All(results, result => Assert.True(result.Changed));
    }

    [Fact]
    public async Task remove_view_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(remove_view_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        await storage.UpsertSportAsync(sportId, "Soccer", "SOCCER", CancellationToken.None);
        await storage.UpsertCategoryAsync(sportId, new GeoTaxonomyNode("category-1", "US"), CancellationToken.None);

        var removedViews = await Task.WhenAll(Enumerable.Range(0, 20)
                                                       .Select(_ => storage.RemoveViewAsync(sportId, CancellationToken.None).AsTask()));

        Assert.Single(removedViews, view => view is not null);
        Assert.Null(await storage.GetViewAsync(sportId, CancellationToken.None));
        Assert.Null(await storage.GetMetadataAsync(sportId, CancellationToken.None));
    }

    [Fact]
    public async Task clear_async_should_serialize_concurrent_mutations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(clear_async_should_serialize_concurrent_mutations));
        using var storage = context.CreateGeoTaxonomyViewStorage();

        for (var index = 1; index <= 100; index++)
        {
            var sportId = new SportId($"sport-{index}");
            await storage.UpsertSportAsync(sportId, $"Sport {index}", $"TYPE-{index}", CancellationToken.None);
            await storage.UpsertCategoryAsync(sportId, new GeoTaxonomyNode($"category-{index}", $"C{index:D2}"), CancellationToken.None);
        }

        await Task.WhenAll(Enumerable.Range(0, 20)
                                     .Select(_ => storage.ClearAsync(CancellationToken.None).AsTask()));

        Assert.Empty(context.ReadAllByPrefix<GeoTaxonomyViewMessage>(StorageKey.CountryTaxonomyMaterializedViewPrefix));
        Assert.Empty(context.ReadAllByPrefix<GeoTaxonomyViewMetadata>(StorageKey.GeoTaxonomyViewMetadataPrefix));
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
