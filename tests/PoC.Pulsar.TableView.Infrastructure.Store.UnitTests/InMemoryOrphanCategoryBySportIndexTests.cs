using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class InMemoryOrphanCategoryBySportIndexTests
{
    [Fact]
    public async Task get_categories_waiting_for_sport_async_should_return_real_category_ids_when_storage_key_is_sanitized()
    {
        var index = new InMemoryOrphanCategoryBySportIndex();
        var sportId = new SportId("sport:live");

        await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                        new CategoryId("soccer:int"),
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        var categories = await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None);

        var category = Assert.Single(categories);
        Assert.Equal("soccer:int", category.Value);
    }

    [Fact]
    public async Task get_missing_sports_for_category_async_should_return_real_sport_ids_when_storage_key_is_sanitized()
    {
        var index = new InMemoryOrphanCategoryBySportIndex();
        var categoryId = new CategoryId("soccer:int");

        await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport:live"),
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        var sports = await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None);

        var sport = Assert.Single(sports);
        Assert.Equal("sport:live", sport.Value);
    }

    [Fact]
    public async Task pending_ids_with_sanitized_equivalent_values_should_not_collide()
    {
        var index = new InMemoryOrphanCategoryBySportIndex();
        var sportId = new SportId("sport:live");

        await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                        new CategoryId("soccer:int"),
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);
        await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                        new CategoryId("soccer-int"),
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        var categories = await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None);

        Assert.Equal(["soccer-int", "soccer:int"], categories.Select(category => category.Value).Order());
    }

    [Fact]
    public async Task remove_category_from_pending_async_should_remove_both_relation_directions_using_real_ids()
    {
        var index = new InMemoryOrphanCategoryBySportIndex();
        var sportId = new SportId("sport:live");
        var categoryId = new CategoryId("soccer:int");
        await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        await index.RemoveCategoryFromPendingAsync(categoryId, CancellationToken.None);

        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None));
    }
}
