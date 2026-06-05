using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages;

public sealed class TsavoriteCategoryPendingIndexTests
{
    [Fact]
    public async Task get_categories_waiting_for_sport_async_should_return_real_category_ids_when_key_is_encoded()
    {
        using var context = new TsavoriteIntegrationContext(nameof(get_categories_waiting_for_sport_async_should_return_real_category_ids_when_key_is_encoded));
        using var index = context.CreateCategoryPendingIndex();
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
    public async Task get_missing_sports_for_category_async_should_return_real_sport_ids_when_key_is_encoded()
    {
        using var context = new TsavoriteIntegrationContext(nameof(get_missing_sports_for_category_async_should_return_real_sport_ids_when_key_is_encoded));
        using var index = context.CreateCategoryPendingIndex();
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
    public async Task sport_ids_with_colon_should_not_collide_with_ids_using_dash()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sport_ids_with_colon_should_not_collide_with_ids_using_dash));
        using var index = context.CreateCategoryPendingIndex();
        var categoryId = new CategoryId("soccer:int");

        await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport:live"),
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);
        await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport-live"),
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        var sports = await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None);

        Assert.Equal(["sport-live", "sport:live"], sports.Select(sport => sport.Value).Order());
    }

    [Fact]
    public async Task category_ids_with_colon_should_not_collide_with_ids_using_dash()
    {
        using var context = new TsavoriteIntegrationContext(nameof(category_ids_with_colon_should_not_collide_with_ids_using_dash));
        using var index = context.CreateCategoryPendingIndex();
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
    public async Task try_mark_should_not_mark_pending_when_sport_exists_before_insert()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_mark_should_not_mark_pending_when_sport_exists_before_insert));
        using var index = context.CreateCategoryPendingIndex();
        var sportId = new SportId("sport:live");
        var categoryId = new CategoryId("soccer:int");

        var marked = await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                                     categoryId,
                                                                     static (_, _) => ValueTask.FromResult(true),
                                                                     CancellationToken.None);

        Assert.False(marked);
        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None));
    }

    [Fact]
    public async Task try_mark_should_remove_pending_when_sport_exists_on_second_check()
    {
        using var context = new TsavoriteIntegrationContext(nameof(try_mark_should_remove_pending_when_sport_exists_on_second_check));
        using var index = context.CreateCategoryPendingIndex();
        var sportId = new SportId("sport:live");
        var categoryId = new CategoryId("soccer:int");
        var callCount = 0;

        var marked = await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                                     categoryId,
                                                                     (_, _) => ValueTask.FromResult(++callCount > 1),
                                                                     CancellationToken.None);

        Assert.False(marked);
        Assert.Equal(2, callCount);
        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None));
    }

    [Fact]
    public async Task resolve_category_waiting_for_sport_async_should_remove_both_relation_directions()
    {
        using var context = new TsavoriteIntegrationContext(nameof(resolve_category_waiting_for_sport_async_should_remove_both_relation_directions));
        using var index = context.CreateCategoryPendingIndex();
        var sportId = new SportId("sport:live");
        var categoryId = new CategoryId("soccer:int");
        await index.TryMarkCategoryWaitingForSportAsync(sportId,
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        await index.ResolveCategoryWaitingForSportAsync(sportId, categoryId, CancellationToken.None);

        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(sportId, CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None));
    }

    [Fact]
    public async Task remove_category_from_pending_async_should_remove_all_missing_sports_for_category()
    {
        using var context = new TsavoriteIntegrationContext(nameof(remove_category_from_pending_async_should_remove_all_missing_sports_for_category));
        using var index = context.CreateCategoryPendingIndex();
        var firstSportId = new SportId("sport:live");
        var secondSportId = new SportId("sport-live");
        var categoryId = new CategoryId("soccer:int");
        await index.TryMarkCategoryWaitingForSportAsync(firstSportId,
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);
        await index.TryMarkCategoryWaitingForSportAsync(secondSportId,
                                                        categoryId,
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        await index.RemoveCategoryFromPendingAsync(categoryId, CancellationToken.None);

        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(firstSportId, CancellationToken.None));
        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(secondSportId, CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(categoryId, CancellationToken.None));
    }

    [Fact]
    public async Task clear_async_should_remove_all_pending_entries()
    {
        using var context = new TsavoriteIntegrationContext(nameof(clear_async_should_remove_all_pending_entries));
        using var index = context.CreateCategoryPendingIndex();

        await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport:1"),
                                                        new CategoryId("category:1"),
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);
        await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport:2"),
                                                        new CategoryId("category:2"),
                                                        static (_, _) => ValueTask.FromResult(false),
                                                        CancellationToken.None);

        await index.ClearAsync(CancellationToken.None);

        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(new SportId("sport:1"), CancellationToken.None));
        Assert.Empty(await index.GetCategoriesWaitingForSportAsync(new SportId("sport:2"), CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(new CategoryId("category:1"), CancellationToken.None));
        Assert.Empty(await index.GetMissingSportsForCategoryAsync(new CategoryId("category:2"), CancellationToken.None));
    }

    [Fact]
    public async Task pending_entries_should_persist_after_checkpoint_and_reopen()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(pending_entries_should_persist_after_checkpoint_and_reopen));
        var serializer = new MemoryPackWrapper();

        using (var engine = new TsavoriteEngine(storeScope.StorePath))
        {
            using (var session = new TsavoriteSessionWrapper(engine))
            {
                var index = new TsavoriteCategoryPendingIndex(session, serializer);

                await index.TryMarkCategoryWaitingForSportAsync(new SportId("sport:live"),
                                                                new CategoryId("soccer:int"),
                                                                static (_, _) => ValueTask.FromResult(false),
                                                                CancellationToken.None);
            }

            await engine.CompleteWriteAsync(CancellationToken.None);
            await engine.FlushAsync(CancellationToken.None);
        }

        using var reopenedEngine = new TsavoriteEngine(storeScope.StorePath);
        using var reopenedSession = new TsavoriteSessionWrapper(reopenedEngine);
        using var reopenedIndex = new TsavoriteCategoryPendingIndex(reopenedSession, serializer);

        Assert.Equal(["soccer:int"],
                     (await reopenedIndex.GetCategoriesWaitingForSportAsync(new SportId("sport:live"), CancellationToken.None))
                     .Select(category => category.Value));
        Assert.Equal(["sport:live"],
                     (await reopenedIndex.GetMissingSportsForCategoryAsync(new CategoryId("soccer:int"), CancellationToken.None))
                     .Select(sport => sport.Value));
    }
}
