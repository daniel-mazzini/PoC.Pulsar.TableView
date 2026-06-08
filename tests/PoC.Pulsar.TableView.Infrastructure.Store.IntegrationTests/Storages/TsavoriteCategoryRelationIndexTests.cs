using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages;

public sealed class TsavoriteCategoryRelationIndexTests
{
    [Fact]
    public async Task get_categories_by_sport_async_should_return_real_category_ids_when_key_is_sanitized()
    {
        using var context = new TsavoriteIntegrationContext(nameof(get_categories_by_sport_async_should_return_real_category_ids_when_key_is_sanitized));
        using var index = context.CreateCategoryRelationIndex();
        var sportId = new SportId("sport:live");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer:int"), sportId, null), CancellationToken.None);

        var categories = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        var category = Assert.Single(categories);
        Assert.Equal("soccer:int", category.Value);
    }

    [Fact]
    public async Task get_categories_by_parent_async_should_return_real_category_ids_when_key_is_sanitized()
    {
        using var context = new TsavoriteIntegrationContext(nameof(get_categories_by_parent_async_should_return_real_category_ids_when_key_is_sanitized));
        using var index = context.CreateCategoryRelationIndex();
        var parentId = new CategoryId("parent:int");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("child:int"), new SportId("sport:live"), parentId), CancellationToken.None);

        var categories = await index.GetCategoriesByParentAsync(parentId, CancellationToken.None);

        var child = Assert.Single(categories);
        Assert.Equal("child:int", child.Value);
    }

    [Fact]
    public async Task scan_by_sport_should_return_all_category_ids_so_categories_can_be_loaded_by_id()
    {
        using var context = new TsavoriteIntegrationContext(nameof(scan_by_sport_should_return_all_category_ids_so_categories_can_be_loaded_by_id));
        using var index = context.CreateCategoryRelationIndex();
        var storage = context.CreateCategoryMessageStorage();
        var sportId = new SportId("sport:live");

        var first = IntegrationTestData.Category("soccer:int", sportId.Value);
        var second = IntegrationTestData.Category("tennis-int", sportId.Value);
        var third = IntegrationTestData.Category("ft|ofr|cat|01-09", sportId.Value);

        await storage.UpsertAsync(first, CancellationToken.None);
        await storage.UpsertAsync(second, CancellationToken.None);
        await storage.UpsertAsync(third, CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId(first.Id), sportId, null), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId(second.Id), sportId, null), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId(third.Id), sportId, null), CancellationToken.None);

        var categoryIds = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        Assert.Equal([third.Id, first.Id, second.Id], categoryIds.Select(category => category.Value).Order());

        foreach (var categoryId in categoryIds)
        {
            var category = await storage.TryLoadAsync(categoryId.Value, CancellationToken.None);
            Assert.NotNull(category);
            Assert.Equal(sportId.Value, category!.SportId);
        }
    }

    [Fact]
    public async Task sanitized_equivalent_category_ids_should_not_collide()
    {
        using var context = new TsavoriteIntegrationContext(nameof(sanitized_equivalent_category_ids_should_not_collide));
        using var index = context.CreateCategoryRelationIndex();
        var sportId = new SportId("sport:live");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer:int"), sportId, null), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer-int"), sportId, null), CancellationToken.None);

        var categories = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        Assert.Equal(["soccer-int", "soccer:int"], categories.Select(category => category.Value).Order());
    }

    [Fact]
    public async Task ids_with_colon_should_not_collide_with_ids_using_dash()
    {
        using var context = new TsavoriteIntegrationContext(nameof(ids_with_colon_should_not_collide_with_ids_using_dash));
        using var index = context.CreateCategoryRelationIndex();
        var sportId = new SportId("ft|ofr|sport|01");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("fifa:2026"), sportId, null), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("fifa-2026"), sportId, null), CancellationToken.None);

        var categories = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        Assert.Equal(["fifa-2026", "fifa:2026"], categories.Select(category => category.Value).Order());
    }

    [Fact]
    public async Task has_queries_should_work_with_granular_keys()
    {
        using var context = new TsavoriteIntegrationContext(nameof(has_queries_should_work_with_granular_keys));
        using var index = context.CreateCategoryRelationIndex();
        var sportId = new SportId("sport:live");
        var parentId = new CategoryId("parent:int");
        var categoryId = new CategoryId("soccer:int");

        await index.IndexCategoryAsync(new CategoryRelations(categoryId, sportId, parentId), CancellationToken.None);

        Assert.True(await index.HasCategoryBySportAsync(sportId, categoryId, CancellationToken.None));
        Assert.True(await index.HasCategoryByParentAsync(parentId, categoryId, CancellationToken.None));
        Assert.False(await index.HasCategoryBySportAsync(sportId, new CategoryId("missing:int"), CancellationToken.None));
    }

    [Fact]
    public async Task remove_category_relations_async_should_remove_sport_and_parent_keys()
    {
        using var context = new TsavoriteIntegrationContext(nameof(remove_category_relations_async_should_remove_sport_and_parent_keys));
        using var index = context.CreateCategoryRelationIndex();
        var sportId = new SportId("sport:live");
        var categoryId = new CategoryId("soccer:int");
        var parentId = new CategoryId("parent:int");
        var relations = new CategoryRelations(categoryId, sportId, parentId);

        await index.IndexCategoryAsync(relations, CancellationToken.None);
        await index.RemoveCategoryRelationsAsync(relations, CancellationToken.None);

        Assert.Empty(await index.GetCategoriesBySportAsync(sportId, CancellationToken.None));
        Assert.Empty(await index.GetCategoriesByParentAsync(parentId, CancellationToken.None));
        Assert.False(await index.HasCategoryBySportAsync(sportId, categoryId, CancellationToken.None));
        Assert.False(await index.HasCategoryByParentAsync(parentId, categoryId, CancellationToken.None));
    }

    [Fact]
    public async Task replace_category_relations_async_should_remove_obsolete_relations_and_keep_current_ones()
    {
        using var context = new TsavoriteIntegrationContext(nameof(replace_category_relations_async_should_remove_obsolete_relations_and_keep_current_ones));
        using var index = context.CreateCategoryRelationIndex();
        var previous = new CategoryRelations(new CategoryId("category-1"), new SportId("sport-old"), new CategoryId("parent-old"));
        var current = new CategoryRelations(new CategoryId("category-1"), new SportId("sport-new"), new CategoryId("parent-new"));

        await index.IndexCategoryAsync(previous, CancellationToken.None);
        await index.ReplaceCategoryRelationsAsync(previous, current, CancellationToken.None);

        Assert.False(await index.HasCategoryBySportAsync(previous.SportId, previous.CategoryId, CancellationToken.None));
        Assert.False(await index.HasCategoryByParentAsync(previous.ParentCategoryId!.Value, previous.CategoryId, CancellationToken.None));
        Assert.True(await index.HasCategoryBySportAsync(current.SportId, current.CategoryId, CancellationToken.None));
        Assert.True(await index.HasCategoryByParentAsync(current.ParentCategoryId!.Value, current.CategoryId, CancellationToken.None));
    }

    [Fact]
    public async Task clear_async_should_remove_all_granular_relations()
    {
        using var context = new TsavoriteIntegrationContext(nameof(clear_async_should_remove_all_granular_relations));
        using var index = context.CreateCategoryRelationIndex();

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("category-1"), new SportId("sport-1"), new CategoryId("parent-1")), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("category-2"), new SportId("sport-2"), null), CancellationToken.None);

        await index.ClearAsync(CancellationToken.None);

        Assert.Empty(await index.GetCategoriesBySportAsync(new SportId("sport-1"), CancellationToken.None));
        Assert.Empty(await index.GetCategoriesBySportAsync(new SportId("sport-2"), CancellationToken.None));
        Assert.Empty(await index.GetCategoriesByParentAsync(new CategoryId("parent-1"), CancellationToken.None));
    }

    [Fact]
    public async Task relations_should_persist_after_checkpoint_and_reopen()
    {
        using var storeScope = new TsavoriteStoreScope(nameof(relations_should_persist_after_checkpoint_and_reopen));
        var serializer = new PoC.Pulsar.TableView.Infrastructure.Store.Serialization.MemoryPackWrapper();

        using (var engine = new TsavoriteEngine(storeScope.StorePath))
        {
            using (var session = new TsavoriteSessionWrapper(engine))
            {
                var index = new TsavoriteCategoryRelationIndex(session, serializer);
                var sportId = new SportId("sport-live");
                var parentId = new CategoryId("parent-live");
                var categoryId = new CategoryId("category-live");

                await index.IndexCategoryAsync(new CategoryRelations(categoryId, sportId, parentId), CancellationToken.None);
            }

            await engine.CompleteWriteAsync(CancellationToken.None);
            await engine.FlushAsync(CancellationToken.None);
        }

        using var reopenedEngine = new TsavoriteEngine(storeScope.StorePath);
        using var reopenedSession = new TsavoriteSessionWrapper(reopenedEngine);
        using var reopenedIndex = new TsavoriteCategoryRelationIndex(reopenedSession, serializer);
        var reopenedSportId = new SportId("sport-live");
        var reopenedParentId = new CategoryId("parent-live");
        var reopenedCategoryId = new CategoryId("category-live");

        Assert.True(await reopenedIndex.HasCategoryBySportAsync(reopenedSportId, reopenedCategoryId, CancellationToken.None));
        Assert.True(await reopenedIndex.HasCategoryByParentAsync(reopenedParentId, reopenedCategoryId, CancellationToken.None));
        Assert.Equal(["category-live"], (await reopenedIndex.GetCategoriesBySportAsync(reopenedSportId, CancellationToken.None)).Select(category => category.Value));
    }
}
