using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class InMemoryCategoryBySportIndexTests
{
    [Fact]
    public async Task get_categories_by_sport_async_should_return_real_category_ids_when_storage_key_is_sanitized()
    {
        var index = new InMemoryCategoryBySportIndex();
        var sportId = new SportId("sport:live");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer:int"), sportId, null), CancellationToken.None);

        var categories = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        var category = Assert.Single(categories);
        Assert.Equal("soccer:int", category.Value);
    }

    [Fact]
    public async Task get_categories_by_parent_async_should_return_real_category_ids_when_storage_key_is_sanitized()
    {
        var index = new InMemoryCategoryBySportIndex();
        var parentId = new CategoryId("parent:int");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("child:int"), new SportId("sport:live"), parentId), CancellationToken.None);

        var children = await index.GetCategoriesByParentAsync(parentId, CancellationToken.None);

        var child = Assert.Single(children);
        Assert.Equal("child:int", child.Value);
    }

    [Fact]
    public async Task categories_with_sanitized_equivalent_ids_should_not_collide()
    {
        var index = new InMemoryCategoryBySportIndex();
        var sportId = new SportId("sport:live");

        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer:int"), sportId, null), CancellationToken.None);
        await index.IndexCategoryAsync(new CategoryRelations(new CategoryId("soccer-int"), sportId, null), CancellationToken.None);

        var categories = await index.GetCategoriesBySportAsync(sportId, CancellationToken.None);

        Assert.Equal(["soccer-int", "soccer:int"], categories.Select(category => category.Value).Order());
    }

    [Fact]
    public async Task remove_category_relations_async_should_remove_using_real_category_id()
    {
        var index = new InMemoryCategoryBySportIndex();
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
}
