using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Contracts.UnitTests;

public sealed class GeoTaxonomyViewMessageBehaviorTests
{
    [Fact]
    public void add_or_update_category_should_add_new_category()
    {
        var view = GeoTaxonomyViewMessage.CreateNew("sport-1", "Football", "team");

        var updated = view.AddOrUpdateCategory(new GeoTaxonomyNode("category-1", "GB"));

        Assert.Equal(1, updated.Version);
        Assert.Single(updated.GeoCategories);
        Assert.Contains(updated.GeoCategories, category => category.CategoryId == "category-1" && category.CountryCode == "GB");
    }

    [Fact]
    public void add_or_update_category_should_replace_existing_category_and_increment_version()
    {
        var view = GeoTaxonomyViewMessage.Create(
            new SportMessage
            {
                Id = "sport-1",
                Name = "Football",
                SportType = "team"
            },
            [new GeoTaxonomyNode("category-1", "GB")],
            version: 2);

        var updated = view.AddOrUpdateCategory(new GeoTaxonomyNode("category-1", "ES"));

        Assert.Equal(3, updated.Version);
        Assert.Single(updated.GeoCategories);
        Assert.Contains(updated.GeoCategories, category => category.CategoryId == "category-1" && category.CountryCode == "ES");
    }
}
