using PoC.Pulsar.TableView.Contracts;
using Xunit;

namespace PoC.Pulsar.TableView.Contracts.UnitTests;

public sealed class GeoTaxonomyViewMessageTests
{
    [Fact]
    public void create_new_should_initialize_empty_geo_categories_and_zero_version()
    {
        var view = GeoTaxonomyViewMessage.CreateNew("sport-1", "Football", "team");

        Assert.Equal("sport-1", view.SportId);
        Assert.Equal("Football", view.SportName);
        Assert.Equal("team", view.SportType);
        Assert.Equal(0, view.Version);
        Assert.Empty(view.GeoCategories);
    }

    [Fact]
    public void create_should_preserve_version_and_categories()
    {
        var view = GeoTaxonomyViewMessage.Create(
            new SportMessage
            {
                Id = "sport-1",
                Name = "Football",
                SportType = "team"
            },
            [new GeoTaxonomyNode("category-1", "GB")],
            version: 5);

        Assert.Equal("sport-1", view.SportId);
        Assert.Equal("Football", view.SportName);
        Assert.Equal("team", view.SportType);
        Assert.Equal(5, view.Version);
        Assert.Single(view.GeoCategories);
    }
}
