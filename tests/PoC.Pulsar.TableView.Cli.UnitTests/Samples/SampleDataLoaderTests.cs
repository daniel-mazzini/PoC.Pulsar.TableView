using PoC.Pulsar.TableView.Cli.Samples;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Samples;

public sealed class SampleDataLoaderTests
{
    [Fact]
    public void load_should_read_expected_sample_payloads()
    {
        var sampleFolder = Path.Combine(AppContext.BaseDirectory, "samples", "publish");
        Directory.CreateDirectory(sampleFolder);

        File.WriteAllText(Path.Combine(sampleFolder, "sports_mock_data.json"), """
            [
              {
                "id": "sport-1",
                "provider": "provider-a",
                "entityCoverage": "global",
                "name": "Football",
                "version": 1,
                "sportType": "team"
              }
            ]
            """);

        File.WriteAllText(Path.Combine(sampleFolder, "categories_mock_data.json"), """
            [
              {
                "id": "category-1",
                "provider": "provider-a",
                "entityCoverage": "global",
                "name": "Premier League",
                "version": 2,
                "sportId": "sport-1",
                "countryCode": "GB"
              }
            ]
            """);

        var loader = new SampleDataLoader();

        var sports = loader.LoadSports();
        var categories = loader.LoadCategories();

        Assert.Single(sports);
        Assert.Equal("sport-1", sports[0].Id);
        Assert.Equal("team", sports[0].SportType);

        Assert.Single(categories);
        Assert.Equal("category-1", categories[0].Id);
        Assert.Equal("sport-1", categories[0].SportId);
        Assert.Equal("GB", categories[0].CountryCode);
    }
}
