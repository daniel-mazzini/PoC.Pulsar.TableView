using System.Collections.Immutable;

namespace PoC.Pulsar.TableView.Contracts;

public sealed record GeoTaxonomyViewMessage
{
    public string SportId { get; init; } = string.Empty;
    public string SportName { get; init; } = string.Empty;
    public string SportType { get; init; } = string.Empty;
    public int Version { get; init; }
    public DateTimeOffset Timestamp { get; set; }
    public ImmutableHashSet<GeoTaxonomyNode> GeoCategories { get; init; } = [];

    public GeoTaxonomyViewMessage()
    {
    }

    private GeoTaxonomyViewMessage(string sportId,
                                   string sportName,
                                   string sportType,
                                   int version,
                                   IEnumerable<GeoTaxonomyNode> geoCategories)
    {
        SportId = sportId;
        SportName = sportName;
        SportType = sportType;
        Version = version;
        GeoCategories = [.. geoCategories];
    }

    public static GeoTaxonomyViewMessage Create(SportMessage sport, IEnumerable<GeoTaxonomyNode> geoCategories, int version = 0)
    {
        return new GeoTaxonomyViewMessage(sport.Id, sport.Name, sport.SportType, version, geoCategories);

    }
}




