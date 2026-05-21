namespace PoC.Pulsar.TableView.Contracts;

public sealed class GeoTaxonomyMessage
{
    public string SportId { get; init; } = null!;
    public string SportName { get; init; } = null!;
    public string SportType { get; init; } = null!;
    public int Version { get; init; } // used in compactation to detect changes in the taxonomy and avoid unnecessary updates when only the categories change
    public List<GeoTaxonomyNode> GeoCategories { get; init; } = [];
}
