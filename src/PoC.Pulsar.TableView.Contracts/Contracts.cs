namespace PoC.Pulsar.TableView.Contracts;

public class Entity
{
    public string Id { get; init; } = null!;
    public string Provider { get; init; } = null!;
    public string EntityCoverage { get; init; } = null!;
    public List<ExternalEntity> ExternalEntities { get; init; } = [];
}

public class OfferHierarchyEntity : Entity
{
    public string Name { get; init; } = null!;
}

public sealed class ExternalEntity
{
    public string Id { get; init; } = null!;
    public string Provider { get; init; } = null!;
    public string EntityCoverage { get; init; } = null!;
    public string DefaultName { get; init; } = null!;
}

public sealed class SportMessage : OfferHierarchyEntity
{
    public string SportType { get; init; } = null!;
}

public sealed class RawCategoryMessage : OfferHierarchyEntity
{
    public string SportId { get; init; } = null!;
    public string? ParentId { get; init; }
    public string? SportType { get; init; }
    public string? CountryCode { get; init; }
    public string? Gender { get; init; }
}

public sealed class GeoTaxonomyNode
{
    public string CountryCode { get; init; } = null!;
}

public sealed class GeoTaxonomyMessage
{
    public string SportId { get; init; } = null!;
    public string SportName { get; init; } = null!;
    public string SportType { get; init; } = null!;
    public List<GeoTaxonomyNode> GeoCategories { get; init; } = [];
}
