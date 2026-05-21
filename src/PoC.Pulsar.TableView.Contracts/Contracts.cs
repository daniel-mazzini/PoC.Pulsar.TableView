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

    public int Version { get; init; }
}

public sealed class ExternalEntity
{
    public string Id { get; init; } = null!;
    public string Provider { get; init; } = null!;
    public string EntityCoverage { get; init; } = null!;
    public string DefaultName { get; init; } = null!;
}
