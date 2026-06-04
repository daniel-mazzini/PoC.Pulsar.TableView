using MemoryPack;

namespace PoC.Pulsar.TableView.Contracts;

[MemoryPackable]
public partial class Entity
{
    public string Id { get; init; } = null!;
    public string Provider { get; init; } = null!;
    public string EntityCoverage { get; init; } = null!;
    public List<ExternalEntity> ExternalEntities { get; init; } = [];
}

[MemoryPackable]
public partial class OfferHierarchyEntity : Entity
{
    public string Name { get; init; } = null!;

    public int Version { get; init; }
}

[MemoryPackable]
public sealed partial class ExternalEntity
{
    public string Id { get; init; } = null!;
    public string Provider { get; init; } = null!;
    public string EntityCoverage { get; init; } = null!;
    public string DefaultName { get; init; } = null!;
}
