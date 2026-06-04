using MemoryPack;

namespace PoC.Pulsar.TableView.Contracts;

[MemoryPackable]
public sealed partial class SportMessage : OfferHierarchyEntity
{
    public string SportType { get; init; } = null!;
}
