using MemoryPack;

namespace PoC.Pulsar.TableView.Contracts;

[MemoryPackable]
public sealed partial class RawCategoryMessage : OfferHierarchyEntity
{
    public string SportId { get; init; } = null!;
    public string? ParentId { get; init; }
    public string? SportType { get; init; }
    public string? CountryCode { get; init; }
    public string? Gender { get; init; }
}
