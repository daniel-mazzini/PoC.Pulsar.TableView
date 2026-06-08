using MemoryPack;

namespace PoC.Pulsar.TableView.Contracts;

[MemoryPackable]
public sealed partial record GeoTaxonomyNode(string CategoryId, string CountryCode);
