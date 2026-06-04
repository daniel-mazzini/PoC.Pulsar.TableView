using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.Checkpoints;

[MemoryPackable]
public sealed partial record ViewCheckpoint(string ViewName, string StoreId, bool BuildCompleted, DateTimeOffset UpdatedAt);
