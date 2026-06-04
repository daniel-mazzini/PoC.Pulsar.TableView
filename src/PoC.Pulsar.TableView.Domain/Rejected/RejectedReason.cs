using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.Rejected;

[MemoryPackable]
public sealed partial record RejectedReason(string Code, string Description);
