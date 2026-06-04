using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.TableView;

[MemoryPackable]
public readonly partial record struct PulsarMessageId(long LedgerId, long EntryId, int PartitionIndex, int BatchIndex);
