namespace PoC.Pulsar.TableView.Domain.TableView;

public readonly record struct PulsarMessageId(long LedgerId, long EntryId,  int PartitionIndex, int BatchIndex);
