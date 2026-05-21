namespace PoC.Pulsar.TableView.Infrastructure.Store;

public readonly record struct PulsarMessageId(long LedgerId, long EntryId, int PartitionIndex = -1);
