namespace PoC.Pulsar.TableView.Domain.Entities;

public readonly record struct PulsarMessageId(long LedgerId, long EntryId,  int PartitionIndex, int BatchIndex);
