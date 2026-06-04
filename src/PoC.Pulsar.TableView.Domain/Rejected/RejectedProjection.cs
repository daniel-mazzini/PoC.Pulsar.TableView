using MemoryPack;

namespace PoC.Pulsar.TableView.Domain.Rejected;

[MemoryPackable]
public sealed partial record RejectedProjection(string MessageKey, string TopicName, int PartitionId, RejectedReason Reason, DateTimeOffset CreatedAt);
