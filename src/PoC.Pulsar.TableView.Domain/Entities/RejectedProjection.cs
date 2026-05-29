namespace PoC.Pulsar.TableView.Domain.Entities;

public sealed partial record RejectedProjection(string MessageKey, string TopicName, int PartitionId, string Reason, DateTimeOffset CreatedAt);
