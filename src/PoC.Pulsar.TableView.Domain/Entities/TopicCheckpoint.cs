namespace PoC.Pulsar.TableView.Domain.Entities;

public sealed partial record TopicCheckpoint(string TopicName, int PartitionId, PulsarMessageId LastProcessedMessageId, DateTimeOffset UpdatedAt);
