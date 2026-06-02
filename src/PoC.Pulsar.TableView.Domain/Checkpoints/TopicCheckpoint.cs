using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Checkpoints;

public sealed partial record TopicCheckpoint(string TopicName, int PartitionId, PulsarMessageId LastProcessedMessageId, Guid StoreId, DateTimeOffset UpdatedAt);
