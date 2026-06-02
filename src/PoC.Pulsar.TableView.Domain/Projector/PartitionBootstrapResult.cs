using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record PartitionBootstrapResult<TMessage>(
    int PartitionId);

public sealed record PartitionRecoveredFromStateStore<TMessage>(
    int PartitionId,
    IReadOnlyCollection<TableEntryChange<TMessage>> DeltaChanges)
    : PartitionBootstrapResult<TMessage>(PartitionId);

public sealed record PartitionRebuiltFromEarliest<TMessage>(
    int PartitionId)
    : PartitionBootstrapResult<TMessage>(PartitionId);

