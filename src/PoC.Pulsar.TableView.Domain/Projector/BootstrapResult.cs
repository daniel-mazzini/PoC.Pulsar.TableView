using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record TopicBootstrapResult<TMessage>;

public sealed record TopicRecoveredFromStateStore<TMessage>(
    IReadOnlyCollection<TableEntryChange<TMessage>> DeltaChanges)
    : TopicBootstrapResult<TMessage>;

public sealed record TopicRebuiltFromEarliest<TMessage>(string Reason)
    : TopicBootstrapResult<TMessage>;

public sealed record TopicHighWatermarkNotFound<TMessage>()
    : TopicBootstrapResult<TMessage>;


