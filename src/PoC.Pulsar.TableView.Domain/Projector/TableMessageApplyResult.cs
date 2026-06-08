using PoC.Pulsar.TableView.Domain.Rejected;

namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record TableMessageApplyResult<TMessage>;

public sealed record TableMessageApplied<TMessage>(string EntityId, TMessage NewValue, TableMessageApplyDecision Decision)
    : TableMessageApplyResult<TMessage>;

public sealed record TableMessageNoOp<TMessage>(string EntityId, string Reason)
    : TableMessageApplyResult<TMessage>;

public sealed record TableMessageRejected<TMessage>(string EntityId, RejectedReason Reason)
    : TableMessageApplyResult<TMessage>;

public sealed record TableMessageDeleted<TMessage>(string EntityId, TMessage CurrentValue)
    : TableMessageApplyResult<TMessage>;
