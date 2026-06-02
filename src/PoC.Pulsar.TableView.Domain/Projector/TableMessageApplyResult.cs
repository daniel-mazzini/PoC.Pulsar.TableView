using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record TableMessageApplyResult<TMessage>;

public sealed record TableMessageApplied<TMessage>(TableEntryChange<TMessage> Change)
    : TableMessageApplyResult<TMessage>;

public sealed record TableMessageNoOp<TMessage>(string EntityId, string Reason)
    : TableMessageApplyResult<TMessage>;

public sealed record TableMessageRejected<TMessage>(string EntityId, RejectedReason Reason)
    : TableMessageApplyResult<TMessage>;
