using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.Rejected;

public sealed record RejectedMessageWrite<TMessage>(
    RejectedMessage<TMessage> RejectedMessage,
    IReadOnlyDictionary<string, string> Headers);