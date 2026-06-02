using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.TableView;

using ContractRejectedReason = PoC.Pulsar.TableView.Contracts.RejectedReasonMessage;
using DomainRejectedReason = PoC.Pulsar.TableView.Domain.Rejected.RejectedReason;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public static class RejectedFactory
{
    public static RejectedMessage<TMessage> CreateFromPayload<TMessage>(TMessage message, TableViewMessage input, DomainRejectedReason reason)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var rejectedId = Guid.CreateVersion7();

        return new RejectedMessage<TMessage>(RejectedId: rejectedId,
                                             OriginalTopic: input.TopicName,
                                             OriginalPartitionId: input.PartitionId,
                                             OriginalMessageKey: input.Key!,
                                             OriginalBrokerMessageId: input.BrokerMessageId.ToString(),
                                             OriginalPayload: message,
                                             Reason: new ContractRejectedReason(reason.Code, reason.Description),
                                             RejectedAt: timestamp,
                                             OriginalCorrelationId: HeaderOrNull(input, "correlation-id"),
                                             OriginalCausationId: HeaderOrNull(input, "causation-id"),
                                             OriginalMessageId: HeaderOrNull(input, "message-id"));
    }

    public static RejectedMessage<TMessage> CreateFromTombStone<TMessage>(TableViewMessage input, DomainRejectedReason reason)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var rejectedId = Guid.CreateVersion7();

        return new RejectedMessage<TMessage>(RejectedId: rejectedId,
                                             OriginalTopic: input.TopicName,
                                             OriginalPartitionId: input.PartitionId,
                                             OriginalMessageKey: input.Key!,
                                             OriginalBrokerMessageId: input.BrokerMessageId.ToString(),
                                             OriginalPayload: default,
                                             Reason: new ContractRejectedReason(reason.Code, reason.Description),
                                             RejectedAt: timestamp,
                                             OriginalCorrelationId: HeaderOrNull(input, "correlation-id"),
                                             OriginalCausationId: HeaderOrNull(input, "causation-id"),
                                             OriginalMessageId: HeaderOrNull(input, "message-id"));
    }

    private static string? HeaderOrNull(TableViewMessage message, string name)
        => message.Headers.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : null;
}
