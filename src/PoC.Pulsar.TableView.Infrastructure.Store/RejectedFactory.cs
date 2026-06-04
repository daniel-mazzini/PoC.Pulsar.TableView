using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.TableView;

using DomainRejectedReason = PoC.Pulsar.TableView.Domain.Rejected.RejectedReason;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public static class RejectedFactory
{
    public static Rejected<TMessage> CreateFromPayload<TMessage>(TMessage message, TableViewMessage input, DomainRejectedReason reason)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var rejectedId = Guid.CreateVersion7();

        return new Rejected<TMessage>(RejectedId: rejectedId,
                                             OriginalTopic: input.TopicName,
                                             OriginalPartitionId: input.PartitionId,
                                             OriginalMessageKey: input.Key!,
                                             OriginalBrokerMessageId: input.BrokerMessageId.ToString(),
                                             OriginalPayload: message,
                                              Reason: reason,
                                             RejectedAt: timestamp,
                                             OriginalCorrelationId: HeaderOrNull(input, "correlation-id"),
                                             OriginalCausationId: HeaderOrNull(input, "causation-id"),
                                             OriginalMessageId: HeaderOrNull(input, "message-id"));
    }

    public static Rejected<TMessage> CreateFromTombStone<TMessage>(TableViewMessage input, DomainRejectedReason reason)
    {
        var timestamp = DateTimeOffset.UtcNow;
        var rejectedId = Guid.CreateVersion7();

        return new Rejected<TMessage>(RejectedId: rejectedId,
                                             OriginalTopic: input.TopicName,
                                             OriginalPartitionId: input.PartitionId,
                                             OriginalMessageKey: input.Key!,
                                             OriginalBrokerMessageId: input.BrokerMessageId.ToString(),
                                             OriginalPayload: default,
                                              Reason: reason,
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
