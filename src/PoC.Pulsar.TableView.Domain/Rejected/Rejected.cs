namespace PoC.Pulsar.TableView.Domain.Rejected;

public sealed record Rejected<T>(
    Guid RejectedId,
    string OriginalTopic,
    int OriginalPartitionId,
    string OriginalBrokerMessageId,
    string OriginalMessageKey,
    RejectedReason Reason,
    T? OriginalPayload,
    DateTimeOffset RejectedAt,
    string? OriginalCorrelationId,
    string? OriginalCausationId,
    string? OriginalMessageId);
