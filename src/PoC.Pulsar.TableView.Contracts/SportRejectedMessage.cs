namespace PoC.Pulsar.TableView.Contracts;

public sealed record RejectedMessage<T>(
    Guid RejectedId,
    string OriginalTopic,
    int OriginalPartitionId,
    string OriginalBrokerMessageId,
    string OriginalMessageKey,
    RejectedReasonMessage Reason,
    T? OriginalPayload,
    DateTimeOffset RejectedAt,
    string? OriginalCorrelationId,
    string? OriginalCausationId,
    string? OriginalMessageId);

public sealed record RejectedReasonMessage(string ReasonCode, string Reason);
