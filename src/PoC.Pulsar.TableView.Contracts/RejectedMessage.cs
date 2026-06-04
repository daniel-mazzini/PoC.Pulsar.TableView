namespace PoC.Pulsar.TableView.Contracts;

public abstract record RejectedMessageBase(
    Guid RejectedId,
    string OriginalTopic,
    int OriginalPartitionId,
    string OriginalBrokerMessageId,
    string OriginalMessageKey,
    RejectedReasonMessage Reason,
    DateTimeOffset RejectedAt,
    string? OriginalCorrelationId,
    string? OriginalCausationId,
    string? OriginalMessageId);

public sealed record SportRejectedMessage(
    Guid RejectedId,
    string OriginalTopic,
    int OriginalPartitionId,
    string OriginalBrokerMessageId,
    string OriginalMessageKey,
    RejectedReasonMessage Reason,
    SportMessage? OriginalPayload,
    DateTimeOffset RejectedAt,
    string? OriginalCorrelationId,
    string? OriginalCausationId,
    string? OriginalMessageId)
    : RejectedMessageBase(RejectedId,
                          OriginalTopic,
                          OriginalPartitionId,
                          OriginalBrokerMessageId,
                          OriginalMessageKey,
                          Reason,
                          RejectedAt,
                          OriginalCorrelationId,
                          OriginalCausationId,
                          OriginalMessageId);

public sealed record RawCategoryRejectedMessage(
    Guid RejectedId,
    string OriginalTopic,
    int OriginalPartitionId,
    string OriginalBrokerMessageId,
    string OriginalMessageKey,
    RejectedReasonMessage Reason,
    RawCategoryMessage? OriginalPayload,
    DateTimeOffset RejectedAt,
    string? OriginalCorrelationId,
    string? OriginalCausationId,
    string? OriginalMessageId)
    : RejectedMessageBase(RejectedId,
                          OriginalTopic,
                          OriginalPartitionId,
                          OriginalBrokerMessageId,
                          OriginalMessageKey,
                          Reason,
                          RejectedAt,
                          OriginalCorrelationId,
                          OriginalCausationId,
                          OriginalMessageId);

public sealed record RejectedReasonMessage(string ReasonCode, string Reason);
