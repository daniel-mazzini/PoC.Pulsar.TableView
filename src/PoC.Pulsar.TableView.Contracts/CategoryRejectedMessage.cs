namespace PoC.Pulsar.TableView.Contracts;

public sealed record CategoryRejectedMessage(
    Guid Id,
    string OriginalTopic,
    string OriginalMessageKey,
    string OriginalPulsarMessageId,
    string OriginalMessageType,
    string OriginalEventType,
    string OriginalKey,
    string ReasonCode,
    string Reason,
    RawCategoryMessage OriginalPayload,
    DateTimeOffset RejectedAt,
    string CorrelationId,
    string CausationId,
    string MessageId);
