namespace PoC.Pulsar.TableView.Contracts;

public sealed record SportRejectedMessage(
    Guid Id,
    string OriginalTopic,
    string OriginalMessageKey,
    string OriginalPulsarMessageId,
    string OriginalMessageType,
    string OriginalEventType,
    string OriginalKey,
    string ReasonCode,
    string Reason,
    SportMessage OriginalPayload,
    DateTimeOffset RejectedAt,
    string CorrelationId,
    string CausationId,
    string MessageId);
