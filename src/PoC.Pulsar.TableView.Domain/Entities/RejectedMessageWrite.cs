namespace PoC.Pulsar.TableView.Domain.Entities;

public sealed record RejectedMessageWrite(
    string Topic,
    string MessageKey,
    object Message,
    IReadOnlyDictionary<string, string> Headers,
    DateTimeOffset Timestamp,
    string MessageType,
    string EventType);