namespace PoC.Pulsar.TableView.Domain.TableView;

public sealed record TableEntry<T>(
    T? Value,
    TableEntryStatus Status,
    long Version,
    PulsarMessageId? MessageId,
    DateTimeOffset UpdatedAt,
    string? Reason = null);


public enum TableEntryStatus
{
    Active,
    RejectedLocal
}