namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record StoreRecoveryMode;

public sealed record RecoverFromStateStore : StoreRecoveryMode;

public sealed record RebuildFromEarliest(string Reason) : StoreRecoveryMode;
