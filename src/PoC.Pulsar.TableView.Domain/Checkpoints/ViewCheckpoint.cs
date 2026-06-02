namespace PoC.Pulsar.TableView.Domain.Checkpoints;

public sealed partial record ViewCheckpoint(string ViewName, string StoreId, bool BuildCompleted, DateTimeOffset UpdatedAt);
