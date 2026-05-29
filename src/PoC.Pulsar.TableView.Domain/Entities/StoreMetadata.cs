namespace PoC.Pulsar.TableView.Domain.Entities;

public sealed partial record StoreMetadata(Guid StoreGenerationId, int SchemaVersion, bool IsBoostrapCompleted, DateTimeOffset CreatedAt)
{
    public static StoreMetadata CreateDefault() => new(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: false, CreatedAt: DateTimeOffset.UtcNow);
};