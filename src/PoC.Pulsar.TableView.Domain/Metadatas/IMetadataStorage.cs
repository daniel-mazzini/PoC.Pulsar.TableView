namespace PoC.Pulsar.TableView.Domain.Metadatas;

public interface IMetadataStorage
{
    ValueTask<StoreMetadata> EnsureMetadataAsync(CancellationToken cancellationToken);

    ValueTask<StoreMetadata?> TryLoadMetadataAsync(CancellationToken cancellationToken);

    ValueTask MarkBootstrapCompletedAsync(CancellationToken cancellationToken);
}