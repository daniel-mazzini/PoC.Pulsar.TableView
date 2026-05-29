using PoC.Pulsar.TableView.Domain.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.Controls;

public interface IMetadataStorage
{
    ValueTask<StoreMetadata> EnsureMetadataAsync(CancellationToken cancellationToken);

    ValueTask<StoreMetadata?> TryLoadMetadataAsync(CancellationToken cancellationToken);

    ValueTask MarkBootstrapCompletedAsync(CancellationToken cancellationToken);
}