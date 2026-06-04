using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class MetadataStorageTests
{
    [Fact]
    public async Task ensure_metadata_async_should_create_metadata_when_store_is_empty()
    {
        using var context = new TsavoriteIntegrationContext(nameof(ensure_metadata_async_should_create_metadata_when_store_is_empty));

        var metadata = await context.MetadataStorage.EnsureMetadataAsync(CancellationToken.None);

        Assert.NotEqual(Guid.Empty, metadata.StoreGenerationId);
        Assert.Equal(1, metadata.SchemaVersion);
        Assert.False(metadata.IsBoostrapCompleted);
        Assert.NotEqual(default, metadata.CreatedAt);
    }

    [Fact]
    public async Task mark_bootstrap_completed_async_should_persist_flag()
    {
        using var context = new TsavoriteIntegrationContext(nameof(mark_bootstrap_completed_async_should_persist_flag));

        await context.MetadataStorage.EnsureMetadataAsync(CancellationToken.None);
        await context.MetadataStorage.MarkBootstrapCompletedAsync(CancellationToken.None);
        var metadata = await context.MetadataStorage.TryLoadMetadataAsync(CancellationToken.None);

        Assert.NotNull(metadata);
        Assert.True(metadata.IsBoostrapCompleted);
    }
}
