using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public class CheckpointStorage : TsavoriteRepositoryBase, ICheckpointStorage
{
    private readonly IMetadataStorage _metadataStorage;
    private bool _disposed;
    private readonly ITsavoriteSessionProvider _sessionProvider;

    public CheckpointStorage(IStateSession session, IStateSerializer serializer, IMetadataStorage metadataStorage)
        : base(serializer)
    {
        _metadataStorage = metadataStorage;
        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async Task SaveCheckpointAsync(TopicShard shard, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var metadata = await _metadataStorage.EnsureMetadataAsync(cancellationToken);
        var existing = await GetLastCheckpoint(shard, cancellationToken);

        existing = existing != null
            ? (existing with { LastProcessedMessageId =  lastProcessedMessageId, UpdatedAt = DateTimeOffset.UtcNow, StoreId = metadata.StoreGenerationId })
            : new TopicCheckpoint(shard.LogicalTopic,
                                  shard.PhysicalTopic,
                                  shard.PartitionId,
                                  shard.IsPartitioned,
                                  lastProcessedMessageId,
                                  metadata.StoreGenerationId,
                                  DateTimeOffset.UtcNow);

        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.TopicCheckpoint(shard.PhysicalTopic),
                                     default,
                                     existing,
                                     cancellationToken);
    }

    public async ValueTask<TopicCheckpoint?> GetLastCheckpoint(TopicShard shard, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<TopicCheckpoint,SpanByte,SpanByteAndMemory,SpanByteFunctions<Empty>>(session,
                                                                                                                StorageKey.TopicCheckpoint(shard.PhysicalTopic),
                                                                                                                cancellationToken);
    }

    public async Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var metadata = await _metadataStorage.EnsureMetadataAsync(cancellationToken);
        var checkpoint = new ViewCheckpoint(viewName,
                                            metadata.StoreGenerationId.ToString("D"),
                                            BuildCompleted: true,
                                            DateTimeOffset.UtcNow);

        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.ViewCheckpoint(viewName),
                                     default,
                                     checkpoint,
                                     cancellationToken);
    }

    public async ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<ViewCheckpoint, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                  StorageKey.ViewCheckpoint(viewName),
                                                                                                                  cancellationToken);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _sessionProvider.Dispose();
        _disposed = true;
    }
}
