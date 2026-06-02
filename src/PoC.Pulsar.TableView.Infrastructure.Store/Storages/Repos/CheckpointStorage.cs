using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
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

    public async Task SaveCheckpointAsync(string topicName, int partitionId, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
    {
        var metadata = await _metadataStorage.EnsureMetadataAsync(cancellationToken);
        var existing = await GetLastCheckpoint(topicName, partitionId, cancellationToken);

        existing = existing != null
            ? (existing with { LastProcessedMessageId =  lastProcessedMessageId, UpdatedAt = DateTimeOffset.UtcNow, StoreId = metadata.StoreGenerationId })
            : new TopicCheckpoint(topicName, partitionId, lastProcessedMessageId, metadata.StoreGenerationId, DateTimeOffset.UtcNow);

        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.TopicCheckpoint(topicName, partitionId),
                                     default,
                                     existing,
                                     cancellationToken);
    }

    public async ValueTask<TopicCheckpoint?> GetLastCheckpoint(string topicName, int partitionId, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<TopicCheckpoint,SpanByte,SpanByteAndMemory,SpanByteFunctions<Empty>>(session,
                                                                                                               StorageKey.TopicCheckpoint(topicName, partitionId),
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
