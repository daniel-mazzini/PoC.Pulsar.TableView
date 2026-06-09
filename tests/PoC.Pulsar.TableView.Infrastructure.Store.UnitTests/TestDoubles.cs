using DotPulsar;
using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using System.Buffers;
using System.Text.Json;
using PoC.Pulsar.TableView.Domain.Storages.Messages;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

internal sealed class JsonAvroSerializer : IAvroSerializer
{
    public T Deserialize<T>(ReadOnlySpan<byte> data)
        => JsonSerializer.Deserialize<T>(data) ?? throw new InvalidOperationException($"Could not deserialize {typeof(T).Name}.");

    public T Deserialize<T>(ReadOnlySequence<byte> data)
        => Deserialize<T>(data.ToArray());

    public async Task<T> DeserializeFromStream<T>(Stream stream, CancellationToken cancellationToken)
        => (await JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellationToken))
           ?? throw new InvalidOperationException($"Could not deserialize {typeof(T).Name}.");

    public void Serialize<T>(T message, Stream output)
        => JsonSerializer.Serialize(output, message);
}

internal sealed class FakeRejectedMessagePublisher : IRejectedMessagePublisher
{
    public List<object> PublishedMessages { get; } = [];
    public List<Dictionary<string, string>> PublishedHeaders { get; } = [];

    public Task PublishAsync<TMessage>(Rejected<TMessage> write, Dictionary<string, string> headers, CancellationToken cancellationToken)
    {
        PublishedMessages.Add(write!);
        PublishedHeaders.Add(new Dictionary<string, string>(headers, StringComparer.Ordinal));
        return Task.CompletedTask;
    }
}

internal sealed class FakeSportMessageStorage : IMessageStorage<string, SportMessage>
{
    private readonly Dictionary<string, SportMessage> _messages = new(StringComparer.Ordinal);

    public int ClearCallCount { get; private set; }
    public int UpsertCallCount { get; private set; }
    public int TryApplyCallCount { get; private set; }
    public int DeleteCallCount { get; private set; }
    public bool ThrowOnTryApply { get; set; }

    public void Seed(SportMessage message)
        => _messages[message.Id] = Clone(message);

    public ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        DeleteCallCount++;
        _messages.Remove(id);
        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ClearCallCount++;
        _messages.Clear();
        return ValueTask.CompletedTask;
    }

    public ValueTask<SportMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
        => ValueTask.FromResult(_messages.TryGetValue(id, out var message) ? Clone(message) : null);

    public ValueTask UpsertAsync(SportMessage message, CancellationToken cancellationToken)
    {
        UpsertCallCount++;
        _messages[message.Id] = Clone(message);
        return ValueTask.CompletedTask;
    }

    public ValueTask<TableMessageApplyDecision> TryApplyAsync(SportMessage message, CancellationToken cancellationToken)
    {
        TryApplyCallCount++;

        if (ThrowOnTryApply)
        {
            throw new InvalidOperationException("TryApply failed.");
        }

        if (!_messages.TryGetValue(message.Id, out var current))
        {
            _messages[message.Id] = Clone(message);
            return ValueTask.FromResult(TableMessageApplyDecision.Created());
        }

        if (message.Version <= current.Version)
        {
            return ValueTask.FromResult(TableMessageApplyDecision.NoOp("incoming_version_not_greater_than_current"));
        }

        _messages[message.Id] = Clone(message);
        return ValueTask.FromResult(TableMessageApplyDecision.Updated());
    }

    public Dictionary<string, SportMessage> GetAll(IValuePredicate<SportMessage>? valuePredicate = null)
    {
        if (valuePredicate is null)
        {
            return _messages.ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
        }

        return _messages.Where(pair => valuePredicate.Match(pair.Value))
                        .ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
    }

    public SportMessage? GetById(string id)
        => _messages.TryGetValue(id, out var message) ? Clone(message) : null;

    private static SportMessage Clone(SportMessage message)
        => new()
        {
            Id = message.Id,
            Name = message.Name,
            SportType = message.SportType,
            Provider = message.Provider,
            EntityCoverage = message.EntityCoverage,
            Version = message.Version,
            ExternalEntities = message.ExternalEntities.Select(entity => new ExternalEntity
            {
                Id = entity.Id,
                Provider = entity.Provider,
                EntityCoverage = entity.EntityCoverage,
                DefaultName = entity.DefaultName
            }).ToList()
        };
}

internal sealed class FakeRawCategoryMessageStorage : IMessageStorage<string, RawCategoryMessage>
{
    private readonly Dictionary<string, RawCategoryMessage> _messages = new(StringComparer.Ordinal);

    public int ClearCallCount { get; private set; }
    public int UpsertCallCount { get; private set; }
    public int TryApplyCallCount { get; private set; }
    public int DeleteCallCount { get; private set; }
    public bool ThrowOnTryApply { get; set; }

    public void Seed(RawCategoryMessage message)
        => _messages[message.Id] = Clone(message);

    public ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
    {
        DeleteCallCount++;
        _messages.Remove(id);
        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ClearCallCount++;
        _messages.Clear();
        return ValueTask.CompletedTask;
    }

    public ValueTask<RawCategoryMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
        => ValueTask.FromResult(_messages.TryGetValue(id, out var message) ? Clone(message) : null);

    public ValueTask UpsertAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        UpsertCallCount++;
        _messages[message.Id] = Clone(message);
        return ValueTask.CompletedTask;
    }

    public ValueTask<TableMessageApplyDecision> TryApplyAsync(RawCategoryMessage message, CancellationToken cancellationToken)
    {
        TryApplyCallCount++;

        if (ThrowOnTryApply)
        {
            throw new InvalidOperationException("TryApply failed.");
        }

        if (!_messages.TryGetValue(message.Id, out var current))
        {
            _messages[message.Id] = Clone(message);
            return ValueTask.FromResult(TableMessageApplyDecision.Created());
        }

        if (message.Version <= current.Version)
        {
            return ValueTask.FromResult(TableMessageApplyDecision.NoOp("incoming_version_not_greater_than_current"));
        }

        _messages[message.Id] = Clone(message);
        return ValueTask.FromResult(TableMessageApplyDecision.Updated());
    }

    public Dictionary<string, RawCategoryMessage> GetAll(IValuePredicate<RawCategoryMessage>? valuePredicate = null)
    {
        if (valuePredicate is null)
        {
            return _messages.ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
        }

        return _messages.Where(pair => valuePredicate.Match(pair.Value))
                        .ToDictionary(pair => pair.Key, pair => Clone(pair.Value), StringComparer.Ordinal);
    }

    public RawCategoryMessage? GetById(string id)
        => _messages.TryGetValue(id, out var message) ? Clone(message) : null;

    private static RawCategoryMessage Clone(RawCategoryMessage message)
        => new()
        {
            Id = message.Id,
            Name = message.Name,
            SportId = message.SportId,
            ParentId = message.ParentId,
            SportType = message.SportType,
            CountryCode = message.CountryCode,
            Gender = message.Gender,
            Provider = message.Provider,
            EntityCoverage = message.EntityCoverage,
            Version = message.Version,
            ExternalEntities = message.ExternalEntities.Select(entity => new ExternalEntity
            {
                Id = entity.Id,
                Provider = entity.Provider,
                EntityCoverage = entity.EntityCoverage,
                DefaultName = entity.DefaultName
            }).ToList()
        };
}

internal sealed class FakeRejectedStorage : IRejectedStorage
{
    public RejectedProjection? LastSaved { get; private set; }
    public int SaveCallCount { get; private set; }

    public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken)
    {
        SaveCallCount++;
        LastSaved = rejectedProjection;
        return ValueTask.CompletedTask;
    }
}

internal sealed class FakeCheckpointStorage : ICheckpointStorage
{
    public TopicCheckpoint? LastSaved { get; private set; }
    public List<TopicCheckpoint> SavedCheckpoints { get; } = [];
    public Dictionary<string, TopicCheckpoint> Checkpoints { get; } = new(StringComparer.Ordinal);
    public ViewCheckpoint? LastSavedViewCheckpoint { get; private set; }
    public Dictionary<string, ViewCheckpoint> ViewCheckpoints { get; } = new(StringComparer.Ordinal);
    public string CurrentStoreId { get; set; } = Guid.NewGuid().ToString("D");

    public void Seed(TopicCheckpoint checkpoint)
        => Checkpoints[checkpoint.PhysicalTopic] = checkpoint;

    public void Seed(ViewCheckpoint checkpoint)
        => ViewCheckpoints[checkpoint.ViewName] = checkpoint;

    public Task SaveCheckpointAsync(TopicShard shard, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
    {
        var checkpoint = new TopicCheckpoint(shard.LogicalTopic,
                                             shard.PhysicalTopic,
                                             shard.PartitionId,
                                             shard.IsPartitioned,
                                             lastProcessedMessageId,
                                             Guid.NewGuid(),
                                             DateTimeOffset.UtcNow);
        LastSaved = checkpoint;
        SavedCheckpoints.Add(checkpoint);
        Checkpoints[shard.PhysicalTopic] = checkpoint;
        return Task.CompletedTask;
    }

    public ValueTask<TopicCheckpoint?> GetLastCheckpoint(TopicShard shard, CancellationToken cancellationToken)
        => ValueTask.FromResult(Checkpoints.TryGetValue(shard.PhysicalTopic, out var checkpoint) ? checkpoint : null);

    public Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
    {
        var checkpoint = new ViewCheckpoint(viewName, CurrentStoreId, BuildCompleted: true, DateTimeOffset.UtcNow);
        LastSavedViewCheckpoint = checkpoint;
        ViewCheckpoints[viewName] = checkpoint;
        return Task.CompletedTask;
    }

    public ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
        => ValueTask.FromResult(ViewCheckpoints.TryGetValue(viewName, out var checkpoint) ? checkpoint : null);
}

internal sealed class FakeSportTableViewUnitOfWork : ITableViewUnitOfWork<SportMessage>
{
    public FakeSportTableViewUnitOfWork(FakeSportMessageStorage messageStorage, FakeCheckpointStorage checkpointStorage, FakeRejectedStorage rejectedStorage)
        => (MessageStorage, CheckpointStorage, RejectedStorage) = (messageStorage, checkpointStorage, rejectedStorage);

    public IMessageStorage<string, SportMessage> MessageStorage { get; }
    public ICheckpointStorage CheckpointStorage { get; }
    public IRejectedStorage RejectedStorage { get; }

    public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class FakeCategoryTableViewUnitOfWork : ITableViewUnitOfWork<RawCategoryMessage>
{
    public FakeCategoryTableViewUnitOfWork(FakeRawCategoryMessageStorage messageStorage, FakeCheckpointStorage checkpointStorage, FakeRejectedStorage rejectedStorage)
        => (MessageStorage, CheckpointStorage, RejectedStorage) = (messageStorage, checkpointStorage, rejectedStorage);

    public IMessageStorage<string, RawCategoryMessage> MessageStorage { get; }
    public ICheckpointStorage CheckpointStorage { get; }
    public IRejectedStorage RejectedStorage { get; }

    public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class FakeUnitOfWorkFactory : IUnitOfWorkFactory
{
    private readonly FakeSportTableViewUnitOfWork _unitOfWork;

    public FakeUnitOfWorkFactory(FakeSportTableViewUnitOfWork unitOfWork)
        => _unitOfWork = unitOfWork;

    public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
    {
        if (typeof(TMessage) != typeof(SportMessage))
        {
            throw new NotSupportedException($"Unsupported message type {typeof(TMessage).FullName}.");
        }

        return (ITableViewUnitOfWork<TMessage>)(object)_unitOfWork;
    }

    public IGeoTaxonomyBuildUnitOfWork CreateGeoTaxonomyBuild()
        => new NoOpGeoTaxonomyBuildUnitOfWork();

    public Task MoveDurableAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

internal sealed class NoOpGeoTaxonomyBuildUnitOfWork : IGeoTaxonomyBuildUnitOfWork
{
    public ICategoryRelationIndex CategoryRelationIndex { get; } = new LocalCategoryBySportIndex();
    public ICategoryPendingIndex CategoryPendingIndex { get; } = new LocalOrphanCategoryBySportIndex();
    public IGeoTaxonomyViewStorage MaterializeViewStorage { get; } = new LocalGeoTaxonomyViewStorage();
    public ICheckpointStorage CheckpointStorage { get; } = new FakeCheckpointStorage();

    public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose()
    {
    }
}

internal sealed class LocalCategoryBySportIndex : ICategoryRelationIndex
{
    private readonly Dictionary<SportId, HashSet<CategoryId>> _bySport = new();
    private readonly Dictionary<CategoryId, HashSet<CategoryId>> _byParent = new();

    public ValueTask IndexCategoryAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        GetOrCreate(_bySport, current.SportId).Add(current.CategoryId);
        if (current.ParentCategoryId is not null)
        {
            GetOrCreate(_byParent, current.ParentCategoryId.Value).Add(current.CategoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask ReplaceCategoryRelationsAsync(CategoryRelations? previous, CategoryRelations current, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (previous is not null)
        {
            if (_bySport.TryGetValue(previous.Value.SportId, out var bySport))
            {
                bySport.Remove(previous.Value.CategoryId);
            }

            if (previous.Value.ParentCategoryId is not null && _byParent.TryGetValue(previous.Value.ParentCategoryId.Value, out var byParent))
            {
                byParent.Remove(previous.Value.CategoryId);
            }
        }

        return IndexCategoryAsync(current, cancellationToken);
    }

    public ValueTask RemoveCategoryRelationsAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_bySport.TryGetValue(current.SportId, out var bySport))
        {
            bySport.Remove(current.CategoryId);
        }

        if (current.ParentCategoryId is not null && _byParent.TryGetValue(current.ParentCategoryId.Value, out var byParent))
        {
            byParent.Remove(current.CategoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(_bySport.TryGetValue(sportId, out var set) ? set.ToHashSet() : []);
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(_byParent.TryGetValue(parentCategoryId, out var set) ? set.ToHashSet() : []);
    }

    public ValueTask<bool> HasCategoryBySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_bySport.TryGetValue(sportId, out var set) && set.Contains(categoryId));
    }

    public ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_byParent.TryGetValue(parentCategoryId, out var set) && set.Contains(categoryId));
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _bySport.Clear();
        _byParent.Clear();
        return ValueTask.CompletedTask;
    }

    private static HashSet<TValue> GetOrCreate<TKey, TValue>(Dictionary<TKey, HashSet<TValue>> source, TKey key)
        where TKey : notnull
    {
        if (!source.TryGetValue(key, out var set))
        {
            set = [];
            source[key] = set;
        }

        return set;
    }
}

internal sealed class LocalOrphanCategoryBySportIndex : ICategoryPendingIndex
{
    private readonly Dictionary<SportId, HashSet<CategoryId>> _waitingForSport = new();
    private readonly Dictionary<CategoryId, HashSet<SportId>> _missingSports = new();

    public ValueTask<bool> TryMarkCategoryWaitingForSportAsync(SportId sportId,
                                                               CategoryId categoryId,
                                                               Func<SportId, CancellationToken, ValueTask<bool>> sportExistsCheck,
                                                               CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (sportExistsCheck(sportId, cancellationToken).GetAwaiter().GetResult())
        {
            return ValueTask.FromResult(false);
        }

        GetOrCreate(_waitingForSport, sportId).Add(categoryId);
        GetOrCreate(_missingSports, categoryId).Add(sportId);
        return ValueTask.FromResult(true);
    }

    public ValueTask ResolveCategoryWaitingForSportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RemovePending(sportId, categoryId);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesWaitingForSportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(_waitingForSport.TryGetValue(sportId, out var set) ? set.ToHashSet() : []);
    }

    public ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult<IReadOnlySet<SportId>>(_missingSports.TryGetValue(categoryId, out var set) ? set.ToHashSet() : []);
    }

    public ValueTask RemoveCategoryFromPendingAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_missingSports.TryGetValue(categoryId, out var sports))
        {
            foreach (var sportId in sports.ToArray())
            {
                RemovePending(sportId, categoryId);
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _waitingForSport.Clear();
        _missingSports.Clear();
        return ValueTask.CompletedTask;
    }

    private void RemovePending(SportId sportId, CategoryId categoryId)
    {
        if (_waitingForSport.TryGetValue(sportId, out var categories))
        {
            categories.Remove(categoryId);
        }

        if (_missingSports.TryGetValue(categoryId, out var sports))
        {
            sports.Remove(sportId);
        }
    }

    private static HashSet<TValue> GetOrCreate<TKey, TValue>(Dictionary<TKey, HashSet<TValue>> source, TKey key)
        where TKey : notnull
    {
        if (!source.TryGetValue(key, out var set))
        {
            set = [];
            source[key] = set;
        }

        return set;
    }
}

internal sealed class LocalGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
{
    private readonly Dictionary<SportId, GeoTaxonomyViewMessage> _views = new();
    private readonly Dictionary<SportId, GeoTaxonomyViewMetadata> _metadata = new();

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var current = _views.TryGetValue(sportId, out var existing) ? existing : GeoTaxonomyViewMessage.CreateNew(sportId.Value, sportName, sportType);
        if (current.SportName == sportName && current.SportType == sportType)
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(current));
        }

        var updated = current with { SportName = sportName, SportType = sportType, Version = current.Version + 1 };
        _views[sportId] = updated;
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var nextVersion = (_metadata.TryGetValue(sportId, out var metadata) ? Math.Max(metadata.CalculatedVersion, metadata.PublishedVersion) : 0) + 1;
        var versionedView = view with { Version = checked((int)nextVersion) };
        var updatedMetadata = new GeoTaxonomyViewMetadata
        {
            CalculatedVersion = nextVersion,
            PublishedVersion = _metadata.TryGetValue(sportId, out var existingMetadata) ? existingMetadata.PublishedVersion : 0,
            BuildGenerationId = buildGenerationId,
            UpdatedAtUtc = DateTimeOffset.UtcNow,
            PublishedAtUtc = _metadata.TryGetValue(sportId, out var currentMetadata) ? currentMetadata.PublishedAtUtc : null
        };

        _views[sportId] = versionedView;
        _metadata[sportId] = updatedMetadata;

        return ValueTask.FromResult(new GeoTaxonomyViewUpsertResult
        {
            SportId = sportId,
            CalculatedVersion = nextVersion,
            PublishedVersion = updatedMetadata.PublishedVersion,
            BuildGenerationId = buildGenerationId,
            View = versionedView
        });
    }

    public ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_metadata.TryGetValue(sportId, out var metadata))
        {
            _metadata[sportId] = metadata with
            {
                PublishedVersion = calculatedVersion,
                PublishedAtUtc = DateTimeOffset.UtcNow
            };
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var existing = view.GeoCategories.FirstOrDefault(category => category.CategoryId == node.CategoryId);
        if (existing is not null && existing.Equals(node))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var updated = view with { GeoCategories = view.GeoCategories.Where(category => category.CategoryId != node.CategoryId).Append(node).ToImmutableHashSet(), Version = view.Version + 1 };
        _views[sportId] = updated;
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var removed = view.GeoCategories.Where(category => category.CategoryId == categoryId.Value).ToArray();
        if (removed.Length == 0)
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var updated = view with { GeoCategories = view.GeoCategories.Except(removed).ToImmutableHashSet(), Version = view.Version + 1 };
        _views[sportId] = updated;
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_views.TryGetValue(sportId, out var view) ? view : null);
    }

    public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _metadata.Remove(sportId);
        return ValueTask.FromResult(_views.Remove(sportId, out var view) ? view : null);
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.Clear();
        _metadata.Clear();
        return ValueTask.CompletedTask;
    }
}

internal sealed class FakeProjectorTopicReaderFactory : ITopicShardReaderStrategy
{
    private readonly Dictionary<TopicShard, Queue<TableViewMessage>> _messages = new();
    private readonly Dictionary<string, TopicHighWatermark> _highWatermarks = new(StringComparer.Ordinal);

    public MessageId? LastStartMessageId { get; private set; }

    public void SeedHighWatermark(string topicName, int partitionId, MessageId messageId)
    {
        var shard = CreateShard(topicName, partitionId);
        _highWatermarks[topicName] = new TopicHighWatermark(topicName, [new TopicShardHighWatermark(shard, messageId)]);
    }

    public void SeedMessages(string topicName, int partitionId, params TableViewMessage[] messages)
        => _messages[CreateShard(topicName, partitionId)] = new Queue<TableViewMessage>(messages);

    public Task<TopicHighWatermark> CaptureHighWatermarkAsync(string topicName, CancellationToken cancellationToken)
        => Task.FromResult(_highWatermarks[topicName]);

    public Task<IReadOnlyCollection<TopicShard>> DiscoverShardsAsync(string logicalTopic, CancellationToken cancellationToken)
        => Task.FromResult<IReadOnlyCollection<TopicShard>>(_highWatermarks.TryGetValue(logicalTopic, out var highWatermark)
            ? highWatermark.Shards
            : [TopicShard.NonPartitioned(logicalTopic)]);

    public Task<IProjectorTopicReader> CreateReaderAsync(TopicShard shard, MessageId startMessageId, CancellationToken cancellationToken)
    {
        LastStartMessageId = startMessageId;
        _messages.TryGetValue(shard, out var messages);
        return Task.FromResult<IProjectorTopicReader>(new FakeProjectorTopicReader(messages ?? new Queue<TableViewMessage>()));
    }

    private static TopicShard CreateShard(string topicName, int partitionId)
        => partitionId < 0 ? TopicShard.NonPartitioned(topicName) : TopicShard.Partition(topicName, partitionId);

    private sealed class FakeProjectorTopicReader : IProjectorTopicReader
    {
        private readonly Queue<TableViewMessage> _messages;

        public FakeProjectorTopicReader(Queue<TableViewMessage> messages)
            => _messages = messages;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<TableViewMessage> ReceiveAsync(CancellationToken cancellationToken)
        {
            if (_messages.Count == 0)
            {
                throw new InvalidOperationException("No more bootstrap messages were configured.");
            }

            return Task.FromResult(_messages.Dequeue());
        }

        public async IAsyncEnumerable<TableViewMessage> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (_messages.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return _messages.Dequeue();
                await Task.Yield();
            }
        }
    }
}

internal sealed class TestLogger<T> : ILogger<T>
{
    public List<string> Messages { get; } = [];

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => Messages.Add(formatter(state, exception));

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();
        public void Dispose() { }
    }
}
