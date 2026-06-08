using System.Reactive.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Processor;
using Xunit;

namespace PoC.Pulsar.TableView.Observability.UnitTests;

public sealed class GeoTaxonomyProcessorObservabilityTests
{
    [Fact]
    [Trait("Category", "UnitTest")]
    public async Task run_async_should_record_rebuild_metrics()
    {
        using var collector = new ObservabilityCollector();
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(metadata);
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>([Category("category-es", "sport-1", "ES")]);
        var processor = new GeoTaxonomyProcessor(sports,
                                                 categories,
                                                 new FakeTaxonomyViewPublisher(),
                                                 unitOfWorkFactory,
                                                 metadata,
                                                 NullLogger<GeoTaxonomyProcessor>.Instance);

        await processor.RunAsync(CancellationToken.None);

        Assert.True(collector.HasLongSum("projector.geo_taxonomy.rebuilds.total",
                                         1,
                                         new("operation", "rebuild"),
                                         new("phase", "rebuild"),
                                         new("result", "success")));
        Assert.True(collector.HasLongSum("projector.geo_taxonomy.views.published.total",
                                         1,
                                         new("operation", "save_publish_view"),
                                         new("result", "success"),
                                         new("entity_type", "taxonomy_view")));
        Assert.True(collector.HasHistogramPoint("projector.geo_taxonomy.operation.duration.ms",
                                                new("operation", "rebuild"),
                                                new("result", "success")));
    }

    [Fact]
    [Trait("Category", "UnitTest")]
    public async Task run_async_should_record_pending_category_metric_when_sport_is_missing()
    {
        using var collector = new ObservabilityCollector();
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(metadata);
        var sports = new FakePulsarTableView<SportMessage>([]);
        var categories = new FakePulsarTableView<RawCategoryMessage>([Category("category-es", "missing-sport", "ES")]);
        var processor = new GeoTaxonomyProcessor(sports,
                                                 categories,
                                                 new FakeTaxonomyViewPublisher(),
                                                 unitOfWorkFactory,
                                                 metadata,
                                                 NullLogger<GeoTaxonomyProcessor>.Instance);

        await processor.RunAsync(CancellationToken.None);

        Assert.True(collector.HasLongSum("projector.geo_taxonomy.pending.categories.total",
                                         1,
                                         new("operation", "pending_category"),
                                         new("phase", "rebuild"),
                                         new("result", "success"),
                                         new("entity_type", "category")));
    }

    private static SportMessage Sport(string id, string name, string sportType)
        => new()
        {
            Id = id,
            Name = name,
            SportType = sportType,
            Provider = "provider",
            EntityCoverage = "covered"
        };

    private static RawCategoryMessage Category(string id, string sportId, string? countryCode)
        => new()
        {
            Id = id,
            Name = id,
            SportId = sportId,
            CountryCode = countryCode,
            Provider = "provider",
            EntityCoverage = "covered"
        };

    private sealed class FakePulsarTableView<T> : IPulsarTableView<T>
        where T : class
    {
        private readonly Dictionary<string, T> _items = new(StringComparer.Ordinal);

        public FakePulsarTableView(IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                var id = item switch
                {
                    SportMessage sport => sport.Id,
                    RawCategoryMessage category => category.Id,
                    _ => throw new NotSupportedException(typeof(T).Name)
                };
                _items[id] = item;
            }
        }

        public IObservable<TableEntryChange<T>> OnChanges => Observable.Empty<TableEntryChange<T>>();

        public ValueTask<T?> GetEntry(string key, CancellationToken cancellationToken)
            => ValueTask.FromResult(_items.TryGetValue(key, out var item) ? item : null);

        public IDictionary<string, T> GetSnapshot(IValuePredicate<T>? filter = null)
            => filter is null
                ? new Dictionary<string, T>(_items, StringComparer.Ordinal)
                : _items.Where(item => filter.Match(item.Value)).ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);

        public Task<TopicBootstrapResult<T>> StartBootstrapAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<TopicBootstrapResult<T>>(new TopicRecoveredFromStateStore<T>([]));

        public Task StartLiveTailAsync(CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class FakeUnitOfWorkFactory : IUnitOfWorkFactory
    {
        private readonly FakeCheckpointStorage _checkpointStorage;
        private readonly FakeGeoTaxonomyBuildUnitOfWork _unitOfWork;

        public FakeUnitOfWorkFactory(StoreMetadata metadata)
        {
            _ = metadata;
            _checkpointStorage = new FakeCheckpointStorage();
            _unitOfWork = new FakeGeoTaxonomyBuildUnitOfWork(_checkpointStorage);
        }

        public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
            => new FakeTableViewUnitOfWork<TMessage>(_checkpointStorage);

        public IGeoTaxonomyBuildUnitOfWork CreateGeoTaxonomyBuild() => _unitOfWork;

        public Task MoveDurableAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class FakeGeoTaxonomyBuildUnitOfWork(ICheckpointStorage checkpointStorage) : IGeoTaxonomyBuildUnitOfWork
    {
        public ICategoryRelationIndex CategoryRelationIndex { get; } = new FakeCategoryRelationIndex();
        public ICategoryPendingIndex CategoryPendingIndex { get; } = new FakeCategoryPendingIndex();
        public IGeoTaxonomyViewStorage MaterializeViewStorage { get; } = new FakeGeoTaxonomyViewStorage();
        public ICheckpointStorage CheckpointStorage { get; } = checkpointStorage;
        public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;
        public void Dispose() { }
    }

    private sealed class FakeTableViewUnitOfWork<TMessage>(ICheckpointStorage checkpointStorage) : ITableViewUnitOfWork<TMessage>
    {
        public IMessageStorage<string, TMessage> MessageStorage { get; } = new FakeMessageStorage<TMessage>();
        public ICheckpointStorage CheckpointStorage { get; } = checkpointStorage;
        public IRejectedStorage RejectedStorage { get; } = new FakeRejectedStorage();
        public Task CommitAsync(CancellationToken ct) => Task.CompletedTask;
        public void Dispose() { }
    }

    private sealed class FakeCheckpointStorage : ICheckpointStorage
    {
        public Task SaveCheckpointAsync(TopicShard shard, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask<TopicCheckpoint?> GetLastCheckpoint(TopicShard shard, CancellationToken cancellationToken) => ValueTask.FromResult<TopicCheckpoint?>(null);
        public Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken) => ValueTask.FromResult<ViewCheckpoint?>(null);
    }

    private sealed class FakeMessageStorage<TMessage> : IMessageStorage<string, TMessage>
    {
        public ValueTask DeleteAsync(string id, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask ClearAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TMessage?> TryLoadAsync(string id, CancellationToken cancellationToken) => ValueTask.FromResult<TMessage?>(default);
        public ValueTask UpsertAsync(TMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TableMessageApplyDecision> TryApplyAsync(TMessage message, CancellationToken cancellationToken) => ValueTask.FromResult(TableMessageApplyDecision.Created());
        public Dictionary<string, TMessage> GetAll(IValuePredicate<TMessage>? valuePredicate = null) => [];
    }

    private sealed class FakeRejectedStorage : IRejectedStorage
    {
        public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class FakeCategoryRelationIndex : ICategoryRelationIndex
    {
        private readonly Dictionary<string, HashSet<CategoryId>> _bySport = new(StringComparer.Ordinal);

        public ValueTask IndexCategoryAsync(CategoryRelations current, CancellationToken cancellationToken)
        {
            if (!_bySport.TryGetValue(current.SportId.Value, out var categories))
            {
                categories = [];
                _bySport[current.SportId.Value] = categories;
            }

            categories.Add(current.CategoryId);
            return ValueTask.CompletedTask;
        }

        public ValueTask ReplaceCategoryRelationsAsync(CategoryRelations? previous, CategoryRelations current, CancellationToken cancellationToken) => IndexCategoryAsync(current, cancellationToken);
        public ValueTask RemoveCategoryRelationsAsync(CategoryRelations current, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlySet<CategoryId>>(_bySport.TryGetValue(sportId.Value, out var categories) ? categories : new HashSet<CategoryId>());
        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlySet<CategoryId>>(new HashSet<CategoryId>());
        public ValueTask<bool> HasCategoryBySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.FromResult(false);
        public ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.FromResult(false);
        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            _bySport.Clear();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeCategoryPendingIndex : ICategoryPendingIndex
    {
        public ValueTask<bool> TryMarkCategoryWaitingForSportAsync(SportId sportId, CategoryId categoryId, Func<SportId, CancellationToken, ValueTask<bool>> sportExistsCheck, CancellationToken cancellationToken) => ValueTask.FromResult(true);
        public ValueTask ResolveCategoryWaitingForSportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesWaitingForSportAsync(SportId sportId, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlySet<CategoryId>>(new HashSet<CategoryId>());
        public ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlySet<SportId>>(new HashSet<SportId>());
        public ValueTask RemoveCategoryFromPendingAsync(CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask ClearAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class FakeGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
    {
        public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken) => ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        public ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken)
            => ValueTask.FromResult(new GeoTaxonomyViewUpsertResult { SportId = sportId, CalculatedVersion = 1, PublishedVersion = 0, BuildGenerationId = buildGenerationId, View = view with { Version = 1 } });
        public ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken) => ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        public ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken) => ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken) => ValueTask.FromResult<GeoTaxonomyViewMessage?>(null);
        public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken) => ValueTask.FromResult<GeoTaxonomyViewMessage?>(null);
        public ValueTask ClearAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class FakeTaxonomyViewPublisher : ITaxonomyViewPublisher
    {
        public ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask PublishListMessage(IEnumerable<GeoTaxonomyViewMessage> taxonomies, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask PublishDeleteMessageAsync(string sportId, DateTimeOffset eventTimestamp, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
