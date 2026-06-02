using System.Reactive.Subjects;
using Microsoft.Extensions.Logging.Abstractions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using Xunit;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class GeoTaxonomyProcessorTests
{
    [Fact]
    public async Task run_async_should_rebuild_projector_state_when_view_checkpoint_is_missing()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        dependencies.ViewStorage.AddTaxonomyView(new SportId("stale-sport"), GeoTaxonomyViewMessage.Create(Sport("stale-sport", "Stale", "STALE"), []));
        await dependencies.RelationIndex.AddCategorybySportAsync(new SportId("stale-sport"), new CategoryId("stale-category"), CancellationToken.None);
        await dependencies.PendingIndex.AddOrphanCategorybySportAsync(new SportId("stale-sport"), new CategoryId("stale-category"), CancellationToken.None);

        var sports = new FakePulsarTableView<SportMessage>(
            [Sport("sport-1", "Soccer", "SOCCER")],
            new TopicRecoveredFromStateStore<SportMessage>([]));
        var categories = new FakePulsarTableView<RawCategoryMessage>(
            [Category("category-es", "sport-1", "ES")],
            new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);

        Assert.Equal(1, dependencies.ViewStorage.ClearCallCount);
        Assert.Equal(1, dependencies.RelationIndex.ClearCallCount);
        Assert.Equal(1, dependencies.PendingIndex.ClearCallCount);
        Assert.NotNull(dependencies.CheckpointStorage.LastSavedViewCheckpoint);
        var savedCheckpoint = dependencies.CheckpointStorage.LastSavedViewCheckpoint!;
        Assert.Equal("category-taxonomy", savedCheckpoint.ViewName);
        Assert.Equal(metadata.StoreGenerationId.ToString("D"), savedCheckpoint.StoreId);
        Assert.Single(publisher.PublishedLists);
    }

    [Fact]
    public async Task run_async_should_apply_bootstrap_deltas_when_view_checkpoint_is_trusted()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        dependencies.CheckpointStorage.Seed(new ViewCheckpoint("category-taxonomy", metadata.StoreGenerationId.ToString("D"), BuildCompleted: true, DateTimeOffset.UtcNow));
        dependencies.ViewStorage.AddTaxonomyView(new SportId("sport-1"), GeoTaxonomyViewMessage.Create(Sport("sport-1", "Soccer", "SOCCER"), []));

        var sports = new FakePulsarTableView<SportMessage>(
            [Sport("sport-1", "Soccer Updated", "SOCCER")],
            new TopicRecoveredFromStateStore<SportMessage>(
            [
                new TableEntryUpdated<SportMessage>("sport-1", Sport("sport-1", "Soccer Updated", "SOCCER"), Sport("sport-1", "Soccer", "SOCCER"))
            ]));
        var categories = new FakePulsarTableView<RawCategoryMessage>([], new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal(0, dependencies.ViewStorage.ClearCallCount);
        Assert.Equal(0, dependencies.RelationIndex.ClearCallCount);
        Assert.Equal(0, dependencies.PendingIndex.ClearCallCount);
        Assert.Equal("Soccer Updated", taxonomy[0].SportName);
        Assert.Equal(metadata.StoreGenerationId.ToString("D"), dependencies.CheckpointStorage.LastSavedViewCheckpoint!.StoreId);
    }

    [Fact]
    public async Task run_async_should_rebuild_projector_state_when_view_checkpoint_store_id_mismatches()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        dependencies.CheckpointStorage.Seed(new ViewCheckpoint("category-taxonomy", Guid.NewGuid().ToString("D"), BuildCompleted: true, DateTimeOffset.UtcNow));

        var sports = new FakePulsarTableView<SportMessage>(
            [Sport("sport-1", "Soccer", "SOCCER")],
            new TopicRecoveredFromStateStore<SportMessage>([]));
        var categories = new FakePulsarTableView<RawCategoryMessage>([], new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);

        Assert.Equal(1, dependencies.ViewStorage.ClearCallCount);
        Assert.Equal(metadata.StoreGenerationId.ToString("D"), dependencies.CheckpointStorage.LastSavedViewCheckpoint!.StoreId);
        Assert.Single(publisher.PublishedLists);
    }

    [Fact]
    public async Task run_async_should_bootstrap_both_table_views_before_live_tail()
    {
        var sports = new FakePulsarTableView<SportMessage>();
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var processor = CreateProcessor(sports, categories, new FakeTaxonomyViewPublisher());
        using var cancellationTokenSource = new CancellationTokenSource();

        var runTask = processor.RunAsync(cancellationTokenSource.Token);
        await Task.WhenAll(sports.WaitForLiveTailStartedAsync(), categories.WaitForLiveTailStartedAsync());
        await cancellationTokenSource.CancelAsync();
        await runTask;

        Assert.True(sports.BootstrapCompletedBeforeLiveTail);
        Assert.True(categories.BootstrapCompletedBeforeLiveTail);
    }

    [Fact]
    public async Task sport_update_should_publish_taxonomy_for_updated_sport()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>(
        [
            Category("category-es", "sport-1", "ES"),
            Category("category-empty", "sport-1", ""),
            Category("category-null", "sport-1", null),
            Category("category-other", "sport-2", "GB")
        ]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        sports.EmitUpdate("sport-1", Sport("sport-1", "Soccer Updated", "SOCCER"), Sport("sport-1", "Soccer", "SOCCER"));
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal("sport-1", taxonomy[0].SportId);
        Assert.Equal("Soccer Updated", taxonomy[0].SportName);
        Assert.Equal("SOCCER", taxonomy[0].SportType);
        Assert.Equal(["ES"], taxonomy[0].GeoCategories.Select(category => category.CountryCode));
    }

    [Fact]
    public async Task sport_update_should_publish_distinct_ordered_country_codes()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>(
        [
            Category("category-us", "sport-1", "US"),
            Category("category-es", "sport-1", "ES"),
            Category("category-us-duplicate", "sport-1", "US")
        ]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        sports.EmitUpdate("sport-1", Sport("sport-1", "Soccer Updated", "SOCCER"), Sport("sport-1", "Soccer", "SOCCER"));
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal(["ES", "US"], taxonomy[0].GeoCategories.Select(category => category.CountryCode).OrderBy(code => code));
    }

    [Fact]
    public async Task sport_delete_should_publish_tombstone_when_sport_is_missing()
    {
        var sports = new FakePulsarTableView<SportMessage>();
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        sports.EmitDelete("sport-1", Sport("sport-1", "Soccer", "SOCCER"));
        var tombstones = await publisher.WaitForDeletedCountAsync(1);

        Assert.Equal(["sport-1"], tombstones);
        Assert.Empty(publisher.PublishedTaxonomies);
    }

    [Fact]
    public async Task category_update_should_publish_taxonomy_for_category_sport()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var oldCategory = Category("category-es", "sport-1", null);
        categories.Upsert(oldCategory.Id, oldCategory);
        var category = Category("category-es", "sport-1", "ES");
        categories.EmitUpdate(category.Id, category, oldCategory);
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal("sport-1", taxonomy[0].SportId);
        Assert.Equal(["ES"], taxonomy[0].GeoCategories.Select(categoryNode => categoryNode.CountryCode));
    }

    [Fact]
    public async Task category_update_should_publish_previous_and_new_sport_when_sport_changes()
    {
        var sports = new FakePulsarTableView<SportMessage>(
        [
            Sport("sport-1", "Soccer", "SOCCER"),
            Sport("sport-2", "Basketball", "GENERIC")
        ]);
        var existingCategory = Category("category-es", "sport-1", "ES");
        var categories = new FakePulsarTableView<RawCategoryMessage>([existingCategory]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var movedCategory = Category("category-es", "sport-2", "ES");
        categories.Upsert(movedCategory.Id, movedCategory);
        categories.EmitUpdate(movedCategory.Id, movedCategory, existingCategory);
        var taxonomies = await publisher.WaitForPublishedCountAsync(2);

        Assert.Equal(["sport-1", "sport-2"], taxonomies.Select(taxonomy => taxonomy.SportId));
        Assert.Empty(taxonomies[0].GeoCategories);
        Assert.Equal(["ES"], taxonomies[1].GeoCategories.Select(category => category.CountryCode));
    }

    [Fact]
    public async Task category_delete_should_publish_taxonomy_for_mapped_sport()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var existingCategory = Category("category-es", "sport-1", "ES");
        var categories = new FakePulsarTableView<RawCategoryMessage>([existingCategory]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        categories.Delete(existingCategory.Id);
        categories.EmitDelete(existingCategory.Id, existingCategory);
        var taxonomies = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal("sport-1", taxonomies[0].SportId);
        Assert.Empty(taxonomies[0].GeoCategories);
    }

    [Fact]
    public async Task category_delete_should_not_publish_when_category_mapping_is_missing()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        categories.EmitDelete("missing-category", Category("missing-category", "sport-1", null));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Empty(publisher.PublishedTaxonomies);
        Assert.Empty(publisher.DeletedSportIds);
    }

    private static GeoTaxonomyProcessor CreateProcessor(
        IPulsarTableView<SportMessage> sports,
        IPulsarTableView<RawCategoryMessage> categories,
        ITaxonomyViewPublisher publisher,
        ProcessorDependencies? dependencies = null)
    {
        dependencies ??= CreateDependencies(new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow));

        return new GeoTaxonomyProcessor(
            sports,
            categories,
            publisher,
            dependencies.RelationIndex,
            dependencies.PendingIndex,
            dependencies.ViewStorage,
            dependencies.UnitOfWorkFactory,
            dependencies.StoreMetadata,
            NullLogger<GeoTaxonomyProcessor>.Instance);
    }

    private static ProcessorDependencies CreateDependencies(StoreMetadata storeMetadata)
    {
        var checkpointStorage = new FakeCheckpointStorage
        {
            CurrentStoreId = storeMetadata.StoreGenerationId.ToString("D")
        };

        return new ProcessorDependencies(storeMetadata,
                                         checkpointStorage,
                                         new TrackingCategoryBySportIndex(),
                                         new TrackingOrphanCategoryBySportIndex(),
                                         new TrackingGeoTaxonomyViewStorage(),
                                         new FakeUnitOfWorkFactory(checkpointStorage));
    }

    private static SportMessage Sport(string id, string name, string sportType)
    {
        return new SportMessage
        {
            Id = id,
            Name = name,
            SportType = sportType,
            Provider = "provider",
            EntityCoverage = "covered"
        };
    }

    private static RawCategoryMessage Category(string id, string sportId, string? countryCode)
    {
        return new RawCategoryMessage
        {
            Id = id,
            Name = id,
            SportId = sportId,
            CountryCode = countryCode,
            Provider = "provider",
            EntityCoverage = "covered"
        };
    }

    private sealed class FakePulsarTableView<T> : IPulsarTableView<T>
        where T : class
    {
        private readonly Dictionary<string, T> _items = new(StringComparer.Ordinal);
        private readonly Subject<TableEntryChange<T>> _updates = new();
        private readonly TaskCompletionSource _liveTailStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TopicBootstrapResult<T> _bootstrapResult;

        public FakePulsarTableView(IReadOnlyList<T>? items = null, TopicBootstrapResult<T>? bootstrapResult = null)
        {
            _bootstrapResult = bootstrapResult ?? new TopicHighWatermarkNotFound<T>();

            if (items is null)
            {
                return;
            }

            foreach (var item in items)
            {
                var id = (string)item.GetType().GetProperty(nameof(Entity.Id))!.GetValue(item)!;
                _items[id] = item;
            }
        }

        public IObservable<TableEntryChange<T>> OnChanges => _updates;

        public bool BootstrapCompleted { get; private set; }

        public bool BootstrapCompletedBeforeLiveTail { get; private set; }

        public T? Get(string key)
        {
            return _items.GetValueOrDefault(key);
        }

        public ValueTask<T?> GetEntry(string key, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(Get(key));
        }

        public IDictionary<string, T> GetSnapshot(IValuePredicate<T>? filter = null)
        {
            if (filter is null)
            {
                return new Dictionary<string, T>(_items, StringComparer.Ordinal);
            }

            return _items.Where(entry => filter.Match(entry.Value))
                         .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);
        }

        public async IAsyncEnumerable<T> GetAllAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var item in GetSnapshot().Values.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
                await Task.Yield();
            }
        }

        public Task<TopicBootstrapResult<T>> StartBootstrapAsync(CancellationToken cancellationToken = default)
        {
            BootstrapCompleted = true;
            return Task.FromResult(_bootstrapResult);
        }

        public async Task StartLiveTailAsync(CancellationToken cancellationToken)
        {
            BootstrapCompletedBeforeLiveTail = BootstrapCompleted;
            _liveTailStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        public Task WaitForLiveTailStartedAsync()
        {
            return _liveTailStarted.Task;
        }

        public void Upsert(string key, T value)
        {
            _items[key] = value;
        }

        public void Delete(string key)
        {
            _items.Remove(key);
        }

        public void EmitUpdate(string key, T value, T oldValue)
        {
            _updates.OnNext(new TableEntryUpdated<T>(key, value, oldValue));
        }

        public void EmitUpdate(string key, T value)
        {
            _updates.OnNext(new TableEntryUpdated<T>(key, value, value));
        }

        public void EmitDelete(string key, T value)
        {
            _updates.OnNext(new EventDeleted<T>(key, value));
        }

        public void EmitDelete(string key)
        {
            var value = Get(key) ?? throw new InvalidOperationException($"Missing item for key {key}.");
            _updates.OnNext(new EventDeleted<T>(key, value));
        }

        
    }

    private sealed class FakeTaxonomyViewPublisher : ITaxonomyViewPublisher
    {
        private readonly object _gate = new();
        private TaskCompletionSource _changed = NewCompletionSource();

        public List<GeoTaxonomyViewMessage> PublishedTaxonomies { get; } = [];
        public List<IReadOnlyList<GeoTaxonomyViewMessage>> PublishedLists { get; } = [];

        public List<string> DeletedSportIds { get; } = [];

        public ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                PublishedTaxonomies.Add(taxonomy);
                _changed.TrySetResult();
                _changed = NewCompletionSource();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask PublishListMessage(IEnumerable<GeoTaxonomyViewMessage> taxonomies, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                PublishedLists.Add(taxonomies.ToArray());
                _changed.TrySetResult();
                _changed = NewCompletionSource();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask PublishDeleteMessageAsync(string sportId, DateTimeOffset eventTimestamp, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                DeletedSportIds.Add(sportId);
                _changed.TrySetResult();
                _changed = NewCompletionSource();
            }

            return ValueTask.CompletedTask;
        }

        public async Task<IReadOnlyList<GeoTaxonomyViewMessage>> WaitForPublishedCountAsync(int expectedCount)
        {
            while (true)
            {
                Task waitTask;
                lock (_gate)
                {
                    if (PublishedTaxonomies.Count >= expectedCount)
                    {
                        return PublishedTaxonomies.ToArray();
                    }

                    waitTask = _changed.Task;
                }

                await waitTask.WaitAsync(TimeSpan.FromSeconds(2));
            }
        }

        public async Task<IReadOnlyList<string>> WaitForDeletedCountAsync(int expectedCount)
        {
            while (true)
            {
                Task waitTask;
                lock (_gate)
                {
                    if (DeletedSportIds.Count >= expectedCount)
                    {
                        return DeletedSportIds.ToArray();
                    }

                    waitTask = _changed.Task;
                }

                await waitTask.WaitAsync(TimeSpan.FromSeconds(2));
            }
        }

        private static TaskCompletionSource NewCompletionSource()
        {
            return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private sealed class ProcessorRunner : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task _runTask;

        private ProcessorRunner(CancellationTokenSource cancellationTokenSource, Task runTask)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _runTask = runTask;
        }

        public static async Task<ProcessorRunner> StartAsync(
            GeoTaxonomyProcessor processor,
            FakePulsarTableView<SportMessage> sports,
            FakePulsarTableView<RawCategoryMessage> categories)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var runTask = processor.RunAsync(cancellationTokenSource.Token);

            await Task.WhenAll(sports.WaitForLiveTailStartedAsync(), categories.WaitForLiveTailStartedAsync());

            return new ProcessorRunner(cancellationTokenSource, runTask);
        }

        public async ValueTask DisposeAsync()
        {
            await _cancellationTokenSource.CancelAsync();
            await _runTask;
            _cancellationTokenSource.Dispose();
        }
    }

    private sealed record ProcessorDependencies(StoreMetadata StoreMetadata,
                                                FakeCheckpointStorage CheckpointStorage,
                                                TrackingCategoryBySportIndex RelationIndex,
                                                TrackingOrphanCategoryBySportIndex PendingIndex,
                                                TrackingGeoTaxonomyViewStorage ViewStorage,
                                                FakeUnitOfWorkFactory UnitOfWorkFactory);

    private sealed class TrackingCategoryBySportIndex : ICategoryBySportIndex
    {
        private readonly InMemoryCategoryBySportIndex _inner = new();

        public int ClearCallCount { get; private set; }

        public ValueTask AddCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.AddCategorybySportAsync(sportId, categoryId, cancellationToken);

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            return _inner.ClearAsync(cancellationToken);
        }

        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySport(SportId sportId, CancellationToken cancellationToken)
            => _inner.GetCategoriesBySport(sportId, cancellationToken);

        public ValueTask RemoveCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.RemoveCategorybySportAsync(sportId, categoryId, cancellationToken);

        public ValueTask ClearCategoryWithSportIdAsync(SportId sportId, CancellationToken cancellationToken)
            => _inner.ClearCategoryWithSportIdAsync(sportId, cancellationToken);

        public ValueTask AddCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.AddCategoryByParentAsync(parentCategoryId, categoryId, cancellationToken);

        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParent(CategoryId parentCategoryId, CancellationToken cancellationToken)
            => _inner.GetCategoriesByParent(parentCategoryId, cancellationToken);

        public ValueTask RemoveCategorybyParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.RemoveCategorybyParentAsync(parentCategoryId, categoryId, cancellationToken);

        public ValueTask ClearCategoryWithParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
            => _inner.ClearCategoryWithParentAsync(parentCategoryId, cancellationToken);
    }

    private sealed class TrackingOrphanCategoryBySportIndex : IOrphanCategoryBySportIndex
    {
        private readonly InMemoryOrphanCategoryBySportIndex _inner = new();

        public int ClearCallCount { get; private set; }

        public ValueTask AddOrphanCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.AddOrphanCategorybySportAsync(sportId, categoryId, cancellationToken);

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            return _inner.ClearAsync(cancellationToken);
        }

        public ValueTask ClearOrphanCategoryWithSportIdAsync(SportId sportId, CancellationToken cancellationToken)
            => _inner.ClearOrphanCategoryWithSportIdAsync(sportId, cancellationToken);

        public ValueTask<IReadOnlySet<CategoryId>> GetOrphanCategoriesBySport(SportId sportId, CancellationToken cancellationToken)
            => _inner.GetOrphanCategoriesBySport(sportId, cancellationToken);

        public ValueTask RemoveOrphanCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.RemoveOrphanCategorybySportAsync(sportId, categoryId, cancellationToken);

        public ValueTask AddOrphanCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.AddOrphanCategoryByParentAsync(parentCategoryId, categoryId, cancellationToken);

        public ValueTask<IReadOnlySet<CategoryId>> GetOrphanCategoriesByParent(CategoryId parentCategoryId, CancellationToken cancellationToken)
            => _inner.GetOrphanCategoriesByParent(parentCategoryId, cancellationToken);

        public ValueTask RemoveOrphanCategorybyParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.RemoveOrphanCategorybyParentAsync(parentCategoryId, categoryId, cancellationToken);

        public ValueTask ClearOrphanCategoryWithParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
            => _inner.ClearOrphanCategoryWithParentAsync(parentCategoryId, cancellationToken);
    }

    private sealed class TrackingGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
    {
        private readonly InMemoryGeoTaxonomyViewStorage _inner = new();

        public int ClearCallCount { get; private set; }

        public void AddTaxonomyView(SportId id, GeoTaxonomyViewMessage view)
            => _inner.AddTaxonomyView(id, view);

        public GeoTaxonomyViewMessage? AddCategoryAsync(string sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
            => _inner.AddCategoryAsync(sportId, node, cancellationToken);

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            return _inner.ClearAsync(cancellationToken);
        }

        public GeoTaxonomyViewMessage? TryGetView(SportId id)
            => _inner.TryGetView(id);

        public ValueTask<GeoTaxonomyViewMessage?> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
            => _inner.RemoveCategoryAsync(sportId, categoryId, cancellationToken);

        public GeoTaxonomyViewMessage? RemoveView(SportId id)
            => _inner.RemoveView(id);
    }

    private sealed class FakeCheckpointStorage : ICheckpointStorage
    {
        public ViewCheckpoint? LastSavedViewCheckpoint { get; private set; }
        public string CurrentStoreId { get; set; } = Guid.NewGuid().ToString("D");
        private readonly Dictionary<string, ViewCheckpoint> _viewCheckpoints = new(StringComparer.Ordinal);

        public void Seed(ViewCheckpoint checkpoint)
            => _viewCheckpoints[checkpoint.ViewName] = checkpoint;

        public Task SaveCheckpointAsync(string topicName, int partitionId, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public ValueTask<TopicCheckpoint?> GetLastCheckpoint(string topicName, int partitionId, CancellationToken cancellationToken)
            => ValueTask.FromResult<TopicCheckpoint?>(null);

        public Task SaveViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
        {
            var checkpoint = new ViewCheckpoint(viewName, CurrentStoreId, BuildCompleted: true, DateTimeOffset.UtcNow);
            LastSavedViewCheckpoint = checkpoint;
            _viewCheckpoints[viewName] = checkpoint;
            return Task.CompletedTask;
        }

        public ValueTask<ViewCheckpoint?> GetViewCheckpointAsync(string viewName, CancellationToken cancellationToken)
            => ValueTask.FromResult(_viewCheckpoints.TryGetValue(viewName, out var checkpoint) ? checkpoint : null);
    }

    private sealed class FakeUnitOfWorkFactory : IUnitOfWorkFactory
    {
        private readonly FakeCheckpointStorage _checkpointStorage;

        public FakeUnitOfWorkFactory(FakeCheckpointStorage checkpointStorage)
            => _checkpointStorage = checkpointStorage;

        public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
            => new FakeTableViewUnitOfWork<TMessage>(_checkpointStorage);

        public Task MoveDurableAsync(CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class FakeTableViewUnitOfWork<TMessage> : ITableViewUnitOfWork<TMessage>
    {
        public FakeTableViewUnitOfWork(FakeCheckpointStorage checkpointStorage)
            => CheckpointStorage = checkpointStorage;

        public IMessageStorage<string, TMessage> MessageStorage { get; } = new NoOpMessageStorage<TMessage>();
        public ICheckpointStorage CheckpointStorage { get; }
        public IRejectedStorage RejectedStorage { get; } = new NoOpRejectedStorage();

        public Task CommitAsync(CancellationToken ct)
            => Task.CompletedTask;

        public void Dispose()
        {
        }
    }

    private sealed class NoOpMessageStorage<TMessage> : IMessageStorage<string, TMessage>
    {
        public ValueTask DeleteAsync(string id, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask ClearAsync(CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask<TMessage?> TryLoadAsync(string id, CancellationToken cancellationToken)
            => ValueTask.FromResult<TMessage?>(default);

        public ValueTask UpsertAsync(TMessage message, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public Dictionary<string, TMessage> GetAll(IValuePredicate<TMessage>? valuePredicate = null)
            => [];
    }

    private sealed class NoOpRejectedStorage : IRejectedStorage
    {
        public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}
