using System.Collections.Immutable;
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
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using Xunit;
using PoC.Pulsar.TableView.Domain.Storages.Messages;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class GeoTaxonomyProcessorTests
{
    [Fact]
    public async Task run_async_should_rebuild_projector_state_when_view_checkpoint_is_missing()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        dependencies.ViewStorage.SeedPublishedView(new SportId("stale-sport"), GeoTaxonomyViewMessage.Create(Sport("stale-sport", "Stale", "STALE"), []));
        await dependencies.RelationIndex.IndexCategoryAsync(new CategoryRelations(new CategoryId("stale-category"), new SportId("stale-sport"), null), CancellationToken.None);
        await dependencies.PendingIndex.TryMarkCategoryWaitingForSportAsync(new SportId("stale-sport"),
                                                                            new CategoryId("stale-category"),
                                                                            static (_, _) => ValueTask.FromResult(false),
                                                                            CancellationToken.None);

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
        Assert.Single(publisher.PublishedTaxonomies);
        Assert.Equal(1, dependencies.UnitOfWorkFactory.BuildUnitOfWorkCommitCount);
    }

    [Fact]
    public async Task run_async_should_apply_bootstrap_deltas_when_view_checkpoint_is_trusted()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        dependencies.CheckpointStorage.Seed(new ViewCheckpoint("category-taxonomy", metadata.StoreGenerationId.ToString("D"), BuildCompleted: true, DateTimeOffset.UtcNow));
        dependencies.ViewStorage.SeedPublishedView(new SportId("sport-1"), GeoTaxonomyViewMessage.Create(Sport("sport-1", "Soccer", "SOCCER"), []));

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
        Assert.Single(publisher.PublishedTaxonomies);
    }

    [Fact]
    public async Task run_async_should_publish_versioned_result_view_and_mark_published_after_publish_during_rebuild()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")], new TopicRecoveredFromStateStore<SportMessage>([]));
        var categories = new FakePulsarTableView<RawCategoryMessage>([Category("category-es", "sport-1", "ES")], new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);

        var published = Assert.Single(publisher.PublishedTaxonomies);
        Assert.Equal(1, published.Version);
        Assert.Contains(dependencies.ViewStorage.OperationLog, entry => entry == "publish:sport-1:1");
        Assert.Contains(dependencies.ViewStorage.OperationLog, entry => entry == "mark-published:sport-1:1");
        Assert.True(dependencies.ViewStorage.OperationLog.IndexOf("publish:sport-1:1") < dependencies.ViewStorage.OperationLog.IndexOf("mark-published:sport-1:1"));
    }

    [Fact]
    public async Task run_async_should_rebuild_with_category_ids_that_require_storage_key_sanitization()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport:live", "Soccer", "SOCCER")], new TopicRecoveredFromStateStore<SportMessage>([]));
        var categories = new FakePulsarTableView<RawCategoryMessage>([Category("soccer:int", "sport:live", "ES")], new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);

        var published = Assert.Single(publisher.PublishedTaxonomies);
        Assert.Equal("sport:live", published.SportId);
        var category = Assert.Single(published.GeoCategories);
        Assert.Equal("soccer:int", category.CategoryId);
        Assert.Equal("ES", category.CountryCode);
    }

    [Fact]
    public async Task run_async_should_generate_single_build_generation_id_per_rebuild()
    {
        var metadata = new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow);
        var dependencies = CreateDependencies(metadata);
        var sports = new FakePulsarTableView<SportMessage>(
        [
            Sport("sport-1", "Soccer", "SOCCER"),
            Sport("sport-2", "Basketball", "GENERIC")
        ],
        new TopicRecoveredFromStateStore<SportMessage>([]));
        var categories = new FakePulsarTableView<RawCategoryMessage>([], new TopicRecoveredFromStateStore<RawCategoryMessage>([]));
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);

        Assert.Equal(2, dependencies.ViewStorage.UpsertCalls.Count);
        Assert.Single(dependencies.ViewStorage.UpsertCalls.Select(call => call.BuildGenerationId).Distinct());
        Assert.All(dependencies.ViewStorage.UpsertCalls, call => Assert.Matches("^build-[0-9a-f]{32}$", call.BuildGenerationId));
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
        var taxonomy = await publisher.WaitForPublishedCountAsync(2);
        var updated = taxonomy[^1];

        Assert.Equal("sport-1", updated.SportId);
        Assert.Equal("Soccer Updated", updated.SportName);
        Assert.Equal("SOCCER", updated.SportType);
        Assert.Equal(["ES"], updated.GeoCategories.Select(category => category.CountryCode));
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
    public async Task category_created_should_publish_taxonomy_when_sport_exists()
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var category = Category("category-es", "sport-1", "ES");
        categories.Upsert(category.Id, category);
        categories.EmitCreate(category.Id, category);
        var taxonomy = await publisher.WaitForPublishedCountAsync(2);
        var updated = taxonomy[^1];

        Assert.Equal("sport-1", updated.SportId);
        Assert.Equal("Soccer", updated.SportName);
        Assert.Equal("SOCCER", updated.SportType);
        Assert.Equal(["ES"], updated.GeoCategories.Select(categoryNode => categoryNode.CountryCode));
    }

    [Fact]
    public async Task category_created_before_sport_should_wait_pending_and_resolve_when_sport_arrives()
    {
        var dependencies = CreateDependencies(new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow));
        var sports = new FakePulsarTableView<SportMessage>();
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var category = Category("category-es", "sport-1", "ES");
        categories.Upsert(category.Id, category);
        categories.EmitCreate(category.Id, category);

        Assert.Equal(["category-es"],
                     (await dependencies.PendingIndex.GetCategoriesWaitingForSportAsync(new SportId("sport-1"), CancellationToken.None))
                     .Select(categoryId => categoryId.Value));
        Assert.Empty(publisher.PublishedTaxonomies);

        var sport = Sport("sport-1", "Soccer", "SOCCER");
        sports.Upsert(sport.Id, sport);
        sports.EmitCreate(sport.Id, sport);
        var taxonomy = Assert.Single(await publisher.WaitForPublishedCountAsync(1));

        Assert.Equal("sport-1", taxonomy.SportId);
        Assert.Equal(["ES"], taxonomy.GeoCategories.Select(category => category.CountryCode));
        Assert.Empty(await dependencies.PendingIndex.GetCategoriesWaitingForSportAsync(new SportId("sport-1"), CancellationToken.None));
        Assert.Empty(await dependencies.PendingIndex.GetMissingSportsForCategoryAsync(new CategoryId("category-es"), CancellationToken.None));
    }

    [Fact]
    public async Task category_created_without_geo_country_code_should_not_wait_pending_or_publish()
    {
        var dependencies = CreateDependencies(new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow));
        var sports = new FakePulsarTableView<SportMessage>();
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var nullCountryCodeCategory = Category("category-null", "sport-1", null);
        var emptyCountryCodeCategory = Category("category-empty", "sport-1", "");
        var whitespaceCountryCodeCategory = Category("category-whitespace", "sport-1", "   ");
        categories.Upsert(nullCountryCodeCategory.Id, nullCountryCodeCategory);
        categories.Upsert(emptyCountryCodeCategory.Id, emptyCountryCodeCategory);
        categories.Upsert(whitespaceCountryCodeCategory.Id, whitespaceCountryCodeCategory);

        categories.EmitCreate(nullCountryCodeCategory.Id, nullCountryCodeCategory);
        categories.EmitCreate(emptyCountryCodeCategory.Id, emptyCountryCodeCategory);
        categories.EmitCreate(whitespaceCountryCodeCategory.Id, whitespaceCountryCodeCategory);
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Empty(await dependencies.PendingIndex.GetCategoriesWaitingForSportAsync(new SportId("sport-1"), CancellationToken.None));
        Assert.Empty(publisher.PublishedTaxonomies);
    }

    [Fact]
    public async Task category_update_should_wait_pending_when_geo_category_becomes_eligible_before_sport_arrives()
    {
        var dependencies = CreateDependencies(new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow));
        var sports = new FakePulsarTableView<SportMessage>();
        var oldCategory = Category("category-es", "sport-1", null);
        var categories = new FakePulsarTableView<RawCategoryMessage>([oldCategory]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher, dependencies);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var category = Category("category-es", "sport-1", "ES");
        categories.Upsert(category.Id, category);
        categories.EmitUpdate(category.Id, category, oldCategory);

        Assert.Equal(["category-es"],
                     (await dependencies.PendingIndex.GetCategoriesWaitingForSportAsync(new SportId("sport-1"), CancellationToken.None))
                     .Select(categoryId => categoryId.Value));
        Assert.Empty(publisher.PublishedTaxonomies);

        var sport = Sport("sport-1", "Soccer", "SOCCER");
        sports.Upsert(sport.Id, sport);
        sports.EmitCreate(sport.Id, sport);
        var taxonomy = Assert.Single(await publisher.WaitForPublishedCountAsync(1));

        Assert.Equal("sport-1", taxonomy.SportId);
        Assert.Equal(["ES"], taxonomy.GeoCategories.Select(categoryNode => categoryNode.CountryCode));
        Assert.Empty(await dependencies.PendingIndex.GetCategoriesWaitingForSportAsync(new SportId("sport-1"), CancellationToken.None));
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
        var taxonomy = await publisher.WaitForPublishedCountAsync(2);
        var updated = taxonomy[^1];

        Assert.Equal("sport-1", updated.SportId);
        Assert.Equal(["ES"], updated.GeoCategories.Select(categoryNode => categoryNode.CountryCode));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task category_update_should_remove_geo_category_when_country_code_becomes_missing(string? countryCode)
    {
        var sports = new FakePulsarTableView<SportMessage>([Sport("sport-1", "Soccer", "SOCCER")]);
        var existingCategory = Category("category-es", "sport-1", "ES");
        var categories = new FakePulsarTableView<RawCategoryMessage>([existingCategory]);
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        var category = Category("category-es", "sport-1", countryCode);
        categories.Upsert(category.Id, category);
        categories.EmitUpdate(category.Id, category, existingCategory);
        var taxonomies = await publisher.WaitForPublishedCountAsync(2);
        var updated = taxonomies[^1];

        Assert.Equal("sport-1", updated.SportId);
        Assert.Empty(updated.GeoCategories);
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
        var taxonomies = await publisher.WaitForPublishedCountAsync(4);
        var movedTaxonomies = taxonomies.Skip(taxonomies.Count - 2).ToArray();

        Assert.Equal(["sport-1", "sport-2"], movedTaxonomies.Select(taxonomy => taxonomy.SportId));
        Assert.Empty(movedTaxonomies[0].GeoCategories);
        Assert.Equal(["ES"], movedTaxonomies[1].GeoCategories.Select(category => category.CountryCode));
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
        var taxonomies = await publisher.WaitForPublishedCountAsync(2);
        var updated = taxonomies[^1];

        Assert.Equal("sport-1", updated.SportId);
        Assert.Empty(updated.GeoCategories);
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

        Assert.Single(publisher.PublishedTaxonomies);
        Assert.Empty(publisher.DeletedSportIds);
    }

    private static GeoTaxonomyProcessor CreateProcessor(
        IPulsarTableView<SportMessage> sports,
        IPulsarTableView<RawCategoryMessage> categories,
        ITaxonomyViewPublisher publisher,
        ProcessorDependencies? dependencies = null)
    {
        dependencies ??= CreateDependencies(new StoreMetadata(Guid.NewGuid(), SchemaVersion: 1, IsBoostrapCompleted: true, CreatedAt: DateTimeOffset.UtcNow));

        if (publisher is FakeTaxonomyViewPublisher fakePublisher)
        {
            fakePublisher.OperationLog = dependencies.OperationLog;
        }

        return new GeoTaxonomyProcessor(
            sports,
            categories,
            publisher,
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
        var operationLog = new List<string>();
        var relationIndex = new TrackingCategoryBySportIndex();
        var pendingIndex = new TrackingOrphanCategoryBySportIndex();
        var viewStorage = new TrackingGeoTaxonomyViewStorage(operationLog);
        var unitOfWorkFactory = new FakeUnitOfWorkFactory(checkpointStorage, relationIndex, pendingIndex, viewStorage);

        return new ProcessorDependencies(storeMetadata,
                                         checkpointStorage,
                                         relationIndex,
                                         pendingIndex,
                                         viewStorage,
                                         operationLog,
                                         unitOfWorkFactory);
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

        public void EmitCreate(string key, T value)
        {
            _updates.OnNext(new TableEntryCreated<T>(key, value));
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
        public List<string>? OperationLog { get; set; }

        public ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                PublishedTaxonomies.Add(taxonomy);
                OperationLog?.Add($"publish:{taxonomy.SportId}:{taxonomy.Version}");
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
                                                List<string> OperationLog,
                                                FakeUnitOfWorkFactory UnitOfWorkFactory);

    private sealed class TrackingCategoryBySportIndex : ICategoryRelationIndex
    {
        private readonly Dictionary<SportId, HashSet<CategoryId>> _bySport = new();
        private readonly Dictionary<CategoryId, HashSet<CategoryId>> _byParent = new();

        public int ClearCallCount { get; private set; }

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

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            cancellationToken.ThrowIfCancellationRequested();
            _bySport.Clear();
            _byParent.Clear();
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult<IReadOnlySet<CategoryId>>(_bySport.TryGetValue(sportId, out var set) ? set.ToHashSet() : []);
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

    private sealed class TrackingOrphanCategoryBySportIndex : ICategoryPendingIndex
    {
        private readonly Dictionary<SportId, HashSet<CategoryId>> _waitingForSport = new();
        private readonly Dictionary<CategoryId, HashSet<SportId>> _missingSports = new();

        public int ClearCallCount { get; private set; }

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

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            cancellationToken.ThrowIfCancellationRequested();
            _waitingForSport.Clear();
            _missingSports.Clear();
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

    private sealed class TrackingGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
    {
        private readonly Dictionary<SportId, GeoTaxonomyViewMessage> _views = new();
        private readonly Dictionary<SportId, GeoTaxonomyViewMetadata> _metadata = new();
        private readonly List<string> _operationLog;

        public TrackingGeoTaxonomyViewStorage(List<string> operationLog)
            => _operationLog = operationLog;

        public int ClearCallCount { get; private set; }
        public List<string> OperationLog => _operationLog;
        public List<GeoTaxonomyViewUpsertResult> UpsertCalls { get; } = [];

        public void SeedPublishedView(SportId id, GeoTaxonomyViewMessage view)
        {
            var result = UpsertViewAsync(id, view, "build-seed", CancellationToken.None).GetAwaiter().GetResult();
            MarkViewPublishedAsync(id, result.CalculatedVersion, result.BuildGenerationId, CancellationToken.None).GetAwaiter().GetResult();
        }

        public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var current = _views.TryGetValue(sportId, out var existing) ? existing : GeoTaxonomyViewMessage.CreateNew(sportId.Value, sportName, sportType);
            if (current.SportName == sportName && current.SportType == sportType)
            {
                return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(current));
            }

            var updated = current with { SportName = sportName, SportType = sportType };
            var result = UpsertViewAsync(sportId, updated, GetBuildGenerationId(sportId), cancellationToken).GetAwaiter().GetResult();
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(result.View));
        }

        public async ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var existingMetadata = _metadata.TryGetValue(sportId, out var metadata) ? metadata : null;
            long previousCalculatedVersion = existingMetadata?.CalculatedVersion ?? 0;
            long previousPublishedVersion = existingMetadata?.PublishedVersion ?? 0;
            DateTimeOffset? previousPublishedAtUtc = existingMetadata?.PublishedAtUtc;
            long nextVersion = Math.Max(previousCalculatedVersion, previousPublishedVersion) + 1;
            var versionedView = view with { Version = checked((int)nextVersion) };
            var updatedMetadata = new GeoTaxonomyViewMetadata
            {
                CalculatedVersion = nextVersion,
                PublishedVersion = previousPublishedVersion,
                BuildGenerationId = buildGenerationId,
                UpdatedAtUtc = DateTimeOffset.UtcNow,
                PublishedAtUtc = previousPublishedAtUtc
            };

            _views[sportId] = versionedView;
            _metadata[sportId] = updatedMetadata;

            var result = new GeoTaxonomyViewUpsertResult
            {
                SportId = sportId,
                CalculatedVersion = nextVersion,
                PublishedVersion = previousPublishedVersion,
                BuildGenerationId = buildGenerationId,
                View = versionedView
            };

            UpsertCalls.Add(result);
            _operationLog.Add($"upsert:{sportId.Value}:{result.CalculatedVersion}:{buildGenerationId}");
            return result;
        }

        public async ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken)
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

            _operationLog.Add($"mark-published:{sportId.Value}:{calculatedVersion}");
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

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            ClearCallCount++;
            cancellationToken.ThrowIfCancellationRequested();
            _views.Clear();
            _metadata.Clear();
            return ValueTask.CompletedTask;
        }

        public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_views.TryGetValue(sportId, out var view) ? view : null);
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

        public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _metadata.Remove(sportId);
            return ValueTask.FromResult(_views.Remove(sportId, out var view) ? view : null);
        }

        public ValueTask<GeoTaxonomyViewMetadata?> GetMetadataAsync(SportId sportId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_metadata.TryGetValue(sportId, out var metadata) ? metadata : null);
        }

        private string GetBuildGenerationId(SportId sportId)
            => _metadata.TryGetValue(sportId, out var metadata)
                ? metadata.BuildGenerationId
                : $"build-{Guid.CreateVersion7():N}";
    }

    private sealed class FakeCheckpointStorage : ICheckpointStorage
    {
        public ViewCheckpoint? LastSavedViewCheckpoint { get; private set; }
        public string CurrentStoreId { get; set; } = Guid.NewGuid().ToString("D");
        private readonly Dictionary<string, ViewCheckpoint> _viewCheckpoints = new(StringComparer.Ordinal);

        public void Seed(ViewCheckpoint checkpoint)
            => _viewCheckpoints[checkpoint.ViewName] = checkpoint;

        public Task SaveCheckpointAsync(TopicShard shard, PulsarMessageId lastProcessedMessageId, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public ValueTask<TopicCheckpoint?> GetLastCheckpoint(TopicShard shard, CancellationToken cancellationToken)
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
        private readonly TrackingCategoryBySportIndex _relationIndex;
        private readonly TrackingOrphanCategoryBySportIndex _pendingIndex;
        private readonly TrackingGeoTaxonomyViewStorage _viewStorage;

        public FakeUnitOfWorkFactory(FakeCheckpointStorage checkpointStorage,
                                     TrackingCategoryBySportIndex relationIndex,
                                     TrackingOrphanCategoryBySportIndex pendingIndex,
                                     TrackingGeoTaxonomyViewStorage viewStorage)
            => (_checkpointStorage, _relationIndex, _pendingIndex, _viewStorage)
                = (checkpointStorage, relationIndex, pendingIndex, viewStorage);

        public int BuildUnitOfWorkCommitCount { get; private set; }

        public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
            => new FakeTableViewUnitOfWork<TMessage>(_checkpointStorage);

        public IGeoTaxonomyBuildUnitOfWork CreateGeoTaxonomyBuild()
            => new FakeGeoTaxonomyBuildUnitOfWork(_relationIndex,
                                                  _pendingIndex,
                                                  _viewStorage,
                                                  _checkpointStorage,
                                                  () => BuildUnitOfWorkCommitCount++);

        public Task MoveDurableAsync(CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class FakeGeoTaxonomyBuildUnitOfWork : IGeoTaxonomyBuildUnitOfWork
    {
        private readonly Action _onCommit;

        public FakeGeoTaxonomyBuildUnitOfWork(ICategoryRelationIndex relationIndex,
                                              ICategoryPendingIndex pendingIndex,
                                              IGeoTaxonomyViewStorage materializeViewStorage,
                                              ICheckpointStorage checkpointStorage,
                                              Action onCommit)
            => (CategoryRelationIndex, CategoryPendingIndex, MaterializeViewStorage, CheckpointStorage, _onCommit)
                = (relationIndex, pendingIndex, materializeViewStorage, checkpointStorage, onCommit);

        public ICategoryRelationIndex CategoryRelationIndex { get; }
        public ICategoryPendingIndex CategoryPendingIndex { get; }
        public IGeoTaxonomyViewStorage MaterializeViewStorage { get; }
        public ICheckpointStorage CheckpointStorage { get; }

        public Task CommitAsync(CancellationToken ct)
        {
            _onCommit();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
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

        public ValueTask<TableMessageApplyDecision> TryApplyAsync(TMessage message, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Dictionary<string, TMessage> GetAll(IValuePredicate<TMessage>? valuePredicate = null)
            => [];
    }

    private sealed class NoOpRejectedStorage : IRejectedStorage
    {
        public ValueTask SaveRejectedRecordAsync(RejectedProjection rejectedProjection, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}
