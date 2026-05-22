using System.Reactive.Subjects;
using Microsoft.Extensions.Logging.Abstractions;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;
using Xunit;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class GeoTaxonomyProcessorTests
{
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
        sports.EmitUpdate("sport-1", Sport("sport-1", "Soccer", "SOCCER"));
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal("sport-1", taxonomy[0].SportId);
        Assert.Equal("Soccer", taxonomy[0].SportName);
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
        sports.EmitUpdate("sport-1", Sport("sport-1", "Soccer", "SOCCER"));
        var taxonomy = await publisher.WaitForPublishedCountAsync(1);

        Assert.Equal(["ES", "US"], taxonomy[0].GeoCategories.Select(category => category.CountryCode));
    }

    [Fact]
    public async Task sport_delete_should_publish_tombstone_when_sport_is_missing()
    {
        var sports = new FakePulsarTableView<SportMessage>();
        var categories = new FakePulsarTableView<RawCategoryMessage>();
        var publisher = new FakeTaxonomyViewPublisher();
        var processor = CreateProcessor(sports, categories, publisher);

        await using var runner = await ProcessorRunner.StartAsync(processor, sports, categories);
        sports.EmitDelete("sport-1");
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
        var category = Category("category-es", "sport-1", "ES");
        categories.Upsert(category.Id, category);
        categories.EmitUpdate(category.Id, category);
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
        categories.EmitUpdate(movedCategory.Id, movedCategory);
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
        categories.EmitDelete(existingCategory.Id);
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
        categories.EmitDelete("missing-category");
        await Task.Delay(50);

        Assert.Empty(publisher.PublishedTaxonomies);
        Assert.Empty(publisher.DeletedSportIds);
    }

    private static GeoTaxonomyProcessor CreateProcessor(
        IPulsarTableView<SportMessage> sports,
        IPulsarTableView<RawCategoryMessage> categories,
        ITaxonomyViewPublisher publisher)
    {
        return new GeoTaxonomyProcessor(
            sports,
            categories,
            publisher,
            NullLogger<GeoTaxonomyProcessor>.Instance);
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
        private readonly Subject<Event<T>> _updates = new();
        private readonly TaskCompletionSource _liveTailStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public FakePulsarTableView(IReadOnlyList<T>? items = null)
        {
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

        public IObservable<Event<T>> OnUpdate => _updates;

        public bool BootstrapCompleted { get; private set; }

        public bool BootstrapCompletedBeforeLiveTail { get; private set; }

        public T? Get(string key)
        {
            return _items.GetValueOrDefault(key);
        }

        public async IAsyncEnumerable<T> GetAllAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var item in _items.Values.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
                await Task.Yield();
            }
        }

        public Task StartBootstrapAsync(CancellationToken cancellationToken = default)
        {
            BootstrapCompleted = true;
            return Task.CompletedTask;
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

        public void EmitUpdate(string key, T value)
        {
            _updates.OnNext(new UpdateEvent<T>(key, value));
        }

        public void EmitDelete(string key)
        {
            _updates.OnNext(new DeleteEvent<T>(key));
        }
    }

    private sealed class FakeTaxonomyViewPublisher : ITaxonomyViewPublisher
    {
        private readonly object _gate = new();
        private TaskCompletionSource _changed = NewCompletionSource();

        public List<GeoTaxonomyMessage> PublishedTaxonomies { get; } = [];

        public List<string> DeletedSportIds { get; } = [];

        public ValueTask PublishAsync(GeoTaxonomyMessage taxonomy, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                PublishedTaxonomies.Add(taxonomy);
                _changed.TrySetResult();
                _changed = NewCompletionSource();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask PublishDeleteMessageAsync(string sportId, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                DeletedSportIds.Add(sportId);
                _changed.TrySetResult();
                _changed = NewCompletionSource();
            }

            return ValueTask.CompletedTask;
        }

        public async Task<IReadOnlyList<GeoTaxonomyMessage>> WaitForPublishedCountAsync(int expectedCount)
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
}
