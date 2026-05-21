using System.Text.Json;
using System.Reactive.Linq;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Abstractions;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class GeoTaxonomyProcessor
{
    private readonly IPulsarTableView<SportMessage> _sportsTableView;
    private readonly IPulsarTableView<RawCategoryMessage> _categoriesTableView;
    private readonly ILogger<GeoTaxonomyProcessor> _logger;
    private readonly ITaxonomyViewPublisher? _publisher;

    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> _categoryToSportId = new(StringComparer.Ordinal);

    public GeoTaxonomyProcessor(IPulsarTableView<SportMessage> sportsView,
                                IPulsarTableView<RawCategoryMessage> categoriesView,
                                ILogger<GeoTaxonomyProcessor> logger,
                                ITaxonomyViewPublisher? publisher = null)
    {
        _sportsTableView = sportsView;
        _categoriesTableView = categoriesView;
        _logger = logger;
        _publisher = publisher;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Bootstrapping sports and categories table views.");
        // the bootstrap methods does not listing any events. We load all the topic compacted messages up to the latest without processing any update until both views are bootstrapped,
        // then we load the category index and build the initial snapshot from the source of truth (the table views) to ensure we don't have
        await Task.WhenAll(_sportsTableView.StartBootstrapAsync(cancellationToken), _categoriesTableView.StartBootstrapAsync(cancellationToken));

        await LoadCategoryIndexAsync(cancellationToken);
        var snapshot = await BuildSnapshotAsync(cancellationToken);
        _logger.LogInformation("Initial taxonomy snapshot:{NewLine}{Snapshot}", Environment.NewLine, ToJson(snapshot));

        using var sportsSubscription = _sportsTableView.OnUpdate.Subscribe(OnSportUpdate);
        using var categoriesSubscription = _categoriesTableView.OnUpdate.Subscribe(OnCategoryUpdate);

        _logger.LogInformation("Starting live tail for both table views.");

        try
        {
            await Task.WhenAll(_sportsTableView.StartLiveTailAsync(cancellationToken), _categoriesTableView.StartLiveTailAsync(cancellationToken));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("Processor shutdown requested.");
        }
    }

    private async Task LoadCategoryIndexAsync(CancellationToken cancellationToken)
    {
        _categoryToSportId.Clear();

        //using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        //await foreach (var category in _categoriesTableView.GetAllAsync(cancellationToken).WithCancellation(timeoutCts.Token))
        await foreach (var category in _categoriesTableView.GetAllAsync(cancellationToken).WithCancellation(cancellationToken))
        {
            _categoryToSportId[category.Id] = category.SportId;
        }
    }

    private async Task<IReadOnlyList<GeoTaxonomyMessage>> BuildSnapshotAsync(CancellationToken cancellationToken)
    {
        var categories = new List<RawCategoryMessage>();

        // 1. First we read all the categories
        await foreach (var category in _categoriesTableView.GetAllAsync(cancellationToken).WithCancellation(cancellationToken))
        {
            categories.Add(category);
        }

        // 2. We create the ultra-fast in-memory index
        var categoriesBySport = categories.ToLookup(category => category.SportId, StringComparer.Ordinal);

        var snapshot = new List<GeoTaxonomyMessage>();

        // 3. We read the sports and build the taxonomy at the same time
        await foreach (var sport in _sportsTableView.GetAllAsync(cancellationToken).WithCancellation(cancellationToken))
        {
            // categoriesBySport[sport.Id] devuelve vacío si no hay categorías, no tira excepción
            var taxonomy = BuildTaxonomy(sport, categoriesBySport[sport.Id]);
            snapshot.Add(taxonomy);
        }

        // 4. Ordenamos la vista final resultante y la devolvemos
        return [.. snapshot.OrderBy(taxonomy => taxonomy.SportId, StringComparer.Ordinal)];
    }

    private static GeoTaxonomyMessage BuildTaxonomy(SportMessage sport, IEnumerable<RawCategoryMessage> sportCategories)
        => new()
        {
            SportId = sport.Id,
            SportName = sport.Name,
            SportType = sport.SportType,
            GeoCategories = [.. sportCategories
                            .Where(category => !string.IsNullOrWhiteSpace(category.CountryCode))
                            .Select(category => category.CountryCode!)
                            .Distinct(StringComparer.Ordinal)
                            .OrderBy(countryCode => countryCode, StringComparer.Ordinal)
                            .Select(countryCode => new GeoTaxonomyNode{CountryCode = countryCode})]
        };

    private void OnSportUpdate(Event<SportMessage> @event)
    {
        switch (@event)
        {
            case UpdateEvent<SportMessage> update:
                _logger.LogInformation("Sports live update for key {Key}:{NewLine}{Payload}",
                                       update.Key,
                                       Environment.NewLine,
                                       ToJson(update.NewValue));
                // Disparamos la reconstrucción asíncrona y la olvidamos (fire and forget seguro)
                _ = PublishTaxonomyForSportAsync(update.Key, "sport update");
                break;

            case DeleteEvent<SportMessage> delete:
                _logger.LogInformation("Sports live delete for key {Key}.",
                                       delete.Key);
                _ = PublishTaxonomyForSportAsync(delete.Key, "sport delete");
                break;
        }
    }

    private void OnCategoryUpdate(Event<RawCategoryMessage> @event)
    {
        switch (@event)
        {
            case UpdateEvent<RawCategoryMessage> update:
                _categoryToSportId.TryGetValue(update.Key, out var previousSportId);
                _categoryToSportId[update.Key] = update.NewValue.SportId;

                _logger.LogInformation("Categories live update for key {Key}:{NewLine}{Payload}", update.Key, Environment.NewLine, ToJson(update.NewValue));

                if (!string.IsNullOrWhiteSpace(previousSportId) && !string.Equals(previousSportId, update.NewValue.SportId, StringComparison.Ordinal))
                {
                    _ = PublishTaxonomyForSportAsync(previousSportId, "category sport change");
                }

                _ = PublishTaxonomyForSportAsync(update.NewValue.SportId, "category update");
                break;

            case DeleteEvent<RawCategoryMessage> delete:
                _logger.LogInformation("Categories live delete for key {Key}.", delete.Key);

                if (_categoryToSportId.TryRemove(delete.Key, out var sportId))
                {
                    _ = PublishTaxonomyForSportAsync(sportId, "category delete");
                }
                else
                {
                    _logger.LogInformation("No sport mapping found for deleted category key {Key}.", delete.Key);
                }

                break;
        }
    }

    private async Task PublishTaxonomyForSportAsync(string sportId, string reason)
    {
        var sport = _sportsTableView.Get(sportId);

        if (sport is null)
        {
            _logger.LogInformation("Taxonomy projection removed for sport {SportId} after {Reason}. Publishing Tombstone.", sportId, reason);
            // Si el deporte ya no existe, enviamos un Tombstone para borrar la vista
            if (_publisher is not null)
            {
                await _publisher.DeleteAsync(sportId);
            }
            return;
        }

        var sportCategories = new List<RawCategoryMessage>();
        
        await foreach (var category in _categoriesTableView.GetAllAsync().ConfigureAwait(false))
        {
            if (string.Equals(category.SportId, sportId, StringComparison.Ordinal))
            {
                sportCategories.Add(category);
            }
        }

        var taxonomy = BuildTaxonomy(sport, sportCategories);
        
        _logger.LogInformation("Publishing recalculated taxonomy for sport {SportId} after {Reason}.", sportId, reason);
        
        // PUBLICAMOS EL RESULTADO A PULSAR
        if (_publisher is not null)
        {
            await _publisher.PublishAsync(taxonomy);
        }
    }

    private static string ToJson<T>(T value) => JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true });
}
