using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities.Categories;
using PoC.Pulsar.TableView.Domain.Entities.Sports;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Indexes;
using PoC.Pulsar.TableView.Domain.Storages.MaterializeViews;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Text.Json;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class GeoTaxonomyProcessor
{
    private readonly ICategoryBySportIndex _relationIndex;
    private readonly IPulsarTableView<RawCategoryMessage> _categoriesTableView;
    private readonly ILogger<GeoTaxonomyProcessor> _logger;
    private readonly IOrphanCategoryBySportIndex _pendingIndex;
    private readonly IPulsarTableView<SportMessage> _sportsTableView;
    private readonly ITaxonomyViewPublisher _taxonomyPublisher;
    private readonly IGeoTaxonomyViewStorage _materializeViewStorage;
    private readonly ConcurrentDictionary<SportId, GeoTaxonomyViewMessage> _lastView;
    public GeoTaxonomyProcessor(IPulsarTableView<SportMessage> sportsTableView,
                                IPulsarTableView<RawCategoryMessage> categoriesTableView,
                                ITaxonomyViewPublisher taxonomyPublisher,
                                ICategoryBySportIndex relationIndex,
                                IOrphanCategoryBySportIndex pendingIndex,
                                IGeoTaxonomyViewStorage materializeViewStorage,
                                ILogger<GeoTaxonomyProcessor> logger)
        => (_sportsTableView, _categoriesTableView, _taxonomyPublisher, _relationIndex, _pendingIndex, _materializeViewStorage, _logger)
            = (sportsTableView, categoriesTableView, taxonomyPublisher, relationIndex, pendingIndex, materializeViewStorage, logger);


    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Bootstrapping sports and categories table views.");
        // the bootstrap methods does not listing any events. We load all the topic compacted messages up to the latest without processing any update until both views are bootstrapped,
        // then we load the category index and build the initial snapshot from the source of truth (the table views) to ensure we don't have
        await _sportsTableView.StartBootstrapAsync(cancellationToken);
        await _categoriesTableView.StartBootstrapAsync(cancellationToken);
        await BuildSnapshotAsync(cancellationToken);

        using var sportsSubscription = _sportsTableView.OnUpdate
                                                        .Select(@event => Observable.FromAsync(ct => OnSportChangeAsync(@event, ct)))
                                                        .Concat()
                                                        .Subscribe(onNext: _ => { },
                                                                    exception => _logger.LogError(exception, "Error processing sport update."));


        using var categoriesSubscription = _categoriesTableView.OnUpdate
                                                        .Select(@event => Observable.FromAsync(ct => OnCategoryChangeAsync(@event, ct)))
                                                        .Concat()
                                                        // replace Concat with Merge() or Merge(maxConcurrency) if you want to process category updates in parallel
                                                        .Subscribe(onNext: _ => { },
                                                                    exception => _logger.LogError(exception, "Error processing category update."));

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



    private static string ToJson<T>(T value) => JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true });

    private async ValueTask BuildSnapshotAsync(CancellationToken cancellationToken)
    {
        // 1. Read all the sports
        var sports = _sportsTableView.GetLoadedOnBoostrap();

        // 2. Read all the categories where Country Code is not null to build the category-sport index and detect pending categories without sport
        IDictionary<string, RawCategoryMessage> countryCategories = _categoriesTableView.GetLoadedOnBoostrapFilterBy(new GeoCategoryMessageFilter());
        foreach (var category in countryCategories.Values)
        {
            var sportId = new SportId(category.SportId);
            var categoryId = new CategoryId(category.Id);
            await _relationIndex.AddCategorybySportAsync(sportId, categoryId, cancellationToken);
            if (!sports.ContainsKey(category.SportId))
                await _pendingIndex.AddOrphanCategorybySportAsync(sportId, categoryId, cancellationToken);
        }

        List<GeoTaxonomyViewMessage> createViewList = new (capacity: sports.Values.Count);
        foreach (var sport in sports.Values)
        {
            var sportId = new SportId(sport.Id);
            var categoriesIds = await _relationIndex.GetCategoriesBySport(sportId, cancellationToken);
            var sportCategories = await FilterCategoriesForGeoViewAsync(categoriesIds, countryCategories, cancellationToken);
            var view = GeoTaxonomyViewMessage.Create(sport, sportCategories);
            createViewList.Add(view);
            _materializeViewStorage.AddTaxonomyView(sportId, view);
        }

        await _taxonomyPublisher.PublishListMessage(createViewList, cancellationToken);
    }

    
    private async Task<List<GeoTaxonomyNode>> FilterCategoriesForGeoViewAsync(IReadOnlySet<CategoryId> categoriesIds, IDictionary<string,RawCategoryMessage> countryCategories, CancellationToken cancellationToken)
    {
        List<GeoTaxonomyNode> nodes = new(categoriesIds.Count);
        foreach (var categoryId in categoriesIds)
        {
            if (countryCategories.TryGetValue(categoryId.Value, out var countryCategory))
            {
                nodes.Add(new GeoTaxonomyNode(countryCategory.Id, countryCategory.CountryCode!));
            }
            else
            {
                var rawCategoryMessage = await _categoriesTableView.GetAsync(categoryId.Value, cancellationToken);
                if(rawCategoryMessage?.CountryCode != null)
                {
                    nodes.Add(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));
                }
            }

        }
        return [.. nodes.OrderBy(cat => cat.CountryCode)];
    }
    private async Task<List<GeoTaxonomyNode>> FilterCategoriesForGeoViewAsync(IReadOnlySet<CategoryId> categoriesIds, CancellationToken cancellationToken)
    {
        List<GeoTaxonomyNode> nodes = new(categoriesIds.Count);
        foreach (var categoryId in categoriesIds)
        {
            var rawCategoryMessage = await _categoriesTableView.GetAsync(categoryId.Value, cancellationToken);
            if(rawCategoryMessage?.CountryCode != null)
            {
                nodes.Add(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));
            }
        }

        return [.. nodes.OrderBy(cat => cat.CountryCode)];
    }
    private async Task<GeoTaxonomyViewMessage> GetViewFromSport(SportMessage sport, CancellationToken cancellationToken)
    {
        var sportId = new SportId(sport.Id);
        var pendingCategories = await _pendingIndex.GetOrphanCategoriesBySport(sportId, cancellationToken);
        var sportCategories = await FilterCategoriesForGeoViewAsync(pendingCategories, cancellationToken);
        return GeoTaxonomyViewMessage.Create(sport, sportCategories);
    }

    private async Task OnCategoryChangeAsync(Event<RawCategoryMessage> @event, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case EventCreated<RawCategoryMessage> created:
                await OnCategoryCreated(created.NewValue, cancellationToken);

                break;
            case EventUpdated<RawCategoryMessage> updated:
                await OnCategoryUpdated(updated.NewValue, updated.CurrentValue, cancellationToken);
                break;

            case EventDeleted<RawCategoryMessage> deleted:
                await OnCategoryDeleted(new SportId(deleted.CurrentValue.SportId), new CategoryId(deleted.CurrentValue.Id), cancellationToken);

                break;
        }
    }

    private async Task OnSportChangeAsync(Event<SportMessage> @event, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case EventCreated<SportMessage> created:
                await OnSportCreated(created.NewValue, cancellationToken);
                break;
            case EventUpdated<SportMessage> updated:
                _logger.LogInformation("Sports live update for key {Key}:{NewLine}{Payload}",
                                       updated.Key,
                                       Environment.NewLine,
                                       ToJson(updated.NewValue));
                await OnSportUpdated(updated.NewValue, updated.CurrentValue, cancellationToken);
                break;

            case EventDeleted<SportMessage> delete:
                _logger.LogInformation("Sports live delete for key {Key}.", delete.Key);
                await OnSportDeleted(delete.Key, cancellationToken);
                break;
        }
    }

    private async Task OnCategoryDeleted(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        await _relationIndex.RemoveCategorybySportAsync(sportId, categoryId, cancellationToken);
        await _pendingIndex.RemoveOrphanCategorybySportAsync(sportId, categoryId, cancellationToken);
        var viewUpdated = await _materializeViewStorage.RemoveCategoryAsync(sportId, categoryId, cancellationToken);

        if (viewUpdated is not null)
        {
            await _taxonomyPublisher.PublishAsync(viewUpdated!, cancellationToken);
        }
    }
    private async Task OnCategoryUpdated(RawCategoryMessage category, RawCategoryMessage oldCategory, CancellationToken cancellationToken)
    {
        // TODO: Use fractional
        if (category.SportId == oldCategory.SportId)
        {
            if (category.CountryCode == oldCategory.CountryCode)
            {
                return;
            }
            // cahange categoryType
            if(category.CountryCode is null)
            {
                await OnCategoryDeleted(new SportId(category.SportId), new CategoryId(category.Id), cancellationToken);
                return;
            }
            var view = _materializeViewStorage.AddCategoryAsync(category.SportId, new GeoTaxonomyNode(category.Id, category.CountryCode!), cancellationToken);

            if (view is not null)
            {
                await _taxonomyPublisher.PublishAsync(view!, cancellationToken);
            }
        }
        else
        {
            await OnCategoryDeleted(oldCategory, oldCategory.Id, cancellationToken);
            await OnCategoryCreated(category, cancellationToken);
        }
    }

    private async Task OnCategoryCreated(RawCategoryMessage rawCategoryMessage, CancellationToken cancellationToken)
    {
        if (rawCategoryMessage.CountryCode is null)
        {
            return;
        }

        await _relationIndex.AddAsync(rawCategoryMessage.SportId, rawCategoryMessage.Id, cancellationToken);
        var sport = await _sportsTableView.GetAsync(rawCategoryMessage.SportId);

        if (sport is null)
        {
            await _pendingIndex.AddAsync(rawCategoryMessage.SportId, rawCategoryMessage.Id, cancellationToken);
            return;
        }

        var viewUpdated = _materializeViewStorage.GetAndUpdate(sport.Id,
                                            (currentView) =>
                                            {
                                                currentView.AddOrUpdateCategory(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode));
                                                return currentView;
                                            });

        if (viewUpdated is not null)
        {
            await _taxonomyPublisher.PublishAsync(viewUpdated!, cancellationToken);
        }
    }


    private async Task OnSportCreated(SportMessage sport, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Sport created for key {Key}:{NewLine}{Payload}",
                               sport.Id,
                               Environment.NewLine,
                               ToJson(sport));
        GeoTaxonomyViewMessage newView = await GetViewFromSport(sport, cancellationToken);
        _materializeViewStorage.AddView(sport.Id, newView);
        await _taxonomyPublisher.PublishAsync(newView, cancellationToken);
        await _pendingIndex.ClearAsync(sport.Id, cancellationToken);
    }
    private async Task OnSportUpdated(SportMessage sport, SportMessage oldSport, CancellationToken cancellationToken)
    {
        // 1. Fractional
        if ((sport.Name, sport.SportType) == (oldSport.Name, oldSport.SportType))
        {
            return;
        }

        var viewUpdated = _materializeViewStorage.TryUpdate(sport.Id
            , (sportId) => GetViewFromSport(sport, cancellationToken).Result
            , (sportId, currentView) => GeoTaxonomyViewMessage.Create(sport, currentView.GeoCategories));

        await _taxonomyPublisher.PublishAsync(viewUpdated, cancellationToken);
    }

    private async Task OnSportDeleted(string sportId, CancellationToken cancellationToken)
    {
        await _relationIndex.ClearAsync(sportId, cancellationToken);
        await _pendingIndex.ClearAsync(sportId, cancellationToken);

        var viewRemoved = _materializeViewStorage.RemoveView(sportId);

        //var categoryIds = viewRemoved?.GeoCategories?.Select(c => c.CategoryId);

    }
}
