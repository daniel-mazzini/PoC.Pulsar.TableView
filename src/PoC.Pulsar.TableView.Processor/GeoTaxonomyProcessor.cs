using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Filter;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using System.Linq;
using System.Reactive.Linq;
using System.Text.Json;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class GeoTaxonomyProcessor
{
    private const string ViewName = "category-taxonomy";

    private readonly ICategoryBySportIndex _relationIndex;
    private readonly IPulsarTableView<RawCategoryMessage> _categoriesTableView;
    private readonly ILogger<GeoTaxonomyProcessor> _logger;
    private readonly StoreMetadata _storeMetadata;
    private readonly IOrphanCategoryBySportIndex _pendingIndex;
    private readonly IPulsarTableView<SportMessage> _sportsTableView;
    private readonly ITaxonomyViewPublisher _taxonomyPublisher;
    private readonly IGeoTaxonomyViewStorage _materializeViewStorage;
    private readonly IUnitOfWorkFactory _unitOfWorkFactory;

    public GeoTaxonomyProcessor(IPulsarTableView<SportMessage> sportsTableView,
                                IPulsarTableView<RawCategoryMessage> categoriesTableView,
                                ITaxonomyViewPublisher taxonomyPublisher,
                                ICategoryBySportIndex relationIndex,
                                IOrphanCategoryBySportIndex pendingIndex,
                                IGeoTaxonomyViewStorage materializeViewStorage,
                                IUnitOfWorkFactory unitOfWorkFactory,
                                StoreMetadata storeMetadata,
                                ILogger<GeoTaxonomyProcessor> logger)
        => (_sportsTableView, _categoriesTableView, _taxonomyPublisher, _relationIndex, _pendingIndex, _materializeViewStorage, _unitOfWorkFactory, _storeMetadata, _logger)
            = (sportsTableView, categoriesTableView, taxonomyPublisher, relationIndex, pendingIndex, materializeViewStorage, unitOfWorkFactory, storeMetadata, logger);


    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Bootstrapping sports and categories table views.");
        var sportsBootstrapTask = _sportsTableView.StartBootstrapAsync(cancellationToken);
        var categoriesBootstrapTask = _categoriesTableView.StartBootstrapAsync(cancellationToken);
        var viewCheckpointTask = GetViewCheckpointAsync(cancellationToken);

        await Task.WhenAll(sportsBootstrapTask, categoriesBootstrapTask, viewCheckpointTask);

        var sportsBootstrap = sportsBootstrapTask.Result;
        var categoriesBootstrap = categoriesBootstrapTask.Result;
        ViewCheckpoint? viewCheckpoint = viewCheckpointTask.Result;


        bool requiresRebuild = RequiresRebuild(viewCheckpoint, sportsBootstrap, categoriesBootstrap);
        if (requiresRebuild)
        {
            await ClearProjectorStateAsync(cancellationToken);

            var sports = _sportsTableView.GetSnapshot();
            var categories = _categoriesTableView.GetSnapshot(new GeoCategoryMessageFilter());

            await BuildAsync(sports, categories, cancellationToken);
            await SaveViewCheckpointAsync(cancellationToken);
        }
        else
        {
            await ApplyBootstrapDeltasAsync(sportsBootstrap, categoriesBootstrap, cancellationToken);
            await SaveViewCheckpointAsync(cancellationToken);
        }

        using var sportsSubscription = _sportsTableView.OnChanges
                                                        .Select(@event => Observable.FromAsync(ct => OnSportChangeAsync(@event, ct)))
                                                        .Concat()
                                                        .Subscribe(onNext: _ => { },
                                                                    exception => _logger.LogError(exception, "Error processing sport update."));


        using var categoriesSubscription = _categoriesTableView.OnChanges
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

    private async Task BuildAsync(IDictionary<string, SportMessage> sports, IDictionary<string, RawCategoryMessage> categories, CancellationToken cancellationToken)
    {
        foreach (var category in categories.Values)
        {
            var sportId = new SportId(category.SportId);
            var categoryId = new CategoryId(category.Id);
            await _relationIndex.AddCategorybySportAsync(sportId, categoryId, cancellationToken);
            if (!sports.ContainsKey(category.SportId))
            {
                await _pendingIndex.AddOrphanCategorybySportAsync(sportId, categoryId, cancellationToken);
            }
        }

        List<GeoTaxonomyViewMessage> createdViews = new(capacity: sports.Values.Count);
        foreach (var sport in sports.Values)
        {
            var sportId = new SportId(sport.Id);
            var categoryIds = await _relationIndex.GetCategoriesBySport(sportId, cancellationToken);
            var sportCategories = await FilterCategoriesForGeoViewAsync(categoryIds, categories, cancellationToken);
            var view = GeoTaxonomyViewMessage.Create(sport, sportCategories);
            createdViews.Add(view);
            _materializeViewStorage.AddTaxonomyView(sportId, view);
        }

        await _taxonomyPublisher.PublishListMessage(createdViews, cancellationToken);
    }

    private static string ToJson<T>(T value) => JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true });

    private async Task ApplyBootstrapDeltasAsync(TopicBootstrapResult<SportMessage> sportsBootstrap,
                                                 TopicBootstrapResult<RawCategoryMessage> categoriesBootstrap,
                                                 CancellationToken cancellationToken)
    {
        foreach (var sportChange in GetDeltaChanges(sportsBootstrap))
        {
            await OnSportChangeAsync(sportChange, cancellationToken);
        }

        foreach (var categoryChange in GetDeltaChanges(categoriesBootstrap))
        {
            await OnCategoryChangeAsync(categoryChange, cancellationToken);
        }
    }

    private async Task ClearProjectorStateAsync(CancellationToken cancellationToken)
    {
        await _materializeViewStorage.ClearAsync(cancellationToken);
        await _relationIndex.ClearAsync(cancellationToken);
        await _pendingIndex.ClearAsync(cancellationToken);
    }

    private async Task SaveViewCheckpointAsync(CancellationToken cancellationToken)
    {
        using var unitOfWork = _unitOfWorkFactory.CreateBootstrap<SportMessage>();
        await unitOfWork.CheckpointStorage.SaveViewCheckpointAsync(ViewName, cancellationToken);
    }

    private async Task<ViewCheckpoint?> GetViewCheckpointAsync(CancellationToken cancellationToken)
    {
        using var unitOfWork = _unitOfWorkFactory.CreateBootstrap<SportMessage>();
        return await unitOfWork.CheckpointStorage.GetViewCheckpointAsync(ViewName, cancellationToken);
    }

    private bool RequiresRebuild(ViewCheckpoint? viewCheckpoint,
                                 TopicBootstrapResult<SportMessage> sportsBootstrap,
                                 TopicBootstrapResult<RawCategoryMessage> categoriesBootstrap)
    {
        if (viewCheckpoint is null)
        {
            return true;
        }

        if (!viewCheckpoint.BuildCompleted)
        {
            return true;
        }

        if (viewCheckpoint.StoreId != _storeMetadata.StoreGenerationId.ToString("D"))
        {
            return true;
        }

        return sportsBootstrap is TopicRebuiltFromEarliest<SportMessage>
               || categoriesBootstrap is TopicRebuiltFromEarliest<RawCategoryMessage>;
    }

    private static IReadOnlyCollection<TableEntryChange<TMessage>> GetDeltaChanges<TMessage>(TopicBootstrapResult<TMessage> bootstrapResult)
        => bootstrapResult switch
        {
            TopicRecoveredFromStateStore<TMessage> recovered => recovered.DeltaChanges,
            TopicHighWatermarkNotFound<TMessage> => [],
            _ => throw new InvalidOperationException($"Bootstrap result {bootstrapResult.GetType().Name} does not expose delta changes.")
        };

    
    private async Task<List<GeoTaxonomyNode>> FilterCategoriesForGeoViewAsync(IReadOnlySet<CategoryId> categoriesIds, IDictionary<string, RawCategoryMessage> countryCategories, CancellationToken cancellationToken)
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
                var rawCategoryMessage = await _categoriesTableView.GetEntry(categoryId.Value, cancellationToken);
                if (rawCategoryMessage?.CountryCode != null)
                {
                    nodes.Add(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));
                }
            }
        }

        return [.. nodes.DistinctBy(cat => cat.CountryCode).OrderBy(cat => cat.CountryCode)];
    }

    private async Task<List<GeoTaxonomyNode>> FilterCategoriesForGeoViewAsync(IReadOnlySet<CategoryId> categoriesIds, CancellationToken cancellationToken)
    {
        List<GeoTaxonomyNode> nodes = new(categoriesIds.Count);
        foreach (var categoryId in categoriesIds)
        {
            var rawCategoryMessage = await _categoriesTableView.GetEntry(categoryId.Value, cancellationToken);
            if (rawCategoryMessage?.CountryCode != null)
            {
                nodes.Add(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));
            }
        }

        return [.. nodes.DistinctBy(cat => cat.CountryCode).OrderBy(cat => cat.CountryCode)];
    }
    private async Task<GeoTaxonomyViewMessage> GetViewFromSport(SportMessage sport, CancellationToken cancellationToken)
    {
        var sportId = new SportId(sport.Id);
        var pendingCategories = await _pendingIndex.GetOrphanCategoriesBySport(sportId, cancellationToken);
        var sportCategories = await FilterCategoriesForGeoViewAsync(pendingCategories, cancellationToken);
        return GeoTaxonomyViewMessage.Create(sport, sportCategories);
    }

    private async Task OnCategoryChangeAsync(TableEntryChange<RawCategoryMessage> @event, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case TableEntryCreated<RawCategoryMessage> created:
                await OnCategoryCreated(created.NewValue, cancellationToken);

                break;
            case TableEntryUpdated<RawCategoryMessage> updated:
                await OnCategoryUpdated(updated.NewValue, updated.CurrentValue, cancellationToken);
                break;

            case EventDeleted<RawCategoryMessage> deleted:
                await OnCategoryDeleted(new SportId(deleted.CurrentValue.SportId), new CategoryId(deleted.CurrentValue.Id), cancellationToken);

                break;
        }
    }

    private async Task OnSportChangeAsync(TableEntryChange<SportMessage> @event, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case TableEntryCreated<SportMessage> created:
                await OnSportCreated(created.NewValue, cancellationToken);
                break;
            case TableEntryUpdated<SportMessage> updated:
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
            await OnCategoryDeleted(new SportId(oldCategory.SportId), new CategoryId(oldCategory.Id), cancellationToken);
            await OnCategoryCreated(category, cancellationToken);
        }
    }

    private async Task OnCategoryCreated(RawCategoryMessage rawCategoryMessage, CancellationToken cancellationToken)
    {
        if (rawCategoryMessage.CountryCode is null)
        {
            return;
        }

        await _relationIndex.AddCategorybySportAsync(new SportId(rawCategoryMessage.SportId), new CategoryId(rawCategoryMessage.Id), cancellationToken);
        var sport = await _sportsTableView.GetEntry(rawCategoryMessage.SportId, cancellationToken);

        if (sport is null)
        {
            await _pendingIndex.AddOrphanCategorybySportAsync(new SportId(rawCategoryMessage.SportId), new CategoryId(rawCategoryMessage.Id), cancellationToken);
            return;
        }

        var currentView = _materializeViewStorage.TryGetView(new SportId(sport.Id));
        var viewUpdated = currentView is null
            ? await GetViewFromSport(sport, cancellationToken)
            : currentView.AddOrUpdateCategory(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));

        _materializeViewStorage.AddTaxonomyView(new SportId(sport.Id), viewUpdated);

        await _taxonomyPublisher.PublishAsync(viewUpdated, cancellationToken);
    }


    private async Task OnSportCreated(SportMessage sport, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Sport created for key {Key}:{NewLine}{Payload}",
                               sport.Id,
                               Environment.NewLine,
                               ToJson(sport));
        GeoTaxonomyViewMessage newView = await GetViewFromSport(sport, cancellationToken);
        _materializeViewStorage.AddTaxonomyView(new SportId(sport.Id), newView);
        await _taxonomyPublisher.PublishAsync(newView, cancellationToken);
        await _pendingIndex.ClearOrphanCategoryWithSportIdAsync(new SportId(sport.Id), cancellationToken);
    }
    private async Task OnSportUpdated(SportMessage sport, SportMessage oldSport, CancellationToken cancellationToken)
    {
        // 1. Fractional
        if ((sport.Name, sport.SportType) == (oldSport.Name, oldSport.SportType))
        {
            return;
        }

        var currentView = _materializeViewStorage.TryGetView(new SportId(sport.Id));
        var viewUpdated = currentView is null
            ? await GetViewFromSport(sport, cancellationToken)
            : GeoTaxonomyViewMessage.Create(sport, currentView.GeoCategories);

        _materializeViewStorage.AddTaxonomyView(new SportId(sport.Id), viewUpdated);

        await _taxonomyPublisher.PublishAsync(viewUpdated, cancellationToken);
    }

    private async Task OnSportDeleted(string sportId, CancellationToken cancellationToken)
    {
        await _relationIndex.ClearCategoryWithSportIdAsync(new SportId(sportId), cancellationToken);
        await _pendingIndex.ClearOrphanCategoryWithSportIdAsync(new SportId(sportId), cancellationToken);

        _materializeViewStorage.RemoveView(new SportId(sportId));
        await _taxonomyPublisher.PublishDeleteMessageAsync(sportId, DateTimeOffset.UtcNow, cancellationToken);

    }
}
