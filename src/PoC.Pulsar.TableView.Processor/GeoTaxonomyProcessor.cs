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
using System.Reactive.Linq;
using System.Text.Json;

namespace PoC.Pulsar.TableView.Processor;

internal sealed class GeoTaxonomyProcessor
{
    private const string ViewName = "category-taxonomy";

    private readonly IPulsarTableView<RawCategoryMessage> _categoriesTableView;
    private readonly ILogger<GeoTaxonomyProcessor> _logger;
    private readonly StoreMetadata _storeMetadata;
    private readonly IPulsarTableView<SportMessage> _sportsTableView;
    private readonly ITaxonomyViewPublisher _taxonomyPublisher;
    private readonly IUnitOfWorkFactory _unitOfWorkFactory;

    public GeoTaxonomyProcessor(IPulsarTableView<SportMessage> sportsTableView,
                                IPulsarTableView<RawCategoryMessage> categoriesTableView,
                                ITaxonomyViewPublisher taxonomyPublisher,
                                IUnitOfWorkFactory unitOfWorkFactory,
                                StoreMetadata storeMetadata,
                                ILogger<GeoTaxonomyProcessor> logger)
        => (_sportsTableView, _categoriesTableView, _taxonomyPublisher, _unitOfWorkFactory, _storeMetadata, _logger)
            = (sportsTableView, categoriesTableView, taxonomyPublisher, unitOfWorkFactory, storeMetadata, logger);


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
        //var sportsBootstrap = await _sportsTableView.StartBootstrapAsync(cancellationToken);
        //var categoriesBootstrap = await _categoriesTableView.StartBootstrapAsync(cancellationToken);
        //ViewCheckpoint? viewCheckpoint = await GetViewCheckpointAsync(cancellationToken);

        bool requiresRebuild = RequiresRebuild(viewCheckpoint, sportsBootstrap, categoriesBootstrap);
        if (requiresRebuild)
        {
            var sports = _sportsTableView.GetSnapshot();
            var categories = _categoriesTableView.GetSnapshot(new GeoCategoryMessageFilter());

            using var unitOfWork = _unitOfWorkFactory.CreateGeoTaxonomyBuild();
            await ClearProjectorStateAsync(cancellationToken, unitOfWork);
            await BuildAsync(sports, categories, cancellationToken, unitOfWork);
            await SaveViewCheckpointAsync(cancellationToken, unitOfWork);
            await unitOfWork.CommitAsync(cancellationToken);
        }
        else
        {
            using var unitOfWork = _unitOfWorkFactory.CreateGeoTaxonomyBuild();
            await ApplyBootstrapDeltasAsync(sportsBootstrap, categoriesBootstrap, cancellationToken, unitOfWork);
            await SaveViewCheckpointAsync(cancellationToken, unitOfWork);
            await unitOfWork.CommitAsync(cancellationToken);
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

    private async Task BuildAsync(IDictionary<string, SportMessage> sports,
                                  IDictionary<string, RawCategoryMessage> categories,
                                  CancellationToken cancellationToken,
                                  IGeoTaxonomyBuildUnitOfWork unitOfWork)
    {
        var buildGenerationId = CreateBuildGenerationId();
        foreach (var category in categories.Values)
        {
            var relations = new CategoryRelations(new CategoryId(category.Id),
                                                  new SportId(category.SportId),
                                                  GetParentCategoryId(category.ParentId));

            await unitOfWork.CategoryRelationIndex.IndexCategoryAsync(relations, cancellationToken);

            if (!sports.ContainsKey(category.SportId))
            {
                await unitOfWork.CategoryPendingIndex.TryMarkCategoryWaitingForSportAsync(relations.SportId,
                                                                                                 relations.CategoryId,
                                                                                                 // in this process, we dont use the doble check because we have all the categories and we are not listen for new ones until the build is finished
                                                                                                 // so we are sure that if the sport is not in the list we can avoid an extra read to the index
                                                                                                 (sportId, ct) => ValueTask.FromResult(false),
                                                                                                 cancellationToken);
            }
        }

        foreach (var sport in sports.Values)
        {
            var sportId = new SportId(sport.Id);
            var categoryIds = await unitOfWork.CategoryRelationIndex.GetCategoriesBySportAsync(sportId, cancellationToken);
            var sportCategories = await FilterCategoriesForGeoViewAsync(categoryIds, categories, cancellationToken);
            var candidateView = GeoTaxonomyViewMessage.Create(sport, sportCategories, version: 0);
            await SaveAndPublishViewAsync(unitOfWork.MaterializeViewStorage, sportId, candidateView, buildGenerationId, cancellationToken);
        }

    }

    private static string ToJson<T>(T value) => JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true });

    private async Task ApplyBootstrapDeltasAsync(TopicBootstrapResult<SportMessage> sportsBootstrap,
                                                 TopicBootstrapResult<RawCategoryMessage> categoriesBootstrap,
                                                 CancellationToken cancellationToken,
                                                 IGeoTaxonomyBuildUnitOfWork? unitOfWork)
    {
        foreach (var sportChange in GetDeltaChanges(sportsBootstrap))
        {
            await OnSportChangeAsync(sportChange, cancellationToken, unitOfWork);
        }

        foreach (var categoryChange in GetDeltaChanges(categoriesBootstrap))
        {
            await OnCategoryChangeAsync(categoryChange, cancellationToken, unitOfWork);
        }

    }

    private async Task ClearProjectorStateAsync(CancellationToken cancellationToken, IGeoTaxonomyBuildUnitOfWork unitOfWork)
    {
        await unitOfWork.MaterializeViewStorage.ClearAsync(cancellationToken);
        await unitOfWork.CategoryRelationIndex.ClearAsync(cancellationToken);
        await unitOfWork.CategoryPendingIndex.ClearAsync(cancellationToken);
    }

    private async Task SaveViewCheckpointAsync(CancellationToken cancellationToken, IGeoTaxonomyBuildUnitOfWork unitOfWork)
    {

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
    private async Task<GeoTaxonomyViewMessage> GetViewFromSport(SportMessage sport, ICategoryPendingIndex pendingIndex, CancellationToken cancellationToken)
    {
        var sportId = new SportId(sport.Id);
        var pendingCategories = await pendingIndex.GetCategoriesWaitingForSportAsync(sportId, cancellationToken);
        var sportCategories = await FilterCategoriesForGeoViewAsync(pendingCategories, cancellationToken);
        return GeoTaxonomyViewMessage.Create(sport, sportCategories);
    }

    private async Task OnCategoryChangeAsync(TableEntryChange<RawCategoryMessage> @event,
                                             CancellationToken cancellationToken,
                                             IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        switch (@event)
        {
            case TableEntryCreated<RawCategoryMessage> created:
                await OnCategoryCreated(created.NewValue, cancellationToken, unitOfWork);

                break;
            case TableEntryUpdated<RawCategoryMessage> updated:
                await OnCategoryUpdated(updated.NewValue, updated.CurrentValue, cancellationToken, unitOfWork);
                break;

            case EventDeleted<RawCategoryMessage> deleted:
                await OnCategoryDeleted(new SportId(deleted.CurrentValue.SportId),
                                        new CategoryId(deleted.CurrentValue.Id),
                                        GetParentCategoryId(deleted.CurrentValue.ParentId),
                                        cancellationToken,
                                        unitOfWork);

                break;
        }
    }

    private async Task OnSportChangeAsync(TableEntryChange<SportMessage> @event,
                                          CancellationToken cancellationToken,
                                          IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        switch (@event)
        {
            case TableEntryCreated<SportMessage> created:
                await OnSportCreated(created.NewValue, cancellationToken, unitOfWork);
                break;
            case TableEntryUpdated<SportMessage> updated:
                _logger.LogInformation("Sports live update for key {Key}:{NewLine}{Payload}",
                                       updated.Key,
                                       Environment.NewLine,
                                       ToJson(updated.NewValue));
                await OnSportUpdated(updated.NewValue, updated.CurrentValue, cancellationToken, unitOfWork);
                break;

            case EventDeleted<SportMessage> delete:
                _logger.LogInformation("Sports live delete for key {Key}.", delete.Key);
                await OnSportDeleted(delete.Key, cancellationToken, unitOfWork);
                break;
        }
    }

    private async Task OnCategoryDeleted(SportId sportId,
                                         CategoryId categoryId,
                                         CategoryId? parentId,
                                         CancellationToken cancellationToken,
                                         IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            CategoryRelations relations = new(categoryId, sportId, parentId);
            await currentUnitOfWork.CategoryRelationIndex.RemoveCategoryRelationsAsync(relations, cancellationToken);
            await currentUnitOfWork.CategoryPendingIndex.RemoveCategoryFromPendingAsync(relations.CategoryId, cancellationToken);
            var viewUpdated = await currentUnitOfWork.MaterializeViewStorage.RemoveCategoryAsync(sportId, categoryId, cancellationToken);

            if (viewUpdated.Changed && viewUpdated.View is not null)
            {
                await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, sportId, viewUpdated.View, CreateBuildGenerationId(), cancellationToken);
            }
        }, cancellationToken);
    }

    private static CategoryId? GetParentCategoryId(string? parentId)
        => string.IsNullOrWhiteSpace(parentId) ? null : new CategoryId(parentId);

    private async Task OnCategoryUpdated(RawCategoryMessage category,
                                         RawCategoryMessage oldCategory,
                                         CancellationToken cancellationToken,
                                         IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        if (category.SportId != oldCategory.SportId)
        {
            await OnCategoryDeleted(new SportId(oldCategory.SportId),
                                    new CategoryId(oldCategory.Id),
                                    GetParentCategoryId(oldCategory.ParentId),
                                    cancellationToken,
                                    unitOfWork);
            await OnCategoryCreated(category, cancellationToken, unitOfWork);
            return;
        }

        if (category.CountryCode == oldCategory.CountryCode && category.ParentId == oldCategory.ParentId)
        {
            return;
        }

        var currentRelations = new CategoryRelations(new CategoryId(category.Id),
                                                     new SportId(category.SportId),
                                                     GetParentCategoryId(category.ParentId));

        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            await currentUnitOfWork.CategoryRelationIndex.IndexCategoryAsync(currentRelations, cancellationToken);

            if (category.CountryCode is null)
            {
                await currentUnitOfWork.CategoryRelationIndex.RemoveCategoryRelationsAsync(currentRelations, cancellationToken);
                await currentUnitOfWork.CategoryPendingIndex.RemoveCategoryFromPendingAsync(currentRelations.CategoryId, cancellationToken);
                var removed = await currentUnitOfWork.MaterializeViewStorage.RemoveCategoryAsync(currentRelations.SportId, currentRelations.CategoryId, cancellationToken);

                if (removed.Changed && removed.View is not null)
                {
                    await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, currentRelations.SportId, removed.View, CreateBuildGenerationId(), cancellationToken);
                }

                return;
            }

            var result = await currentUnitOfWork.MaterializeViewStorage.UpsertCategoryAsync(new SportId(category.SportId),
                                                                                           new GeoTaxonomyNode(category.Id, category.CountryCode),
                                                                                           cancellationToken);

            if (result.Changed && result.View is not null)
            {
                await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, new SportId(category.SportId), result.View, CreateBuildGenerationId(), cancellationToken);
            }
        }, cancellationToken);
    }

    private async Task OnCategoryCreated(RawCategoryMessage rawCategoryMessage,
                                         CancellationToken cancellationToken,
                                         IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        if (rawCategoryMessage.CountryCode is null)
        {
            return;
        }

        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            await currentUnitOfWork.CategoryRelationIndex.IndexCategoryAsync(new CategoryRelations(new CategoryId(rawCategoryMessage.Id),
                                                                                                  new SportId(rawCategoryMessage.SportId),
                                                                                                  GetParentCategoryId(rawCategoryMessage.ParentId)),
                                                                            cancellationToken);
            var sport = await _sportsTableView.GetEntry(rawCategoryMessage.SportId, cancellationToken);

            if (sport is null)
            {
                await currentUnitOfWork.CategoryPendingIndex.TryMarkCategoryWaitingForSportAsync(new SportId(rawCategoryMessage.SportId),
                                                                                                 new CategoryId(rawCategoryMessage.Id),
                                                                                                 (pendingSportId, ct) => ValueTask.FromResult(false),
                                                                                                 cancellationToken);
                return;
            }

            var sportId = new SportId(sport.Id);
            var currentView = await currentUnitOfWork.MaterializeViewStorage.GetViewAsync(sportId, cancellationToken);
            var viewUpdated = currentView is null
                ? await GetViewFromSport(sport, currentUnitOfWork.CategoryPendingIndex, cancellationToken)
                : currentView.AddOrUpdateCategory(new GeoTaxonomyNode(rawCategoryMessage.Id, rawCategoryMessage.CountryCode!));

            await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, sportId, viewUpdated, CreateBuildGenerationId(), cancellationToken);
        }, cancellationToken);
    }


    private async Task OnSportCreated(SportMessage sport,
                                      CancellationToken cancellationToken,
                                      IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        _logger.LogInformation("Sport created for key {Key}:{NewLine}{Payload}",
                               sport.Id,
                               Environment.NewLine,
                               ToJson(sport));
        var sportId = new SportId(sport.Id);
        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            GeoTaxonomyViewMessage newView = await GetViewFromSport(sport, currentUnitOfWork.CategoryPendingIndex, cancellationToken);
            await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, sportId, newView, CreateBuildGenerationId(), cancellationToken);

            var pendingCategories = await currentUnitOfWork.CategoryPendingIndex.GetCategoriesWaitingForSportAsync(sportId, cancellationToken);
            foreach (var pendingCategoryId in pendingCategories)
            {
                await currentUnitOfWork.CategoryPendingIndex.ResolveCategoryWaitingForSportAsync(sportId, pendingCategoryId, cancellationToken);
            }

        }, cancellationToken);
    }

    private async Task OnSportUpdated(SportMessage sport,
                                      SportMessage oldSport,
                                      CancellationToken cancellationToken,
                                      IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        if ((sport.Name, sport.SportType) == (oldSport.Name, oldSport.SportType))
        {
            return;
        }

        var sportId = new SportId(sport.Id);
        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            var currentView = await currentUnitOfWork.MaterializeViewStorage.GetViewAsync(sportId, cancellationToken);
            var viewUpdated = currentView is null
                ? await GetViewFromSport(sport, currentUnitOfWork.CategoryPendingIndex, cancellationToken)
                : GeoTaxonomyViewMessage.Create(sport, currentView.GeoCategories);

            await SaveAndPublishViewAsync(currentUnitOfWork.MaterializeViewStorage, sportId, viewUpdated, CreateBuildGenerationId(), cancellationToken);
        }, cancellationToken);
    }

    private async Task OnSportDeleted(string sportId,
                                      CancellationToken cancellationToken,
                                      IGeoTaxonomyBuildUnitOfWork? unitOfWork = null)
    {
        var parsedSportId = new SportId(sportId);
        await UseGeoTaxonomyUnitOfWorkAsync(unitOfWork, async currentUnitOfWork =>
        {
            var categoryIds = await currentUnitOfWork.CategoryRelationIndex.GetCategoriesBySportAsync(parsedSportId, cancellationToken);

            foreach (var categoryId in categoryIds)
            {
                await currentUnitOfWork.CategoryRelationIndex.RemoveCategoryRelationsAsync(new CategoryRelations(categoryId, parsedSportId, null), cancellationToken);
                await currentUnitOfWork.CategoryPendingIndex.ResolveCategoryWaitingForSportAsync(parsedSportId, categoryId, cancellationToken);
            }

            await currentUnitOfWork.MaterializeViewStorage.RemoveViewAsync(parsedSportId, cancellationToken);
            await _taxonomyPublisher.PublishDeleteMessageAsync(sportId, DateTimeOffset.UtcNow, cancellationToken);
        }, cancellationToken);

    }

    private async Task UseGeoTaxonomyUnitOfWorkAsync(IGeoTaxonomyBuildUnitOfWork? unitOfWork,
                                                     Func<IGeoTaxonomyBuildUnitOfWork, Task> operation,
                                                     CancellationToken cancellationToken)
    {
        if (unitOfWork is not null)
        {
            await operation(unitOfWork);
            return;
        }

        using var createdUnitOfWork = _unitOfWorkFactory.CreateGeoTaxonomyBuild();
        await operation(createdUnitOfWork);
        await createdUnitOfWork.CommitAsync(cancellationToken);
    }

    private static string CreateBuildGenerationId()
        => $"build-{Guid.CreateVersion7():N}";

    private async Task SaveAndPublishViewAsync(IGeoTaxonomyViewStorage viewStorage,
                                               SportId sportId,
                                               GeoTaxonomyViewMessage candidateView,
                                               string buildGenerationId,
                                               CancellationToken cancellationToken)
    {
        var result = await viewStorage.UpsertViewAsync(sportId, candidateView, buildGenerationId, cancellationToken);
        await _taxonomyPublisher.PublishAsync(result.View, cancellationToken);
        await viewStorage.MarkViewPublishedAsync(sportId, result.CalculatedVersion, buildGenerationId, cancellationToken);
    }
}
