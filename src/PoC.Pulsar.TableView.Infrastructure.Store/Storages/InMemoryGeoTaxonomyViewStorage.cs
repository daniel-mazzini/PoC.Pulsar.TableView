using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
{
    private readonly ConcurrentDictionary<SportId, GeoTaxonomyViewMessage> _views = [];

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.TryGetValue(sportId, out var view);
        return ValueTask.FromResult(view);
    }

    public ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var categoriesToRemove = view.GeoCategories
            .Where(x => x.CategoryId == categoryId.Value)
            .ToArray();

        if (categoriesToRemove.Length == 0)
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var updated = view with { GeoCategories = view.GeoCategories.Except(categoriesToRemove) };
        _views[sportId] = updated;

        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.TryRemove(sportId, out var view);
        return ValueTask.FromResult(view);
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var existing = view.GeoCategories.FirstOrDefault(x => x.CategoryId == node.CategoryId);

        if (existing is not null && existing.Equals(node))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var toRemove = view.GeoCategories.Where(x => x.CategoryId == node.CategoryId);
        var updated = view with { GeoCategories = view.GeoCategories.Except(toRemove).Add(node) };

        _views[sportId] = updated;

        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var key = StorageKey.CountryTaxonomyMaterializedView(sportId);

        var updated = _views.AddOrUpdate(sportId,
                                         _ => GeoTaxonomyViewMessage.CreateNew(sportId.Value, sportName, sportType),
                                         (_, existing) => (existing.SportName == sportName && existing.SportType == sportType)
                                                                        ? existing
                                                                        : existing with { SportName = sportName, SportType = sportType });

        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(updated));
    }

    public async ValueTask UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage taxonomyViewMessage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var key = StorageKey.CountryTaxonomyMaterializedView(sportId);

        _views.AddOrUpdate(
            sportId,
            (_) => taxonomyViewMessage,
            (_, current) => taxonomyViewMessage);

        await ValueTask.CompletedTask;
    }
}
