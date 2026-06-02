using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Sports;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
{
    private readonly object _gate = new();
    private readonly Dictionary<string, GeoTaxonomyViewMessage> _views = new(StringComparer.Ordinal);

    public void AddTaxonomyView(SportId id, GeoTaxonomyViewMessage view)
    {
        lock (_gate)
        {
            _views[id.Value] = view;
        }
    }

    public GeoTaxonomyViewMessage? AddCategoryAsync(string sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_views.TryGetValue(sportId, out var view))
            {
                return null;
            }

            var updated = view.AddOrUpdateCategory(node);
            _views[sportId] = updated;
            return updated;
        }
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _views.Clear();
        }

        return ValueTask.CompletedTask;
    }

    public GeoTaxonomyViewMessage? TryGetView(SportId id)
    {
        lock (_gate)
        {
            return _views.GetValueOrDefault(id.Value);
        }
    }

    public ValueTask<GeoTaxonomyViewMessage?> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_views.TryGetValue(sportId.Value, out var view))
            {
                return ValueTask.FromResult<GeoTaxonomyViewMessage?>(null);
            }

            var updated = view.RemoveItem(categoryId.Value);
            if (ReferenceEquals(updated, view))
            {
                return ValueTask.FromResult<GeoTaxonomyViewMessage?>(null);
            }

            _views[sportId.Value] = updated;
            return ValueTask.FromResult<GeoTaxonomyViewMessage?>(updated);
        }
    }

    public GeoTaxonomyViewMessage? RemoveView(SportId id)
    {
        lock (_gate)
        {
            if (_views.TryGetValue(id.Value, out var view))
            {
                _views.Remove(id.Value);
                return view;
            }

            return null;
        }
    }
}
