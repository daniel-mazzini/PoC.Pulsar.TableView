using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryCategoryBySportIndex : ICategoryBySportIndex
{
    private readonly object _gate = new();
    private readonly Dictionary<string, HashSet<CategoryId>> _bySport = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<CategoryId>> _byParent = new(StringComparer.Ordinal);

    public ValueTask AddCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            AddToMap(_bySport, sportId.Value, categoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySport(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult<IReadOnlySet<CategoryId>>(Snapshot(_bySport, sportId.Value));
        }
    }

    public ValueTask RemoveCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            RemoveFromMap(_bySport, sportId.Value, categoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask ClearCategoryWithSportIdAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _bySport.Remove(sportId.Value);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask AddCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            AddToMap(_byParent, parentCategoryId.Value, categoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParent(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult<IReadOnlySet<CategoryId>>(Snapshot(_byParent, parentCategoryId.Value));
        }
    }

    public ValueTask RemoveCategorybyParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            RemoveFromMap(_byParent, parentCategoryId.Value, categoryId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask ClearCategoryWithParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            _byParent.Remove(parentCategoryId.Value);
        }

        return ValueTask.CompletedTask;
    }

    private static void AddToMap(Dictionary<string, HashSet<CategoryId>> map, string key, CategoryId categoryId)
    {
        if (!map.TryGetValue(key, out var values))
        {
            values = [];
            map[key] = values;
        }

        values.Add(categoryId);
    }

    private static void RemoveFromMap(Dictionary<string, HashSet<CategoryId>> map, string key, CategoryId categoryId)
    {
        if (!map.TryGetValue(key, out var values))
        {
            return;
        }

        values.Remove(categoryId);
        if (values.Count == 0)
        {
            map.Remove(key);
        }
    }

    private static IReadOnlySet<CategoryId> Snapshot(Dictionary<string, HashSet<CategoryId>> map, string key)
        => map.TryGetValue(key, out var values) ? new HashSet<CategoryId>(values) : new HashSet<CategoryId>();
}
