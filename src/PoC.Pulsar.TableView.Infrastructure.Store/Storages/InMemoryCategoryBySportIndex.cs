using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using System.Collections.Generic;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;


public sealed class InMemoryCategoryBySportIndex : ICategoryRelationIndex
{
    private static readonly byte Dummy = 0;
    private readonly ConcurrentDictionary<StorageKey, byte> _keys = new();

    public ValueTask IndexCategoryAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        AddCurrentRelations(current);

        return ValueTask.CompletedTask;
    }
    public ValueTask<bool> HasCategoryBySportAsync(SportId sportId,
                                                   CategoryId categoryId,
                                                   CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var key = StorageKey.CategoryBySport(sportId, categoryId);

        return ValueTask.FromResult(_keys.ContainsKey(key));
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var prefix = StorageKey.CategoryByParentPrefix(parentCategoryId).Value;

        var result = _keys.Keys
            .Select(key => key.Value)
            .Where(value => value.StartsWith(prefix, StringComparison.Ordinal))
            .Select(value => new CategoryId(value[prefix.Length..]))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    public ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId,
                                                    CategoryId categoryId,
                                                    CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var key = StorageKey.CategoryByParent(parentCategoryId, categoryId);

        return ValueTask.FromResult(_keys.ContainsKey(key));
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _keys.Clear();

        return ValueTask.CompletedTask;
    }
    public ValueTask ReplaceCategoryRelationsAsync(CategoryRelations? previous,
                                                   CategoryRelations current,
                                                   CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (previous is not null)
        {
            RemoveObsoleteRelations(previous.Value, current);
        }

        AddCurrentRelations(current);

        return ValueTask.CompletedTask;
    }

    public ValueTask RemoveCategoryRelationsAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Remove(StorageKey.CategoryBySport(current.SportId, current.CategoryId));

        if (current.ParentCategoryId is not null)
        {
            Remove(StorageKey.CategoryByParent(
                current.ParentCategoryId.Value,
                current.CategoryId));
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var prefix = StorageKey.CategoryBySportPrefix(sportId).Value;

        var result = _keys.Keys
            .Select(key => key.Value)
            .Where(value => value.StartsWith(prefix, StringComparison.Ordinal))
            .Select(value => new CategoryId(value[prefix.Length..]))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    private void AddCurrentRelations(CategoryRelations current)
    {
        Add(StorageKey.CategoryBySport(current.SportId, current.CategoryId));

        if (current.ParentCategoryId is not null)
        {
            Add(StorageKey.CategoryByParent(current.ParentCategoryId.Value, current.CategoryId));
        }
    }

    private void RemoveObsoleteRelations(CategoryRelations previous, CategoryRelations current)
    {
        if (previous.SportId != current.SportId ||
            previous.CategoryId != current.CategoryId)
        {
            Remove(StorageKey.CategoryBySport(previous.SportId, previous.CategoryId));
        }

        if (previous.ParentCategoryId is not null &&
            (previous.ParentCategoryId != current.ParentCategoryId ||
             previous.CategoryId != current.CategoryId))
        {
            Remove(StorageKey.CategoryByParent(previous.ParentCategoryId.Value, previous.CategoryId));
        }
    }

    private void Add(StorageKey key)
    {
        _keys.TryAdd(key, Dummy);
    }

    private void Remove(StorageKey key)
    {
        _keys.TryRemove(key, out _);
    }
}
