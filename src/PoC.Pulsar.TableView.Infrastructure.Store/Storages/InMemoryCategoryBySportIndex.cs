using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;


public sealed class InMemoryCategoryBySportIndex : ICategoryRelationIndex
{
    private static readonly byte Dummy = 0;
    private readonly ConcurrentDictionary<StorageKey, ConcurrentDictionary<string, byte>> _relations = new();

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

        return ValueTask.FromResult(Contains(StorageKey.CategoryBySportPrefix(sportId), categoryId.Value));
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = GetValues(StorageKey.CategoryByParentPrefix(parentCategoryId))
            .Select(value => new CategoryId(value))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    public ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId,
                                                    CategoryId categoryId,
                                                    CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(Contains(StorageKey.CategoryByParentPrefix(parentCategoryId), categoryId.Value));
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _relations.Clear();

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

        Remove(StorageKey.CategoryBySportPrefix(current.SportId), current.CategoryId.Value);

        if (current.ParentCategoryId is not null)
        {
            Remove(StorageKey.CategoryByParentPrefix(current.ParentCategoryId.Value), current.CategoryId.Value);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = GetValues(StorageKey.CategoryBySportPrefix(sportId))
            .Select(value => new CategoryId(value))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    private void AddCurrentRelations(CategoryRelations current)
    {
        Add(StorageKey.CategoryBySportPrefix(current.SportId), current.CategoryId.Value);

        if (current.ParentCategoryId is not null)
        {
            Add(StorageKey.CategoryByParentPrefix(current.ParentCategoryId.Value), current.CategoryId.Value);
        }
    }

    private void RemoveObsoleteRelations(CategoryRelations previous, CategoryRelations current)
    {
        if (previous.SportId != current.SportId ||
            previous.CategoryId != current.CategoryId)
        {
            Remove(StorageKey.CategoryBySportPrefix(previous.SportId), previous.CategoryId.Value);
        }

        if (previous.ParentCategoryId is not null &&
            (previous.ParentCategoryId != current.ParentCategoryId ||
             previous.CategoryId != current.CategoryId))
        {
            Remove(StorageKey.CategoryByParentPrefix(previous.ParentCategoryId.Value), previous.CategoryId.Value);
        }
    }

    private void Add(StorageKey key, string id)
    {
        _relations.GetOrAdd(key, _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal))
                  .TryAdd(id, Dummy);
    }

    private void Remove(StorageKey key, string id)
    {
        if (_relations.TryGetValue(key, out var values))
        {
            values.TryRemove(id, out _);
        }
    }

    private bool Contains(StorageKey key, string id)
        => _relations.TryGetValue(key, out var values) && values.ContainsKey(id);

    private IReadOnlyCollection<string> GetValues(StorageKey key)
        => _relations.TryGetValue(key, out var values) ? values.Keys.ToArray() : [];
}
