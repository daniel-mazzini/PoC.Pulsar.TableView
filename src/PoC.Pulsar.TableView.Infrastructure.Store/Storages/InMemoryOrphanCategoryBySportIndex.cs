using System.Collections.Generic;
using System.Linq;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryOrphanCategoryBySportIndex : ICategoryPendingIndex
{
    private static readonly byte Dummy = 0;
    private readonly ConcurrentDictionary<StorageKey, ConcurrentDictionary<string, byte>> _relations = new();
    public async ValueTask<bool> TryMarkCategoryWaitingForSportAsync(SportId sportId,
                                                               CategoryId categoryId,
                                                               Func<SportId, CancellationToken, ValueTask<bool>> sportExistsCheck,
                                                               CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (await sportExistsCheck(sportId, cancellationToken))
        {
            return false;
        }

        AddPendingKeys(sportId, categoryId);

        // doble check
        if (await sportExistsCheck(sportId, cancellationToken))
        {
            RemovePendingKeys(sportId, categoryId);
            return false;
        }

        return true;
    }
    public ValueTask ResolveCategoryWaitingForSportAsync(SportId sportId,
                                                         CategoryId categoryId,
                                                         CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        RemovePendingKeys(sportId, categoryId);

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesWaitingForSportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = GetValues(StorageKey.OrphanCategoryBySportPrefix(sportId))
            .Select(value => new CategoryId(value))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    public ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = GetValues(StorageKey.OrphanSportByCategoryPrefix(categoryId))
            .Select(value => new SportId(value))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<SportId>>(result);
    }

    public async ValueTask RemoveCategoryFromPendingAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var missingSports = await GetMissingSportsForCategoryAsync(categoryId, cancellationToken);

        foreach (var sportId in missingSports)
        {
            RemovePendingKeys(sportId, categoryId);
        }
    }

    private void AddPendingKeys(SportId sportId, CategoryId categoryId)
    {
        Add(StorageKey.OrphanCategoryBySportPrefix(sportId), categoryId.Value);
        Add(StorageKey.OrphanSportByCategoryPrefix(categoryId), sportId.Value);
    }

    private void RemovePendingKeys(SportId sportId, CategoryId categoryId)
    {
        Remove(StorageKey.OrphanCategoryBySportPrefix(sportId), categoryId.Value);
        Remove(StorageKey.OrphanSportByCategoryPrefix(categoryId), sportId.Value);
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _relations.Clear();

        return ValueTask.CompletedTask;
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

    private IReadOnlyCollection<string> GetValues(StorageKey key)
        => _relations.TryGetValue(key, out var values) ? values.Keys.ToArray() : [];

}
