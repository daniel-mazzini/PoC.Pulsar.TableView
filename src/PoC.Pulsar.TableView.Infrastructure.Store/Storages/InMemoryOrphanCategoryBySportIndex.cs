using System.Collections.Generic;
using System.Linq;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryOrphanCategoryBySportIndex : ICategoryPendingIndex
{
    private static readonly byte Dummy = 0;
    private readonly ConcurrentDictionary<StorageKey, byte> _store = new();
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

        var key = StorageKey.OrphanCategoryBySport(sportId, categoryId);

        _store.TryAdd(key, Dummy);

        // doble check
        if (await sportExistsCheck(sportId, cancellationToken))
        {
            _store.TryRemove(key, out _);
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

        var prefix = StorageKey.OrphanCategoryBySportPrefix(sportId).Value;

        var result = _store.Keys
            .Select(key => key.Value)
            .Where(value => value.StartsWith(prefix, StringComparison.Ordinal))
            .Select(value => new CategoryId(value[prefix.Length..]))
            .ToHashSet();

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    public ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var prefix = StorageKey.OrphanSportByCategoryPrefix(categoryId).Value;

        var result = _store.Keys
            .Select(key => key.Value)
            .Where(value => value.StartsWith(prefix, StringComparison.Ordinal))
            .Select(value => new SportId(value[prefix.Length..]))
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

    private void RemovePendingKeys(SportId sportId, CategoryId categoryId)
    {
        _store.TryRemove(StorageKey.OrphanCategoryBySport(sportId, categoryId),
                         out _);

        _store.TryRemove(StorageKey.OrphanSportByCategory(categoryId, sportId),
                         out _);
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _store.Clear();

        return ValueTask.CompletedTask;
    }

}
