using System.Text;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

public sealed class DefaultCategoryPendingIndex : TsavoriteRepositoryBase, ICategoryPendingIndex, IDisposable
{
    private static readonly byte[] OrphanCategoryBySportPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.OrphanCategoryBySportIndexPrefix);
    private static readonly byte[] OrphanSportByCategoryPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.OrphanSportByCategoryIndexPrefix);

    private readonly ITsavoriteSessionProvider _sessionProvider;
    private bool _disposed;

    public DefaultCategoryPendingIndex(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);

        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask<bool> TryMarkCategoryWaitingForSportAsync(SportId sportId,
                                                                     CategoryId categoryId,
                                                                     Func<SportId, CancellationToken, ValueTask<bool>> sportExistsCheck,
                                                                     CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (await sportExistsCheck(sportId, cancellationToken))
        {
            return false;
        }

        await AddPendingKeysAsync(sportId, categoryId, cancellationToken);

        if (await sportExistsCheck(sportId, cancellationToken))
        {
            await RemovePendingKeysAsync(sportId, categoryId, cancellationToken);
            return false;
        }

        return true;
    }

    public async ValueTask ResolveCategoryWaitingForSportAsync(SportId sportId,
                                                               CategoryId categoryId,
                                                               CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        await RemovePendingKeysAsync(sportId, categoryId, cancellationToken);
    }

    public ValueTask<IReadOnlySet<CategoryId>> GetCategoriesWaitingForSportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        return ReadCategoryIdsByPrefixAsync(StorageKey.OrphanCategoryBySportPrefix(sportId).Value,
                                            OrphanCategoryBySportPrefixBytes,
                                            cancellationToken);
    }

    public ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        return ReadSportIdsByPrefixAsync(StorageKey.OrphanSportByCategoryPrefix(categoryId).Value,
                                         OrphanSportByCategoryPrefixBytes,
                                         cancellationToken);
    }

    public async ValueTask RemoveCategoryFromPendingAsync(CategoryId categoryId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var missingSports = await GetMissingSportsForCategoryAsync(categoryId, cancellationToken);

        foreach (var sportId in missingSports)
        {
            await RemovePendingKeysAsync(sportId, categoryId, cancellationToken);
        }
    }

    public async ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var keysToDelete = new HashSet<string>(StringComparer.Ordinal);
        CollectKeysByPrefix(OrphanCategoryBySportPrefixBytes, keysToDelete);
        CollectKeysByPrefix(OrphanSportByCategoryPrefixBytes, keysToDelete);

        var session = _sessionProvider.GetLightSession();
        foreach (var key in keysToDelete)
        {
            await DeleteIfExistsAsync(session, StorageKey.Create(key), cancellationToken);
        }
    }

    private async ValueTask AddPendingKeysAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        await UpsertAsync(session, StorageKey.OrphanCategoryBySport(sportId, categoryId), categoryId.Value, cancellationToken);
        await UpsertAsync(session, StorageKey.OrphanSportByCategory(categoryId, sportId), sportId.Value, cancellationToken);
    }

    private async ValueTask RemovePendingKeysAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        await DeleteIfExistsAsync(session, StorageKey.OrphanCategoryBySport(sportId, categoryId), cancellationToken);
        await DeleteIfExistsAsync(session, StorageKey.OrphanSportByCategory(categoryId, sportId), cancellationToken);
    }

    private ValueTask<IReadOnlySet<CategoryId>> ReadCategoryIdsByPrefixAsync(string storagePrefix, byte[] prefixBytes, CancellationToken cancellationToken)
    {
        var result = new HashSet<CategoryId>();
        var seenStorageKeys = new HashSet<string>(StringComparer.Ordinal);

        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, value) =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            var storageKey = Encoding.UTF8.GetString(key);
            if (!storageKey.StartsWith(storagePrefix, StringComparison.Ordinal) || !seenStorageKeys.Add(storageKey))
            {
                return;
            }

            var categoryId = Serializer.Deserialize<string>(value);
            if (!string.IsNullOrWhiteSpace(categoryId))
            {
                result.Add(new CategoryId(categoryId));
            }
        });

        return ValueTask.FromResult<IReadOnlySet<CategoryId>>(result);
    }

    private ValueTask<IReadOnlySet<SportId>> ReadSportIdsByPrefixAsync(string storagePrefix, byte[] prefixBytes, CancellationToken cancellationToken)
    {
        var result = new HashSet<SportId>();
        var seenStorageKeys = new HashSet<string>(StringComparer.Ordinal);

        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, value) =>
        {
            cancellationToken.ThrowIfCancellationRequested();

            var storageKey = Encoding.UTF8.GetString(key);
            if (!storageKey.StartsWith(storagePrefix, StringComparison.Ordinal) || !seenStorageKeys.Add(storageKey))
            {
                return;
            }

            var sportId = Serializer.Deserialize<string>(value);
            if (!string.IsNullOrWhiteSpace(sportId))
            {
                result.Add(new SportId(sportId));
            }
        });

        return ValueTask.FromResult<IReadOnlySet<SportId>>(result);
    }

    private void CollectKeysByPrefix(byte[] prefixBytes, ISet<string> keysToDelete)
    {
        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, _) =>
        {
            var storageKey = Encoding.UTF8.GetString(key);
            keysToDelete.Add(storageKey);
        });
    }

    private async ValueTask UpsertAsync<TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>> session,
                                                                     StorageKey key,
                                                                     string value,
                                                                     CancellationToken cancellationToken)
        where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        await UpsertIntoSessionAsync(session, key, default!, value, cancellationToken);
    }

    private async ValueTask DeleteIfExistsAsync<TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>> session,
                                                                            StorageKey key,
                                                                            CancellationToken cancellationToken)
        where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        await DeleteFromSessionAsync(session, key, cancellationToken);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
    }
}
