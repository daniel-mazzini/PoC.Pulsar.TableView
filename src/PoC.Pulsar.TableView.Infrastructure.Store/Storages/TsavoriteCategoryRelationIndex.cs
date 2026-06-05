using System.Collections.Generic;
using System.Text;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class TsavoriteCategoryRelationIndex : TsavoriteRepositoryBase, ICategoryRelationIndex, IDisposable
{
    private static readonly byte[] CategoryBySportPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.CategoryBySportIndexPrefix);
    private static readonly byte[] CategoryByParentPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.CategoryByParentIndexPrefix);

    private readonly ITsavoriteSessionProvider _sessionProvider;
    private bool _disposed;

    public TsavoriteCategoryRelationIndex(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);

        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public async ValueTask IndexCategoryAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();

        await UpsertAsync(session, StorageKey.CategoryBySport(current.SportId, current.CategoryId), current.CategoryId.Value, cancellationToken);

        if (current.ParentCategoryId is not null)
        {
            await UpsertAsync(session,
                              StorageKey.CategoryByParent(current.ParentCategoryId.Value, current.CategoryId),
                              current.CategoryId.Value,
                              cancellationToken);
        }
    }

    public async ValueTask ReplaceCategoryRelationsAsync(CategoryRelations? previous, CategoryRelations current, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        if (previous is not null)
        {
            await RemoveObsoleteRelationsAsync(previous.Value, current, cancellationToken);
        }

        await IndexCategoryAsync(current, cancellationToken);
    }

    public async ValueTask RemoveCategoryRelationsAsync(CategoryRelations current, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();

        await DeleteIfExistsAsync(session, StorageKey.CategoryBySport(current.SportId, current.CategoryId), cancellationToken);

        if (current.ParentCategoryId is not null)
        {
            await DeleteIfExistsAsync(session,
                                      StorageKey.CategoryByParent(current.ParentCategoryId.Value, current.CategoryId),
                                      cancellationToken);
        }
    }

    public async ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySportAsync(SportId sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var result = await ReadRelationIdsByPrefixAsync(StorageKey.CategoryBySportPrefix(sportId).Value, CategoryBySportPrefixBytes, cancellationToken);
        return result;
    }

    public async ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var result = await ReadRelationIdsByPrefixAsync(StorageKey.CategoryByParentPrefix(parentCategoryId).Value, CategoryByParentPrefixBytes, cancellationToken);
        return result;
    }

    public async ValueTask<bool> HasCategoryBySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        var value = await ReadFromSessionAsync<string, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                              StorageKey.CategoryBySport(sportId, categoryId),
                                                                                                              cancellationToken);
        return value is not null;
    }

    public async ValueTask<bool> HasCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var session = _sessionProvider.GetLightSession();
        var value = await ReadFromSessionAsync<string, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                              StorageKey.CategoryByParent(parentCategoryId, categoryId),
                                                                                                              cancellationToken);
        return value is not null;
    }

    public async ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var keysToDelete = new HashSet<string>(StringComparer.Ordinal);
        CollectKeysByPrefix(CategoryBySportPrefixBytes, keysToDelete);
        CollectKeysByPrefix(CategoryByParentPrefixBytes, keysToDelete);

        var session = _sessionProvider.GetLightSession();
        foreach (var key in keysToDelete)
        {
            await DeleteIfExistsAsync(session, StorageKey.Create(key), cancellationToken);
        }
    }

    private ValueTask<IReadOnlySet<CategoryId>> ReadRelationIdsByPrefixAsync(string storagePrefix, byte[] prefixBytes, CancellationToken cancellationToken)
    {
        var result = new HashSet<CategoryId>();
        var seenStorageKeys = new HashSet<string>(StringComparer.Ordinal);

        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, value) =>
        {
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

    private void CollectKeysByPrefix(byte[] prefixBytes, ISet<string> keysToDelete)
    {
        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, _) =>
        {
            var storageKey = Encoding.UTF8.GetString(key);
            keysToDelete.Add(storageKey);
        });
    }

    private async ValueTask RemoveObsoleteRelationsAsync(CategoryRelations previous, CategoryRelations current, CancellationToken cancellationToken)
    {
        var lightSession = _sessionProvider.GetLightSession();

        if (previous.SportId != current.SportId || previous.CategoryId != current.CategoryId)
        {
            await DeleteIfExistsAsync(lightSession, StorageKey.CategoryBySport(previous.SportId, previous.CategoryId), cancellationToken);
        }

        if (previous.ParentCategoryId is not null &&
            (previous.ParentCategoryId != current.ParentCategoryId || previous.CategoryId != current.CategoryId))
        {
            await DeleteIfExistsAsync(lightSession,
                                      StorageKey.CategoryByParent(previous.ParentCategoryId.Value, previous.CategoryId),
                                      cancellationToken);
        }
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
