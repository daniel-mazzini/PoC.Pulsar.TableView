using System.Text;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class TsavoriteGeoTaxonomyViewStorage : TsavoriteRepositoryBase, IGeoTaxonomyViewStorage, IDisposable
{
    private static readonly byte[] ViewPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.CountryTaxonomyMaterializedViewPrefix);
    private static readonly byte[] MetadataPrefixBytes = Encoding.UTF8.GetBytes(StorageKey.GeoTaxonomyViewMetadataPrefix);

    private readonly ITsavoriteSessionProvider _sessionProvider;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private bool _disposed;

    public TsavoriteGeoTaxonomyViewStorage(IStateSession session, IStateSerializer serializer)
        : base(serializer)
    {
        ArgumentNullException.ThrowIfNull(session);

        _sessionProvider = (ITsavoriteSessionProvider)session;
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var currentView = await ReadViewAsync(sportId, cancellationToken);

            if (currentView is not null &&
                currentView.SportName == sportName &&
                currentView.SportType == sportType)
            {
                return GeoTaxonomyViewMutationResult.Unchanged(currentView);
            }

            var candidateView = currentView ?? GeoTaxonomyViewMessage.CreateNew(sportId.Value, sportName, sportType);
            var updatedView = candidateView with { SportName = sportName, SportType = sportType };
            var versioned = await SaveVersionedViewAsync(sportId, updatedView, cancellationToken);
            return GeoTaxonomyViewMutationResult.ChangedView(versioned.View);
        }, cancellationToken);

    public ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(() => SaveVersionedViewAsync(sportId, view, buildGenerationId, cancellationToken), cancellationToken);

    public ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var currentMetadata = await ReadMetadataAsync(sportId, cancellationToken) ?? throw new InvalidOperationException($"Missing geo taxonomy metadata for sport '{sportId.Value}'.");

            if (calculatedVersion < currentMetadata.PublishedVersion)
            {
                return;
            }

            if (calculatedVersion > currentMetadata.CalculatedVersion)
            {
                throw new InvalidOperationException(
                    $"Cannot mark calculated version {calculatedVersion} as published for sport '{sportId.Value}' because current calculated version is {currentMetadata.CalculatedVersion}.");
            }

            if (buildGenerationId != currentMetadata.BuildGenerationId && calculatedVersion < currentMetadata.CalculatedVersion)
            {
                return;
            }

            var updatedMetadata = currentMetadata with
            {
                PublishedVersion = calculatedVersion,
                PublishedAtUtc = DateTimeOffset.UtcNow
            };

            await PersistMetadataAsync(sportId, updatedMetadata, cancellationToken);
        }, cancellationToken);

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var currentView = await ReadViewAsync(sportId, cancellationToken);
            if (currentView is null)
            {
                return GeoTaxonomyViewMutationResult.Missing();
            }

            var existing = currentView.GeoCategories.FirstOrDefault(category => category.CategoryId == node.CategoryId);
            if (existing is not null && existing.Equals(node))
            {
                return GeoTaxonomyViewMutationResult.Unchanged(currentView);
            }

            var categoriesToRemove = currentView.GeoCategories.Where(category => category.CategoryId == node.CategoryId);
            var updatedView = currentView with { GeoCategories = currentView.GeoCategories.Except(categoriesToRemove).Add(node) };
            var versioned = await SaveVersionedViewAsync(sportId, updatedView, cancellationToken);
            return GeoTaxonomyViewMutationResult.ChangedView(versioned.View);
        }, cancellationToken);

    public ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var currentView = await ReadViewAsync(sportId, cancellationToken);
            if (currentView is null)
            {
                return GeoTaxonomyViewMutationResult.Missing();
            }

            var categoriesToRemove = currentView.GeoCategories.Where(category => category.CategoryId == categoryId.Value).ToArray();
            if (categoriesToRemove.Length == 0)
            {
                return GeoTaxonomyViewMutationResult.Unchanged(currentView);
            }

            var updatedView = currentView with { GeoCategories = currentView.GeoCategories.Except(categoriesToRemove) };
            var versioned = await SaveVersionedViewAsync(sportId, updatedView, cancellationToken);
            return GeoTaxonomyViewMutationResult.ChangedView(versioned.View);
        }, cancellationToken);

    public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken)
        => ReadViewAsync(sportId, cancellationToken);

    public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var currentView = await ReadViewAsync(sportId, cancellationToken);
            await DeleteViewAsync(sportId, cancellationToken);
            await DeleteMetadataAsync(sportId, cancellationToken);
            return currentView;
        }, cancellationToken);

    public ValueTask ClearAsync(CancellationToken cancellationToken)
        => ExecuteMutatingAsync(async () =>
        {
            var keysToDelete = new HashSet<string>(StringComparer.Ordinal);
            CollectKeysByPrefix(ViewPrefixBytes, keysToDelete);
            CollectKeysByPrefix(MetadataPrefixBytes, keysToDelete);

            foreach (var key in keysToDelete)
            {
                await DeleteIfExistsAsync(StorageKey.Create(key), cancellationToken);
            }
        }, cancellationToken);

    public ValueTask<GeoTaxonomyViewMetadata?> GetMetadataAsync(SportId sportId, CancellationToken cancellationToken)
        => ReadMetadataAsync(sportId, cancellationToken);

    private async ValueTask<TResult> ExecuteMutatingAsync<TResult>(Func<ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        await _mutationGate.WaitAsync(cancellationToken);
        try
        {
            return await action();
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask ExecuteMutatingAsync(Func<ValueTask> action, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        await _mutationGate.WaitAsync(cancellationToken);
        try
        {
            await action();
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private async ValueTask<GeoTaxonomyViewMessage?> ReadViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<GeoTaxonomyViewMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                        StorageKey.CountryTaxonomyMaterializedView(sportId),
                                                                                                                        cancellationToken);
    }

    private async ValueTask<GeoTaxonomyViewMetadata?> ReadMetadataAsync(SportId sportId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var session = _sessionProvider.GetLightSession();
        return await ReadFromSessionAsync<GeoTaxonomyViewMetadata, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                          StorageKey.GeoTaxonomyViewMetadata(sportId),
                                                                                                                          cancellationToken);
    }

    private async ValueTask<GeoTaxonomyViewUpsertResult> SaveVersionedViewAsync(SportId sportId, GeoTaxonomyViewMessage candidateView, CancellationToken cancellationToken)
    {
        var existingMetadata = await ReadMetadataAsync(sportId, cancellationToken);
        var buildGenerationId = existingMetadata?.BuildGenerationId ?? $"build-{Guid.CreateVersion7():N}";
        return await SaveVersionedViewAsync(sportId, candidateView, buildGenerationId, existingMetadata, cancellationToken);
    }

    private async ValueTask<GeoTaxonomyViewUpsertResult> SaveVersionedViewAsync(SportId sportId,
                                                                                GeoTaxonomyViewMessage candidateView,
                                                                                string buildGenerationId,
                                                                                CancellationToken cancellationToken)
    {
        var existingMetadata = await ReadMetadataAsync(sportId, cancellationToken);
        return await SaveVersionedViewAsync(sportId, candidateView, buildGenerationId, existingMetadata, cancellationToken);
    }

    private async ValueTask<GeoTaxonomyViewUpsertResult> SaveVersionedViewAsync(SportId sportId,
                                                                                GeoTaxonomyViewMessage candidateView,
                                                                                string buildGenerationId,
                                                                                GeoTaxonomyViewMetadata? existingMetadata,
                                                                                CancellationToken cancellationToken)
    {
        long previousCalculatedVersion = existingMetadata?.CalculatedVersion ?? 0;
        long previousPublishedVersion = existingMetadata?.PublishedVersion ?? 0;
        DateTimeOffset? previousPublishedAtUtc = existingMetadata?.PublishedAtUtc;
        long nextVersion = Math.Max(previousCalculatedVersion, previousPublishedVersion) + 1;

        var versionedView = candidateView with { Version = checked((int)nextVersion) };
        var updatedMetadata = new GeoTaxonomyViewMetadata
        {
            CalculatedVersion = nextVersion,
            PublishedVersion = previousPublishedVersion,
            BuildGenerationId = buildGenerationId,
            UpdatedAtUtc = DateTimeOffset.UtcNow,
            PublishedAtUtc = previousPublishedAtUtc
        };

        await PersistViewAsync(sportId, versionedView, cancellationToken);
        await PersistMetadataAsync(sportId, updatedMetadata, cancellationToken);

        return new GeoTaxonomyViewUpsertResult
        {
            SportId = sportId,
            CalculatedVersion = nextVersion,
            PublishedVersion = previousPublishedVersion,
            BuildGenerationId = buildGenerationId,
            View = versionedView
        };
    }

    private async ValueTask PersistViewAsync(SportId sportId, GeoTaxonomyViewMessage view, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync<GeoTaxonomyViewMessage, SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session,
                                                                                                                      StorageKey.CountryTaxonomyMaterializedView(sportId),
                                                                                                                      default!,
                                                                                                                      view,
                                                                                                                      cancellationToken);
    }

    private async ValueTask PersistMetadataAsync(SportId sportId, GeoTaxonomyViewMetadata metadata, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        await UpsertIntoSessionAsync(session,
                                     StorageKey.GeoTaxonomyViewMetadata(sportId),
                                     default!,
                                     metadata,
                                     cancellationToken);
    }

    private async ValueTask DeleteViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        var currentView = await ReadViewAsync(sportId, cancellationToken);
        if (currentView is null)
        {
            return;
        }

        await DeleteIfExistsAsync(StorageKey.CountryTaxonomyMaterializedView(sportId), cancellationToken);
    }

    private async ValueTask DeleteMetadataAsync(SportId sportId, CancellationToken cancellationToken)
    {
        var currentMetadata = await ReadMetadataAsync(sportId, cancellationToken);
        if (currentMetadata is null)
        {
            return;
        }

        await DeleteIfExistsAsync(StorageKey.GeoTaxonomyViewMetadata(sportId), cancellationToken);
    }

    private async ValueTask DeleteIfExistsAsync(StorageKey storageKey, CancellationToken cancellationToken)
    {
        var session = _sessionProvider.GetLightSession();
        await DeleteFromSessionAsync<SpanByte, SpanByteAndMemory, SpanByteFunctions<Empty>>(session, storageKey, cancellationToken);
    }

    private void CollectKeysByPrefix(byte[] prefixBytes, ISet<string> keysToDelete)
    {
        _sessionProvider.Engine.ScanByPrefix(prefixBytes, (key, _) =>
        {
            keysToDelete.Add(Encoding.UTF8.GetString(key));
        });
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

        _mutationGate.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
