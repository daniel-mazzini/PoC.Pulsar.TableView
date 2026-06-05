using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Sports;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

public sealed class InMemoryGeoTaxonomyViewStorage : IGeoTaxonomyViewStorage
{
    private readonly ConcurrentDictionary<SportId, GeoTaxonomyViewMessage> _views = [];
    private readonly ConcurrentDictionary<SportId, GeoTaxonomyViewMetadata> _metadata = [];

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.Clear();
        _metadata.Clear();
        return ValueTask.CompletedTask;
    }

    public ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.TryGetValue(sportId, out var view);
        return ValueTask.FromResult(view);
    }

    public ValueTask<GeoTaxonomyViewMetadata?> GetMetadataAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _metadata.TryGetValue(sportId, out var metadata);
        return ValueTask.FromResult(metadata);
    }

    public ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var categoriesToRemove = view.GeoCategories.Where(category => category.CategoryId == categoryId.Value).ToArray();

        if (categoriesToRemove.Length == 0)
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var updated = view with { GeoCategories = view.GeoCategories.Except(categoriesToRemove) };
        var versioned = SaveVersionedView(sportId, updated, GetCurrentBuildGenerationId(sportId));
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(versioned.View));
    }

    public ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _views.TryRemove(sportId, out var view);
        _metadata.TryRemove(sportId, out _);
        return ValueTask.FromResult(view);
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_views.TryGetValue(sportId, out var view))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Missing());
        }

        var existing = view.GeoCategories.FirstOrDefault(category => category.CategoryId == node.CategoryId);

        if (existing is not null && existing.Equals(node))
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(view));
        }

        var categoriesToRemove = view.GeoCategories.Where(category => category.CategoryId == node.CategoryId);
        var updated = view with { GeoCategories = view.GeoCategories.Except(categoriesToRemove).Add(node) };

        var versioned = SaveVersionedView(sportId, updated, GetCurrentBuildGenerationId(sportId));
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(versioned.View));
    }

    public ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _ = StorageKey.CountryTaxonomyMaterializedView(sportId);

        var currentView = _views.TryGetValue(sportId, out var existing)
            ? existing
            : GeoTaxonomyViewMessage.CreateNew(sportId.Value, sportName, sportType);

        if (currentView.SportName == sportName && currentView.SportType == sportType)
        {
            return ValueTask.FromResult(GeoTaxonomyViewMutationResult.Unchanged(currentView));
        }

        var updated = currentView with { SportName = sportName, SportType = sportType };
        var versioned = SaveVersionedView(sportId, updated, GetCurrentBuildGenerationId(sportId));
        return ValueTask.FromResult(GeoTaxonomyViewMutationResult.ChangedView(versioned.View));
    }

    public ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _ = StorageKey.CountryTaxonomyMaterializedView(sportId);
        _ = StorageKey.GeoTaxonomyViewMetadata(sportId);
        return ValueTask.FromResult(SaveVersionedView(sportId, view, buildGenerationId));
    }

    public ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_metadata.TryGetValue(sportId, out var currentMetadata))
        {
            throw new InvalidOperationException($"Missing geo taxonomy metadata for sport '{sportId.Value}'.");
        }

        if (calculatedVersion < currentMetadata.PublishedVersion)
        {
            return ValueTask.CompletedTask;
        }

        if (calculatedVersion > currentMetadata.CalculatedVersion)
        {
            throw new InvalidOperationException(
                $"Cannot mark calculated version {calculatedVersion} as published for sport '{sportId.Value}' because current calculated version is {currentMetadata.CalculatedVersion}.");
        }

        if (buildGenerationId != currentMetadata.BuildGenerationId && calculatedVersion < currentMetadata.CalculatedVersion)
        {
            return ValueTask.CompletedTask;
        }

        _metadata[sportId] = currentMetadata with
        {
            PublishedVersion = calculatedVersion,
            PublishedAtUtc = DateTimeOffset.UtcNow
        };

        return ValueTask.CompletedTask;
    }

    private GeoTaxonomyViewUpsertResult SaveVersionedView(SportId sportId, GeoTaxonomyViewMessage candidateView, string buildGenerationId)
    {
        _metadata.TryGetValue(sportId, out var existingMetadata);

        long previousCalculatedVersion = existingMetadata?.CalculatedVersion ?? 0;
        long previousPublishedVersion = existingMetadata?.PublishedVersion ?? 0;
        DateTimeOffset? previousPublishedAtUtc = existingMetadata?.PublishedAtUtc;
        long nextVersion = Math.Max(previousCalculatedVersion, previousPublishedVersion) + 1;

        var versionedView = candidateView with { Version = checked((int)nextVersion) };

        _views[sportId] = versionedView;
        _metadata[sportId] = new GeoTaxonomyViewMetadata
        {
            CalculatedVersion = nextVersion,
            PublishedVersion = previousPublishedVersion,
            BuildGenerationId = buildGenerationId,
            UpdatedAtUtc = DateTimeOffset.UtcNow,
            PublishedAtUtc = previousPublishedAtUtc
        };

        return new GeoTaxonomyViewUpsertResult
        {
            SportId = sportId,
            CalculatedVersion = nextVersion,
            PublishedVersion = previousPublishedVersion,
            BuildGenerationId = buildGenerationId,
            View = versionedView
        };
    }

    private string GetCurrentBuildGenerationId(SportId sportId)
        => _metadata.TryGetValue(sportId, out var metadata)
            ? metadata.BuildGenerationId
            : $"build-{Guid.CreateVersion7():N}";
}
