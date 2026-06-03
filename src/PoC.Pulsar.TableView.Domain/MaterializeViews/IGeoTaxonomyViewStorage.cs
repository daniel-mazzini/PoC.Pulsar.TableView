using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public readonly record struct GeoTaxonomyViewMutationResult(bool ViewExists, bool Changed, GeoTaxonomyViewMessage? View)
{
    public static GeoTaxonomyViewMutationResult Missing()
        => new(false, false, null);

    public static GeoTaxonomyViewMutationResult Unchanged(GeoTaxonomyViewMessage view)
        => new(true, false, view);

    public static GeoTaxonomyViewMutationResult ChangedView(GeoTaxonomyViewMessage view)
        => new(true, true, view);
}

public sealed record GeoTaxonomyViewMetadata
{
    public required long CalculatedVersion { get; init; }
    public required long PublishedVersion { get; init; }
    public required string BuildGenerationId { get; init; }
    public DateTimeOffset UpdatedAtUtc { get; init; }
    public DateTimeOffset? PublishedAtUtc { get; init; }
    public bool HasPendingPublish => CalculatedVersion > PublishedVersion;
}

public sealed record GeoTaxonomyViewUpsertResult
{
    public required SportId SportId { get; init; }
    public required long CalculatedVersion { get; init; }
    public required long PublishedVersion { get; init; }
    public required string BuildGenerationId { get; init; }
    public required GeoTaxonomyViewMessage View { get; init; }
    public bool HasPendingPublish => CalculatedVersion > PublishedVersion;
}

public interface IGeoTaxonomyViewStorage
{
    ValueTask<GeoTaxonomyViewMutationResult> UpsertSportAsync(SportId sportId, string sportName, string sportType, CancellationToken cancellationToken);
    ValueTask<GeoTaxonomyViewUpsertResult> UpsertViewAsync(SportId sportId, GeoTaxonomyViewMessage view, string buildGenerationId, CancellationToken cancellationToken);
    ValueTask MarkViewPublishedAsync(SportId sportId, long calculatedVersion, string buildGenerationId, CancellationToken cancellationToken);

    ValueTask<GeoTaxonomyViewMutationResult> UpsertCategoryAsync(SportId sportId, GeoTaxonomyNode node, CancellationToken cancellationToken);

    ValueTask<GeoTaxonomyViewMutationResult> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);

    ValueTask<GeoTaxonomyViewMessage?> GetViewAsync(SportId sportId, CancellationToken cancellationToken);
    ValueTask<GeoTaxonomyViewMessage?> RemoveViewAsync(SportId sportId, CancellationToken cancellationToken);

    ValueTask ClearAsync(CancellationToken cancellationToken);
}
