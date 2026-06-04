using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

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
