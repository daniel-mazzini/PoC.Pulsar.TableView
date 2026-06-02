using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public interface IGeoTaxonomyViewStorage
{
    void AddTaxonomyView(SportId id, GeoTaxonomyViewMessage view);
    GeoTaxonomyViewMessage? AddCategoryAsync(string sportId, GeoTaxonomyNode node, CancellationToken cancellationToken);
    ValueTask ClearAsync(CancellationToken cancellationToken);
    GeoTaxonomyViewMessage? TryGetView(SportId id);
    ValueTask<GeoTaxonomyViewMessage?> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);
    GeoTaxonomyViewMessage? RemoveView(SportId id);
}
