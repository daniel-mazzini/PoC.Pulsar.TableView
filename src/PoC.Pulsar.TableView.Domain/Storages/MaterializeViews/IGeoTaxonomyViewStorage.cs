using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities.Categories;
using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Storages.MaterializeViews;

public interface IGeoTaxonomyViewStorage
{
    void AddTaxonomyView(SportId id, GeoTaxonomyViewMessage view);
    ValueTask<GeoTaxonomyViewMessage> RemoveCategoryAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);
    GeoTaxonomyViewMessage? RemoveView(SportId id);
}
