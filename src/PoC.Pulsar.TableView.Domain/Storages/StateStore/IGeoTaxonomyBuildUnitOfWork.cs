using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;

namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public interface IGeoTaxonomyBuildUnitOfWork : IUnitOfWork
{
    ICategoryRelationIndex CategoryRelationIndex { get; }
    ICategoryPendingIndex CategoryPendingIndex { get; }
    IGeoTaxonomyViewStorage MaterializeViewStorage { get; }
}
