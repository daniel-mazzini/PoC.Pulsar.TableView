using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public sealed class GeoTaxonomyBuildUnitOfWork : TsavoriteUnitOfWorkBase, IGeoTaxonomyBuildUnitOfWork
{
    public GeoTaxonomyBuildUnitOfWork(ITsavoriteEngine engine,
                                      IStateSerializer stateSerializer,
                                      ICategoryPendingIndex pendingIndex,
                                      IGeoTaxonomyViewStorage materializeViewStorage)
        : base(engine)
    {
        CategoryRelationIndex = new TsavoriteCategoryRelationIndex(SessionWrapper, stateSerializer);
        CategoryPendingIndex = pendingIndex;
        MaterializeViewStorage = materializeViewStorage;
    }

    public ICategoryRelationIndex CategoryRelationIndex { get; }

    public ICategoryPendingIndex CategoryPendingIndex { get; }

    public IGeoTaxonomyViewStorage MaterializeViewStorage { get; }
}
