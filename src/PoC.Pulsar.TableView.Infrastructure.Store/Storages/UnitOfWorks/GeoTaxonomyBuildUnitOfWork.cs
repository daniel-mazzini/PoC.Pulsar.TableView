using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public sealed class GeoTaxonomyBuildUnitOfWork : TsavoriteUnitOfWorkBase, IGeoTaxonomyBuildUnitOfWork
{
    public GeoTaxonomyBuildUnitOfWork(ITsavoriteEngine engine,
                                      IMetadataStorage metadataStorage,
                                      IStateSerializer stateSerializer)
        : base(engine)
    {
        CategoryRelationIndex = new DefaultCategoryRelationIndex(SessionWrapper, stateSerializer);
        CategoryPendingIndex = new DefaultCategoryPendingIndex(SessionWrapper, stateSerializer);
        MaterializeViewStorage = new DefaultGeoTaxonomyViewStorage(SessionWrapper, stateSerializer);
        CheckpointStorage = new CheckpointStorage(SessionWrapper, stateSerializer, metadataStorage);
    }

    public ICategoryRelationIndex CategoryRelationIndex { get; }

    public ICategoryPendingIndex CategoryPendingIndex { get; }

    public IGeoTaxonomyViewStorage MaterializeViewStorage { get; }

    public ICheckpointStorage CheckpointStorage { get; }
}
