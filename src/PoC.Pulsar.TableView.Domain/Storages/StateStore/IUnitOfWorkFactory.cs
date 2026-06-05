namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;


public interface IUnitOfWorkFactory
{
    ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>();

    IGeoTaxonomyBuildUnitOfWork CreateGeoTaxonomyBuild();

    Task MoveDurableAsync(CancellationToken cancellationToken);
}



