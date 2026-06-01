using System.Collections.Generic;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public class UnitOfWorkFactory : IUnitOfWorkFactory
{
    private readonly ITsavoriteEngine _engine;
    private readonly IMetadataStorage _metadataStorage;
    private readonly IStateSerializer _stateSerializer;
    private readonly IReadOnlyDictionary<Type, Func<object>> _bootstrapFactories;

    public UnitOfWorkFactory(ITsavoriteEngine engine, IMetadataStorage metadataStorage, IStateSerializer stateSerializer)
    {
        _engine = engine;
        this._metadataStorage = metadataStorage;
        _stateSerializer = stateSerializer;

        _bootstrapFactories = new Dictionary<Type, Func<object>>
        {
            [typeof(SportMessage)] = () => new SportTableViewUnitOfWork(_engine, _metadataStorage, _stateSerializer),
            [typeof(RawCategoryMessage)] = () => new RawCategoryTableViewUnitOfWork(_engine, _metadataStorage, _stateSerializer)
        };
    }

    public ITableViewUnitOfWork<TMessage> CreateBootstrap<TMessage>()
    {
        if (!_bootstrapFactories.TryGetValue(typeof(TMessage), out var factory))
        {
            throw new NotSupportedException($"Unsupported message type {typeof(TMessage).FullName}");
        }

        return (ITableViewUnitOfWork<TMessage>)factory();
    }

    public async Task MoveDurableAsync(CancellationToken cancellationToken)
    {
        await _engine.CheckpointAsync(cancellationToken);
    }
}
