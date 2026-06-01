using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public sealed class RawCategoryTableViewUnitOfWork : TsavoriteUnitOfWorkBase, ITableViewUnitOfWork<RawCategoryMessage>
{
    public RawCategoryTableViewUnitOfWork(ITsavoriteEngine engine, IMetadataStorage metadataStorage, IStateSerializer stateSerializer) : base(engine)
    {
        MessageStorage = new CategoryMessageStorage(SessionWrapper, stateSerializer);
        CheckpointStorage = new CheckpointStorage(SessionWrapper, stateSerializer, metadataStorage);
        RejectedStorage = new RejectedStorage(SessionWrapper, stateSerializer);
    }

    public IMessageStorage<string, RawCategoryMessage> MessageStorage { get; }

    public ICheckpointStorage CheckpointStorage { get; }

    public IRejectedStorage RejectedStorage { get; }
}
