using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;

public sealed class SportTableViewUnitOfWork : TsavoriteUnitOfWorkBase, ITableViewUnitOfWork<SportMessage>
{
    public SportTableViewUnitOfWork(ITsavoriteEngine engine, IMetadataStorage metadataStorage, IStateSerializer stateSerializer) : base(engine)
    {
        MessageStorage = new SportMessageStorage(SessionWrapper, stateSerializer);
        CheckpointStorage = new CheckpointStorage(SessionWrapper, stateSerializer, metadataStorage);
        RejectedStorage = new RejectedStorage(SessionWrapper, stateSerializer);
    }

    public IMessageStorage<string, SportMessage> MessageStorage { get; }

    public ICheckpointStorage CheckpointStorage { get; }

    public IRejectedStorage RejectedStorage { get; }
}
