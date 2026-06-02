using PoC.Pulsar.TableView.Domain.Checkpoints;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Storages.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public interface ITableViewUnitOfWork<TMessage> : IUnitOfWork
{
    IMessageStorage<string,TMessage> MessageStorage { get; }
    ICheckpointStorage CheckpointStorage { get; }

    IRejectedStorage RejectedStorage { get; }
}



