using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.Entities;

namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public interface ITableViewUnitOfWork<TMessage> : IUnitOfWork
{
    IMessageStorage<string,TMessage> MessageStorage { get; }
    ICheckpointStorage CheckpointStorage { get; }
}



