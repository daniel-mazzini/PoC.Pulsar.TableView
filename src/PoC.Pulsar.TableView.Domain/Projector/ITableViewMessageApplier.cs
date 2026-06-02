using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using System.Buffers;

namespace PoC.Pulsar.TableView.Domain.Projector;

public interface ITableViewMessageApplier<TMessage>
{
    ValueTask<TableMessageApplyResult<TMessage>> ApplyAsync(TableViewMessage input, ProcessPhase processPhase, ITableViewUnitOfWork<TMessage> tableViewUnitOfWork, Func<ReadOnlySequence<byte>, TMessage> serialize,  CancellationToken cancellationToken);
}

