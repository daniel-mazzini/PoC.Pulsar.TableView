using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using System.Buffers;

namespace PoC.Pulsar.TableView.Domain.Entities;

public interface IProjectorMessageApplier<TMessage>
{
    ValueTask<ProjectionApplyResult> ApplyAsync(TableViewMessage input, ProcessPhase processPhase, ITableViewUnitOfWork<TMessage> tableViewUnitOfWork, Func<ReadOnlySequence<byte>, TMessage> serialize,  CancellationToken cancellationToken);
}
