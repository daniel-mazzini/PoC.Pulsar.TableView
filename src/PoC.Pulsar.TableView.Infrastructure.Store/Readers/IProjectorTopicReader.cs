using PoC.Pulsar.TableView.Domain.TableView;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public interface IProjectorTopicReader : IAsyncDisposable
{
    Task<TableViewMessage> ReceiveAsync(CancellationToken cancellationToken);
    IAsyncEnumerable<TableViewMessage> ReadAllAsync(CancellationToken cancellationToken);
}
