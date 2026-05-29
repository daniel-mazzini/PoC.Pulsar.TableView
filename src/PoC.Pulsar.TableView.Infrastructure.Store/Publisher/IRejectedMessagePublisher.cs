using PoC.Pulsar.TableView.Domain.Entities;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

public interface IRejectedMessagePublisher
{
    Task PublishAsync(RejectedMessageWrite write, CancellationToken cancellationToken);
}

