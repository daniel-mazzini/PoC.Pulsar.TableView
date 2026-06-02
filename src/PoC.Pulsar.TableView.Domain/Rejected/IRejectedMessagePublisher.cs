using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.Rejected;

public interface IRejectedMessagePublisher
{
    Task PublishAsync<TMessage>(RejectedMessage<TMessage> write, Dictionary<string,string> headers, CancellationToken cancellationToken);
}

