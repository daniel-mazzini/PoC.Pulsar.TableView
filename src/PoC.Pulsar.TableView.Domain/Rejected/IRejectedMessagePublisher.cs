namespace PoC.Pulsar.TableView.Domain.Rejected;

public interface IRejectedMessagePublisher
{
    Task PublishAsync<TMessage>(Rejected<TMessage> rejected, Dictionary<string,string> headers, CancellationToken cancellationToken);
}

