using PoC.Pulsar.TableView.Domain.Rejected;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal sealed class NoOpRejectedMessagePublisher : IRejectedMessagePublisher
{
    public Task PublishAsync<TMessage>(Rejected<TMessage> rejected, Dictionary<string, string> headers, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
