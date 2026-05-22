using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Processor;

public interface ITaxonomyViewPublisher
{
    ValueTask PublishAsync(GeoTaxonomyMessage taxonomy, CancellationToken cancellationToken);
    ValueTask PublishDeleteMessageAsync(string sportId, CancellationToken cancellationToken);
}
