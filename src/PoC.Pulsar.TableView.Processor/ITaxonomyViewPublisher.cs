using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Processor;

public interface ITaxonomyViewPublisher
{
    ValueTask PublishAsync(GeoTaxonomyMessage taxonomy, CancellationToken cancellationToken = default);
    ValueTask DeleteAsync(string sportId, CancellationToken cancellationToken = default);
}
