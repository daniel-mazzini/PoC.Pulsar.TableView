using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public interface ITaxonomyViewPublisher
{
    ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken);
    ValueTask PublishListMessage(IEnumerable<GeoTaxonomyViewMessage> taxonomies, CancellationToken cancellationToken);
    ValueTask PublishDeleteMessageAsync(string sportId, DateTimeOffset eventTimestamp, CancellationToken cancellationToken);
}
