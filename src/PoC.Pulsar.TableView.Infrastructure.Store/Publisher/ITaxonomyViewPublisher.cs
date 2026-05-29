using PoC.Pulsar.TableView.Contracts;
using System.Collections.Generic;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Publisher;

public interface ITaxonomyViewPublisher
{
    ValueTask PublishAsync(GeoTaxonomyViewMessage taxonomy, CancellationToken cancellationToken);
    ValueTask PublishListMessage(IEnumerable<GeoTaxonomyViewMessage> taxonomies, CancellationToken cancellationToken);
    ValueTask PublishDeleteMessageAsync(string sportId, DateTimeOffset eventTimestamp, CancellationToken cancellationToken);
}
