using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public sealed record GeoTaxonomyViewUpsertResult
{
    public required SportId SportId { get; init; }
    public required long CalculatedVersion { get; init; }
    public required long PublishedVersion { get; init; }
    public required string BuildGenerationId { get; init; }
    public required GeoTaxonomyViewMessage View { get; init; }
    public bool HasPendingPublish => CalculatedVersion > PublishedVersion;
}
