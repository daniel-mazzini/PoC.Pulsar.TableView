namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public sealed record GeoTaxonomyViewMetadata
{
    public required long CalculatedVersion { get; init; }
    public required long PublishedVersion { get; init; }
    public required string BuildGenerationId { get; init; }
    public DateTimeOffset UpdatedAtUtc { get; init; }
    public DateTimeOffset? PublishedAtUtc { get; init; }
    public bool HasPendingPublish => CalculatedVersion > PublishedVersion;
}
