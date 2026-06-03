using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class InMemoryGeoTaxonomyViewStorageTests
{
    [Fact]
    public async Task upsert_view_async_should_assign_initial_calculated_version_and_leave_published_version_unset()
    {
        var storage = new InMemoryGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        var result = await storage.UpsertViewAsync(sportId,
                                                   GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                                   "build-00000000000000000000000000000001",
                                                   CancellationToken.None);

        Assert.Equal(1L, result.CalculatedVersion);
        Assert.Equal(0L, result.PublishedVersion);
        Assert.True(result.HasPendingPublish);
        Assert.Equal(1, result.View.Version);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(1L, metadata!.CalculatedVersion);
        Assert.Equal(0L, metadata.PublishedVersion);
        Assert.True(metadata.HasPendingPublish);
    }

    [Fact]
    public async Task mark_view_published_async_should_advance_published_version_only_after_confirmation()
    {
        var storage = new InMemoryGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        var result = await storage.UpsertViewAsync(sportId,
                                                   GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                                   "build-00000000000000000000000000000001",
                                                   CancellationToken.None);

        await storage.MarkViewPublishedAsync(sportId, result.CalculatedVersion, result.BuildGenerationId, CancellationToken.None);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(1L, metadata!.PublishedVersion);
        Assert.False(metadata.HasPendingPublish);
        Assert.NotNull(metadata.PublishedAtUtc);
    }

    [Fact]
    public async Task second_upsert_view_async_should_increment_calculated_version_and_preserve_last_published_version()
    {
        var storage = new InMemoryGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");
        var first = await storage.UpsertViewAsync(sportId,
                                                  GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                                  "build-00000000000000000000000000000001",
                                                  CancellationToken.None);
        await storage.MarkViewPublishedAsync(sportId, first.CalculatedVersion, first.BuildGenerationId, CancellationToken.None);

        var second = await storage.UpsertViewAsync(sportId,
                                                   GeoTaxonomyViewMessage.Create(Sport("sport-1", "Soccer Updated", "SOCCER"), []),
                                                   "build-00000000000000000000000000000002",
                                                   CancellationToken.None);

        Assert.Equal(2L, second.CalculatedVersion);
        Assert.Equal(1L, second.PublishedVersion);
        Assert.True(second.HasPendingPublish);
        Assert.Equal(2, second.View.Version);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.Equal(2L, metadata!.CalculatedVersion);
        Assert.Equal(1L, metadata.PublishedVersion);
        Assert.True(metadata.HasPendingPublish);
    }

    [Fact]
    public async Task upsert_view_async_without_publish_confirmation_should_leave_pending_publish_metadata()
    {
        var storage = new InMemoryGeoTaxonomyViewStorage();
        var sportId = new SportId("sport-1");

        await storage.UpsertViewAsync(sportId,
                                      GeoTaxonomyViewMessage.Create(Sport("sport-1"), []),
                                      "build-00000000000000000000000000000001",
                                      CancellationToken.None);

        var metadata = await storage.GetMetadataAsync(sportId, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.True(metadata!.HasPendingPublish);
        Assert.Equal(1L, metadata.CalculatedVersion);
        Assert.Equal(0L, metadata.PublishedVersion);
        Assert.Null(metadata.PublishedAtUtc);
    }

    private static SportMessage Sport(string id, string name = "Soccer", string sportType = "SOCCER")
        => new()
        {
            Id = id,
            Name = name,
            SportType = sportType,
            Provider = "provider",
            EntityCoverage = "covered"
        };
}
