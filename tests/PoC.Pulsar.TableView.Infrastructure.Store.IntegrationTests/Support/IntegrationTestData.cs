using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.TableView;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal static class IntegrationTestData
{
    public static SportMessage Sport(string id, int version = 1)
        => new()
        {
            Id = id,
            Name = $"sport-{id}",
            Provider = "provider",
            EntityCoverage = "coverage",
            SportType = "sport",
            Version = version
        };

    public static RawCategoryMessage Category(string id, string sportId, string? parentId = null, int version = 1)
        => new()
        {
            Id = id,
            Name = $"category-{id}",
            Provider = "provider",
            EntityCoverage = "coverage",
            SportId = sportId,
            ParentId = parentId,
            SportType = "sport",
            CountryCode = "AR",
            Gender = "mixed",
            Version = version
        };

    public static GeoTaxonomyViewMessage TaxonomyView(string sportId)
        => GeoTaxonomyViewMessage.CreateNew(sportId, $"sport-{sportId}", "sport") with
        {
            Timestamp = DateTimeOffset.UtcNow
        };

    public static RejectedProjection RejectedProjection(string messageKey)
        => new(messageKey,
               "persistent://public/tableview-inputs/sports",
               0,
               new RejectedReason("invalid", "invalid payload"),
               DateTimeOffset.UtcNow);

    public static PulsarMessageId PulsarMessageId(long ledgerId = 1, long entryId = 1, int partition = 0, int batchIndex = 0)
        => new(ledgerId, entryId, partition, batchIndex);
}
