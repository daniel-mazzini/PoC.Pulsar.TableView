using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class StorageKeyEncodingTests
{
    [Fact]
    public void dynamic_key_segments_should_not_collide_when_ids_contain_colon_or_dash()
    {
        Assert.NotEqual(StorageKey.SportMessage("sport:live"), StorageKey.SportMessage("sport-live"));
        Assert.NotEqual(StorageKey.CategoryMessage("soccer:int"), StorageKey.CategoryMessage("soccer-int"));
        Assert.NotEqual(StorageKey.TopicCheckpoint("persistent://tenant/ns/sports"), StorageKey.TopicCheckpoint("persistent---tenant-ns-sports"));
        Assert.NotEqual(StorageKey.RejectedRecord("record:1"), StorageKey.RejectedRecord("record-1"));
    }

    [Fact]
    public void pending_index_key_segments_should_not_collide_when_ids_contain_colon_or_dash()
    {
        var sportWithColon = new SportId("sport:live");
        var sportWithDash = new SportId("sport-live");
        var categoryWithColon = new CategoryId("soccer:int");
        var categoryWithDash = new CategoryId("soccer-int");

        Assert.NotEqual(StorageKey.OrphanCategoryBySport(sportWithColon, categoryWithColon),
                        StorageKey.OrphanCategoryBySport(sportWithDash, categoryWithColon));
        Assert.NotEqual(StorageKey.OrphanCategoryBySport(sportWithColon, categoryWithColon),
                        StorageKey.OrphanCategoryBySport(sportWithColon, categoryWithDash));
        Assert.NotEqual(StorageKey.OrphanSportByCategory(categoryWithColon, sportWithColon),
                        StorageKey.OrphanSportByCategory(categoryWithDash, sportWithColon));
        Assert.NotEqual(StorageKey.OrphanSportByCategory(categoryWithColon, sportWithColon),
                        StorageKey.OrphanSportByCategory(categoryWithColon, sportWithDash));
    }
}
