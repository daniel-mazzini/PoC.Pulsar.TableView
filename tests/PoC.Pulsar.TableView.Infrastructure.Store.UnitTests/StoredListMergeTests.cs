using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class StoredListMergeTests
{
    [Fact]
    public void merge_guid_should_append_missing_id_and_keep_existing_sequence()
    {
        var existing = Guid.NewGuid();
        var added = Guid.NewGuid();

        var result = StoredListMerge.Merge([existing], added);

        Assert.Equal([existing, added], result);
    }

    [Fact]
    public void merge_guid_should_not_duplicate_existing_id()
    {
        var existing = Guid.NewGuid();

        var result = StoredListMerge.Merge([existing], existing);

        Assert.Equal([existing], result);
    }

    [Fact]
    public void merge_string_should_use_ordinal_comparison()
    {
        var result = StoredListMerge.Merge(["sport"], "SPORT");

        Assert.Equal(["sport", "SPORT"], result);
    }

    [Fact]
    public void merge_int_should_not_duplicate_existing_id()
    {
        var result = StoredListMerge.Merge([1, 2], 2);

        Assert.Equal([1, 2], result);
    }

    [Fact]
    public void remove_guid_should_remove_all_matching_ids()
    {
        var removed = Guid.NewGuid();
        var kept = Guid.NewGuid();

        var result = StoredListMerge.Remove([removed, kept, removed], removed);

        Assert.Equal([kept], result);
    }

    [Fact]
    public void remove_string_should_use_ordinal_comparison()
    {
        var result = StoredListMerge.Remove(["sport", "SPORT"], "sport");

        Assert.Equal(["SPORT"], result);
    }

    [Fact]
    public void remove_int_should_keep_non_matching_ids()
    {
        var result = StoredListMerge.Remove([1, 2, 3], 2);

        Assert.Equal([1, 3], result);
    }
}
