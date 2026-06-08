using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using Xunit;

namespace PoC.Pulsar.TableView.Infrastructure.Store.UnitTests;

public sealed class PulsarTopicsTests
{
    [Fact]
    public void partition_should_append_partition_suffix()
    {
        var result = PulsarTopics.Partition(PulsarTopics.Sports, 3);

        Assert.Equal("sports-partition-3", result);
    }

    [Fact]
    public void qualify_should_build_persistent_topic_name()
    {
        var result = PulsarTopics.Qualify("public/default", PulsarTopics.Categories);

        Assert.Equal("persistent://public/default/categories", result);
    }

    [Theory]
    [InlineData("persistent://public/default/sports")]
    [InlineData("non-persistent://public/default/sports")]
    public void qualify_if_needed_should_keep_already_qualified_topic_names(string topicName)
    {
        var result = PulsarTopics.QualifyIfNeeded("public/default", topicName);

        Assert.Equal(topicName, result);
    }

    [Fact]
    public void qualify_if_needed_should_qualify_short_topic_name()
    {
        var result = PulsarTopics.QualifyIfNeeded("public/default", PulsarTopics.Sports);

        Assert.Equal("persistent://public/default/sports", result);
    }
}
