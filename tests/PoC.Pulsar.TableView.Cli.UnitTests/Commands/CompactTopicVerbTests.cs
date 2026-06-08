using CommandLine;
using PoC.Pulsar.TableView.Cli.Commands;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Commands;

public sealed class CompactTopicVerbTests
{
    [Fact]
    public void compact_topic_verb_should_define_expected_command_name()
    {
        var attribute = typeof(CompactTopicVerb)
            .GetCustomAttributes(typeof(VerbAttribute), inherit: false)
            .Cast<VerbAttribute>()
            .Single();

        Assert.Equal("compact-topic", attribute.Name);
    }
}
