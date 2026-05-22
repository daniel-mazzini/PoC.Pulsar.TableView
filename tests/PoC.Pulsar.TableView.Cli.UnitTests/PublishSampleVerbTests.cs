using CommandLine;
using PoC.Pulsar.TableView.Cli;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests;

public sealed class PublishSampleVerbTests
{
    [Fact]
    public void publish_sample_verb_should_define_expected_command_name()
    {
        var attribute = typeof(PublishSampleVerb)
            .GetCustomAttributes(typeof(VerbAttribute), inherit: false)
            .Cast<VerbAttribute>()
            .Single();

        Assert.Equal("publish-sample", attribute.Name);
    }
}
